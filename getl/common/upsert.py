import time
from contextlib import ExitStack, contextmanager
from dataclasses import InitVar, dataclass, field
from functools import partial
from itertools import islice
from operator import itemgetter
from typing import Any, Callable, Iterable, List, Optional

import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.sql import SQL, Composed, Identifier, Placeholder

SLEEPING_TIME = 60


def handle_postgres_upsert(
    dataframe,
    dsn: str,
    table: str,
    columns: List[str],
    conflict_columns: List[str],
    update_columns: Optional[List[str]] = None,
    **postgres_connection_kwargs,
):
    if not update_columns:
        update_columns = list(set(columns) - set(conflict_columns))
    dataframe.rdd.foreachPartition(
        partial(
            handle_partition,
            connection_cursor_factory=partial(
                postgres_connection_cursor, dsn=dsn, **postgres_connection_kwargs
            ),
            upsert_query_class=PostgresUpsertQuery(
                table=table,
                columns=columns,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
            ),
            columns=columns,
        )
    )


def handle_mysql_upsert(
    dataframe,
    table: str,
    columns: List[str],
    conflict_columns: List[str],
    update_columns: Optional[List[str]] = None,
    **connection_kwargs,
):
    if not update_columns:
        update_columns = list(set(columns) - set(conflict_columns))
    dataframe.rdd.foreachPartition(
        partial(
            handle_partition,
            connection_cursor_factory=partial(
                mysql_connection_cursor, **connection_kwargs
            ),
            upsert_query_class=MysqlUpsertQuery(
                table=table,
                columns=columns,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
            ),
            columns=columns,
        )
    )


def handle_partition(
    iterator: Iterable[Any],
    connection_cursor_factory: Callable,
    upsert_query_class: Any,
    columns: List[str],
):
    rows = map(itemgetter(*columns), iterator)

    upsert_query_class.execute(connection_cursor_factory, rows, page_size=1000)


@contextmanager
def postgres_connection_cursor(*args, **kwargs):
    with ExitStack() as stack:
        conn = stack.enter_context(psycopg2.connect(*args, **kwargs))
        cursor = stack.enter_context(conn.cursor())
        yield cursor


@contextmanager
def mysql_connection_cursor(*args, **kwargs):
    with ExitStack() as stack:
        conn = stack.enter_context(mysql.connector.connect(*args, **kwargs))
        cursor = stack.enter_context(conn.cursor())
        yield cursor
        conn.commit()


@dataclass
class PostgresUpsertQuery:
    table: InitVar[str]
    columns: InitVar[List[str]]
    conflict_columns: InitVar[List[str]]
    update_columns: InitVar[List[str]]

    sql_table: Identifier = field(init=False)
    sql_columns: Composed = field(init=False)
    sql_conflict_columns: Composed = field(init=False)
    sql_update: Composed = field(init=False)

    def __post_init__(self, table, columns, conflict_columns, update_columns):
        # Inits the base sql statements to be build later once the rows are available
        # Escaping all the python fields/columns is the complex part in this function
        self.sql_table = Identifier(table)
        self.sql_columns = SQL(", ").join(map(Identifier, columns))
        self.sql_conflict_columns = SQL(", ").join(map(Identifier, conflict_columns))

        # sql_update creates an update statement for the update_columns
        # Example: columns 'name' and 'age' will generate:
        # "name = EXCLUDED.name, age = EXCLUDED.age"
        self.sql_update = SQL(", ").join(
            SQL("{} = EXCLUDED.{}").format(Identifier(col), Identifier(col))
            for col in update_columns
        )

    def build_query(self) -> SQL:
        sql_query = """
        INSERT INTO {table} ({columns})
        VALUES {values}
        ON CONFLICT ({conflict_columns})
        DO UPDATE SET {update}
        """
        return SQL(sql_query).format(
            table=self.sql_table,
            columns=self.sql_columns,
            values=Placeholder(),
            conflict_columns=self.sql_conflict_columns,
            update=self.sql_update,
        )

    def execute(
        self, connection_cursor_factory, rows: Iterable[Any], page_size: int = 100
    ) -> None:
        sql_query = self.build_query()
        exception = Exception("Unknown error")

        for chunk in chunked(rows, page_size):
            for _ in range(10):
                try:
                    with connection_cursor_factory() as cursor:
                        execute_values(cursor, sql_query, chunk)
                    break
                except psycopg2.OperationalError as e:
                    exception = e
                    print(f"Error: {e}")
                    print(f"Sleeping for {SLEEPING_TIME}")
                    time.sleep(SLEEPING_TIME)
            else:
                raise exception


@dataclass
class MysqlUpsertQuery:
    table: InitVar[str]
    columns: InitVar[List[str]]
    conflict_columns: InitVar[List[str]]
    update_columns: InitVar[List[str]]

    sql_table: str = field(init=False)
    sql_columns: str = field(init=False)
    sql_conflict_columns: str = field(init=False)
    sql_update: str = field(init=False)
    sql_values: str = field(init=False)

    def __post_init__(self, table, columns, conflict_columns, update_columns):
        # Inits the base sql statements to be build later once the rows are available
        # Escaping all the python fields/columns is the complex part in this function
        # Though escaping isn't build into mysql, and maybe overkill for the use case here
        self.sql_table = table
        self.sql_columns = ", ".join(columns)
        # This is actually not being used, but exists to have the same
        # interface as PostgresUpsertQuery
        self.sql_conflict_columns = ", ".join(conflict_columns)

        # sql_update creates an update statement for the update_columns
        # Example: columns 'name' and 'age' will generate:
        # "name = VALUES(name), age = VALUES(age)"
        self.sql_update = ", ".join(
            "{} = VALUES({})".format(col, col) for col in update_columns
        )
        self.sql_values = ", ".join("%s" for _ in columns)

    def build_query(self) -> SQL:
        sql_query = """
        INSERT INTO {table} ({columns})
        VALUES ({values})
        ON DUPLICATE KEY UPDATE
        {update};
        """
        return sql_query.format(
            table=self.sql_table,
            columns=self.sql_columns,
            values=self.sql_values,
            update=self.sql_update,
        )

    def execute(
        self, connection_cursor_factory, rows: Iterable[Any], page_size: int = 100
    ) -> None:
        sql_query = self.build_query()
        for chunk in chunked(rows, page_size):
            for _ in range(10):
                try:
                    with connection_cursor_factory() as cursor:
                        cursor.executemany(sql_query, chunk)
                    break
                except mysql.connector.Error as e:
                    exception = e
                    print(f"Error: {e}")
                    print(f"Sleeping for {SLEEPING_TIME}")
                    time.sleep(SLEEPING_TIME)
            else:
                raise exception


def chunked(iterator, size):
    iterator_ = iter(iterator)
    while True:
        rows = list(islice(iterator_, size))
        if not rows:
            break
        yield rows
