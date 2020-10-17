from contextlib import ExitStack, contextmanager
from dataclasses import InitVar, dataclass, field
from functools import partial
from operator import itemgetter
from typing import Any, Callable, Iterable, List, Optional

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.sql import SQL, Composed, Identifier, Placeholder


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
            postgres_connection_cursor_factory=partial(
                postgres_connection_cursor, dsn=dsn, **postgres_connection_kwargs
            ),
            table=table,
            columns=columns,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
        )
    )


def handle_partition(
    iterator: Iterable[Any],
    postgres_connection_cursor_factory: Callable,
    table: str,
    columns: List[str],
    conflict_columns: List[str],
    update_columns: List[str],
):
    with postgres_connection_cursor_factory() as cursor:
        puq = PostgresUpsertQuery(
            table=table,
            columns=columns,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
        )

        rows = map(itemgetter(*columns), iterator)

        puq.execute(cursor, rows, page_size=1000)


@contextmanager
def postgres_connection_cursor(*args, **kwargs):
    with ExitStack() as stack:
        conn = stack.enter_context(psycopg2.connect(*args, **kwargs))
        cursor = stack.enter_context(conn.cursor())
        yield cursor


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

    def execute(self, cursor, rows: Iterable[Any], page_size: int = 100) -> None:
        sql_query = self.build_query()
        execute_values(cursor, sql_query, rows, page_size=page_size)
