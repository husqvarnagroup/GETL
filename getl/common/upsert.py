from contextlib import ExitStack, contextmanager
from dataclasses import InitVar, dataclass, field
from functools import partial
from itertools import islice
from typing import Any, Callable, Iterable, Iterator, List, Optional

import psycopg2
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

        for rows in chunked(iterator, 1000):
            puq.execute(cursor, rows, columns_map=columns)


@contextmanager
def postgres_connection_cursor(*args, **kwargs):
    with ExitStack() as stack:
        conn = stack.enter_context(psycopg2.connect(*args, **kwargs))
        cursor = stack.enter_context(conn.cursor())
        yield cursor


def chunked(iterator: Iterable[Any], size) -> Iterator[List[Any]]:
    iterator_ = iter(iterator)
    while True:
        rows = list(islice(iterator_, size))
        if not rows:
            break
        yield rows


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
    sql_base_fields: Composed = field(init=False, repr=False)

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

        # sql_base_fields creates sql placeholders for the columns
        # Example, 2 columns will generate:
        # (%s, %s)
        self.sql_base_fields = SQL("({})").format(
            SQL(", ").join(Placeholder() * len(columns))
        )

    def build_query(self, len_rows: int) -> SQL:
        sql_query = """
        INSERT INTO {table} ({columns})
        VALUES {fields}
        ON CONFLICT ({conflict_columns})
        DO UPDATE SET {update}
        """
        return SQL(sql_query).format(
            table=self.sql_table,
            columns=self.sql_columns,
            conflict_columns=self.sql_conflict_columns,
            update=self.sql_update,
            fields=SQL(", ").join(self.sql_base_fields * len_rows),
        )

    def execute(self, cursor, rows: List[Any], columns_map: List[str]) -> None:
        sql_query = self.build_query(len(rows))
        flattened_rows = flatten_rows_dict(rows, columns_map)
        cursor.execute(sql_query, flattened_rows)


def flatten_rows_dict(rows: List[dict], columns: List[str]):
    return [row[col] for row in rows for col in columns]
