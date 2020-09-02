"""Unit tests for the write entrypoint."""

from contextlib import ExitStack, contextmanager

import psycopg2
from pyspark.sql import types as T

from getl.blocks.write.entrypoint import batch_postgres_upsert


def create_dataframe(spark_session, data):
    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("count", T.IntegerType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema)


@contextmanager
def postgres_query(props):
    with ExitStack() as stack:
        conn = stack.enter_context(
            psycopg2.connect(
                dsn=props["ConnUrl"], user=props["User"], password=props["Password"]
            )
        )
        yield stack.enter_context(conn.cursor())


def create_db_table(props):
    with postgres_query(props) as cursor:
        cursor.execute("DROP TABLE IF EXISTS filetable")
        cursor.execute(
            """
        CREATE TABLE filetable (
            id serial PRIMARY KEY,
            file_path varchar,
            count int,
            unique(file_path)
        )
        """
        )


def select_data(props):
    with postgres_query(props) as cursor:
        cursor.execute("select file_path, count from filetable")
        return cursor.fetchall()


def test_batch_postgres_upsert_no_update_columns(helpers, spark_session):
    props = {
        "ConnUrl": "postgres://localhost:5432/testdb",
        "User": "dbadmin",
        "Password": "mintkaka2010",
        "Table": "filetable",
        "Columns": ["file_path", "count"],
        "ConflictColumns": ["file_path"],
    }

    create_db_table(props)

    first_df = create_dataframe(
        spark_session, [("path/to/file1", 1), ("path/to/file2", 4)]
    ).repartition(2)
    assert first_df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(first_df, props)
    batch_postgres_upsert(bconf)
    data = select_data(props)
    assert set(data) == {("path/to/file1", 1), ("path/to/file2", 4)}

    second_df = create_dataframe(
        spark_session, [("path/to/file1", 5), ("path/to/file6", 6)]
    ).repartition(2)
    assert second_df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(second_df, props)
    batch_postgres_upsert(bconf)
    data = select_data(props)
    assert set(data) == {
        ("path/to/file1", 5),
        ("path/to/file2", 4),
        ("path/to/file6", 6),
    }


def test_batch_postgres_upsert_chunked(helpers, spark_session):
    props = {
        "ConnUrl": "postgres://localhost:5432/testdb",
        "User": "dbadmin",
        "Password": "mintkaka2010",
        "Table": "filetable",
        "Columns": ["file_path", "count"],
        "ConflictColumns": ["file_path"],
        "UpdateColumns": ["count"],
    }

    create_db_table(props)

    df_data = [(f"path/to/file{i}", i) for i in range(3000)]
    df = create_dataframe(spark_session, df_data).repartition(2)
    assert df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(df, props)
    batch_postgres_upsert(bconf)
    data = select_data(props)
    assert set(data) == set(df_data)

    # Update the data
    df_data = [(path, i * 2) for path, i in df_data]
    df = create_dataframe(spark_session, df_data).repartition(2)
    assert df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(df, props)
    batch_postgres_upsert(bconf)
    data = select_data(props)
    assert set(data) == set(df_data)
