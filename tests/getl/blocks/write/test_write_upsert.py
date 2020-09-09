"""Unit tests for the write entrypoint."""

from functools import partial

import pytest
from pyspark.sql import types as T

from getl.blocks.write.entrypoint import batch_postgres_upsert
from getl.common.upsert import (
    chunked,
    flatten_rows_dict,
    handle_partition,
    postgres_connection_cursor,
)


def create_dataframe(spark_session, data):
    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("count", T.IntegerType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def create_table(postgres_cursor, postgres_connection):
    def inner():
        postgres_cursor.execute("DROP TABLE IF EXISTS filetable")
        postgres_cursor.execute(
            """
        CREATE TABLE filetable (
            id serial PRIMARY KEY,
            file_path varchar,
            count int,
            unique(file_path)
        )
        """
        )
        postgres_connection.commit()

    return inner


@pytest.fixture(scope="function")
def select_data(postgres_cursor):
    def inner():
        postgres_cursor.execute("select file_path, count from filetable")
        return postgres_cursor.fetchall()

    return inner


def test_batch_postgres_upsert_no_update_columns(
    helpers, spark_session, postgres_connection_details, create_table, select_data
):
    props = {
        "ConnUrl": postgres_connection_details["dsn"],
        "User": postgres_connection_details["user"],
        "Password": postgres_connection_details["password"],
        "Table": "filetable",
        "Columns": ["file_path", "count"],
        "ConflictColumns": ["file_path"],
    }

    create_table()

    first_df = create_dataframe(
        spark_session, [("path/to/file1", 1), ("path/to/file2", 4)]
    ).repartition(2)
    assert first_df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(first_df, props)
    batch_postgres_upsert(bconf)
    data = select_data()
    assert set(data) == {("path/to/file1", 1), ("path/to/file2", 4)}

    second_df = create_dataframe(
        spark_session, [("path/to/file1", 5), ("path/to/file6", 6)]
    ).repartition(2)
    assert second_df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(second_df, props)
    batch_postgres_upsert(bconf)
    data = select_data()
    assert set(data) == {
        ("path/to/file1", 5),
        ("path/to/file2", 4),
        ("path/to/file6", 6),
    }


def test_batch_postgres_upsert_chunked(
    helpers, spark_session, postgres_connection_details, create_table, select_data
):
    props = {
        "ConnUrl": postgres_connection_details["dsn"],
        "User": postgres_connection_details["user"],
        "Password": postgres_connection_details["password"],
        "Table": "filetable",
        "Columns": ["file_path", "count"],
        "ConflictColumns": ["file_path"],
        "UpdateColumns": ["count"],
    }

    create_table()

    df_data = [(f"path/to/file{i}", i) for i in range(3000)]
    df = create_dataframe(spark_session, df_data).repartition(2)
    assert df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(df, props)
    batch_postgres_upsert(bconf)
    data = select_data()
    assert set(data) == set(df_data)

    # Update the data
    df_data = [(path, i * 2) for path, i in df_data]
    df = create_dataframe(spark_session, df_data).repartition(2)
    assert df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(df, props)
    batch_postgres_upsert(bconf)
    data = select_data()
    assert set(data) == set(df_data)


def test_handle_partition(
    helpers, spark_session, postgres_connection_details, create_table, select_data
):
    create_table()

    handle_partition_factory = partial(
        handle_partition,
        postgres_connection_cursor_factory=partial(
            postgres_connection_cursor, **postgres_connection_details
        ),
        table="filetable",
        columns=["file_path", "count"],
        conflict_columns=["file_path"],
        update_columns=["count"],
    )

    iterator = iter(
        [
            {"file_path": "path/to/file1", "count": 1},
            {"file_path": "path/to/file2", "count": 4},
        ]
    )
    handle_partition_factory(iterator)
    data = select_data()
    assert set(data) == {("path/to/file1", 1), ("path/to/file2", 4)}

    iterator = iter(
        [
            {"file_path": "path/to/file1", "count": 5},
            {"file_path": "path/to/file6", "count": 6},
        ]
    )
    handle_partition_factory(iterator)
    data = select_data()
    assert set(data) == {
        ("path/to/file1", 5),
        ("path/to/file2", 4),
        ("path/to/file6", 6),
    }


def test_flatten_rows_dict():
    input_array = [
        {"file_path": "path/to/file1", "count": 1, "extra": "nothing"},
        {"file_path": "path/to/file2", "count": 4, "extra": "something"},
    ]
    result_array = ["path/to/file1", 1, "path/to/file2", 4]
    assert flatten_rows_dict(input_array, ["file_path", "count"]) == result_array


def test_chunked():
    input_array = ["lorem", "ipsum", "dolor", "sit", "amet"]
    result_array = [
        ["lorem", "ipsum"],
        ["dolor", "sit"],
        ["amet"],
    ]

    assert list(chunked(input_array, 2)) == result_array
