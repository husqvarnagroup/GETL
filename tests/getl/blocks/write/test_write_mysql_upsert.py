"""Unit tests for the write entrypoint."""

from functools import partial

import pytest
from pyspark.sql import types as T

from getl.blocks.write.entrypoint import batch_mysql_upsert
from getl.common.upsert import (
    MysqlUpsertQuery,
    handle_partition,
    mysql_connection_cursor,
)


def create_dataframe(spark_session, data):
    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("count", T.IntegerType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema)


def test_mysql_connection(create_table, select_data, mysql_cursor):
    create_table()
    mysql_cursor.execute(
        """
    INSERT INTO filetable
    (file_path, count)
    VALUES
    (%s, %s)
    """,
        ("path/to/file", 1),
    )
    assert select_data() == [("path/to/file", 1)]


@pytest.fixture
def create_table(mysql_cursor, mysql_connection):
    def inner():
        mysql_cursor.execute("DROP TABLE IF EXISTS filetable")
        mysql_cursor.execute(
            """
        CREATE TABLE filetable (
            id int(11) NOT NULL AUTO_INCREMENT,
            file_path varchar(255),
            count int(11),
            PRIMARY KEY(`id`), UNIQUE KEY (`file_path`)
        )
        """
        )
        mysql_connection.commit()

    return inner


@pytest.fixture(scope="function")
def select_data(mysql_connection, mysql_cursor):
    def inner():
        mysql_connection.commit()  # Refreshes query cache
        mysql_cursor.execute("select file_path, count from filetable")
        return mysql_cursor.fetchall()

    return inner


def test_batch_mysql_upsert_no_update_columns(
    helpers, spark_session, mysql_connection_details, create_table, select_data
):
    props = {
        "Host": mysql_connection_details["host"],
        "Port": mysql_connection_details["port"],
        "Database": mysql_connection_details["database"],
        "User": mysql_connection_details["user"],
        "Password": mysql_connection_details["password"],
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
    batch_mysql_upsert(bconf)
    data = select_data()
    assert set(data) == {("path/to/file1", 1), ("path/to/file2", 4)}

    second_df = create_dataframe(
        spark_session, [("path/to/file1", 5), ("path/to/file6", 6)]
    ).repartition(2)
    assert second_df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(second_df, props)
    batch_mysql_upsert(bconf)
    data = select_data()
    assert set(data) == {
        ("path/to/file1", 5),
        ("path/to/file2", 4),
        ("path/to/file6", 6),
    }


def test_batch_mysql_upsert_chunked(
    helpers, spark_session, mysql_connection_details, create_table, select_data
):
    props = {
        "Host": mysql_connection_details["host"],
        "Port": mysql_connection_details["port"],
        "Database": mysql_connection_details["database"],
        "User": mysql_connection_details["user"],
        "Password": mysql_connection_details["password"],
        "Table": "filetable",
        "Columns": ["file_path", "count"],
        "ConflictColumns": ["file_path"],
    }

    create_table()

    df_data = [(f"path/to/file{i}", i) for i in range(3000)]
    df = create_dataframe(spark_session, df_data).repartition(2)
    assert df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(df, props)
    batch_mysql_upsert(bconf)
    data = select_data()
    assert set(data) == set(df_data)

    # Update the data
    df_data = [(path, i * 2) for path, i in df_data]
    df = create_dataframe(spark_session, df_data).repartition(2)
    assert df.rdd.getNumPartitions() == 2

    bconf = helpers.create_block_conf(df, props)
    batch_mysql_upsert(bconf)
    data = select_data()
    assert set(data) == set(df_data)


def test_handle_partition(
    helpers,
    spark_session,
    mysql_connection_details,
    create_table,
    select_data,
    mysql_connection,
):
    create_table()

    handle_partition_factory = partial(
        handle_partition,
        connection_cursor_factory=partial(
            mysql_connection_cursor, **mysql_connection_details
        ),
        upsert_query_class=MysqlUpsertQuery(
            table="filetable",
            columns=["file_path", "count"],
            conflict_columns=["file_path"],
            update_columns=["count"],
        ),
        columns=["file_path", "count"],
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
