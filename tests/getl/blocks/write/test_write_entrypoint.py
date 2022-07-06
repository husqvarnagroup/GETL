"""Unit tests for the write entrypoint."""

from unittest.mock import patch

import pytest
from pyspark.sql import types as T

from getl.blocks.write.batch_delta import BatchDelta
from getl.blocks.write.entrypoint import batch_delta, batch_json

schema = T.StructType(
    [
        T.StructField("file_path", T.StringType(), True),
        T.StructField("count", T.IntegerType(), True),
        T.StructField("year", T.IntegerType(), True),
        T.StructField("month", T.IntegerType(), True),
    ]
)


def create_dataframe(spark_session, data):
    return spark_session.createDataFrame(data, schema)


# TESTS
@patch("getl.blocks.write.entrypoint.HiveTable")
def test_batch_delta_overwrite(m_hive_table, helpers, spark_session, tmp_dir):
    """Batch write delta files."""
    # Arrange
    props = {
        "Path": tmp_dir,
        "Mode": "overwrite",
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
    }
    bconf = helpers.create_block_conf(
        create_dataframe(spark_session, [("abc", 1, 2020, 10), ("qwe", 2, 2020, 10)]),
        props,
    )

    # Act
    batch_delta(bconf)

    # Assert
    assert spark_session.read.load(tmp_dir, format="delta").count() == 2
    assert m_hive_table.called


@patch("getl.blocks.write.entrypoint.HiveTable")
@patch.object(BatchDelta, "write")
def test_batch_delta_partitionby(
    m_write, m_hive_table, helpers, spark_session, tmp_dir
):
    """Batch write delta files with partitionBy."""
    # Arrange
    props = {
        "Path": tmp_dir,
        "Mode": "overwrite",
        "PartitionBy": {"Columns": ["year", "month"]},
        "MergeSchema": True,
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
    }
    bconf = helpers.create_block_conf(
        create_dataframe(
            spark_session,
            [("abc", 1, 2020, 10), ("qwe", 2, 2021, 11), ("asd", 3, 2021, 12)],
        ),
        props,
    )

    # Act
    batch_delta(bconf)

    # Assert
    m_write.assert_called_once_with(tmp_dir, "overwrite", ["year", "month"], True)
    assert m_hive_table.called


@pytest.mark.parametrize(
    "params, calls",
    [
        ({}, []),
        ({"Optimize": {"Enabled": False}}, []),
        ({"Optimize": {"Enabled": True}}, [None]),
        ({"Optimize": {"Enabled": True, "ZorderBy": "columnking"}}, ["columnking"]),
    ],
)
@patch("getl.blocks.write.entrypoint.HiveTable")
@patch.object(BatchDelta, "optimize")
def test_batch_delta_optimize(
    m_optimize, m_hive_table, params, calls, helpers, spark_session, tmp_dir
):
    """While writing create a hive table with and without ZOPTIMIZE."""
    # Arrange
    props = {
        "Path": tmp_dir,
        "Mode": "overwrite",
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
        **params,
    }
    bconf = helpers.create_block_conf(
        create_dataframe(spark_session, [("abc", 1, 2020, 10)]), props
    )

    # Act
    batch_delta(bconf)

    # Assert
    if calls:
        m_optimize.assert_called_once_with(bconf.spark, "default", "table", *calls)
    else:
        assert not m_optimize.called


@pytest.mark.parametrize(
    "params, calls",
    [
        ({}, []),
        ({"Vacuum": {"Enabled": False}}, []),
        ({"Vacuum": {"Enabled": True}}, [168]),
        ({"Vacuum": {"Enabled": True, "RetainHours": 1000}}, [1000]),
    ],
)
@patch("getl.blocks.write.entrypoint.HiveTable")
@patch.object(BatchDelta, "vacuum")
def test_batch_delta_vacuum(
    m_vacuum, m_hive_table, params, calls, helpers, spark_session, tmp_dir
):
    """Test the batch delta vacuum proprety."""
    # Arrange
    props = {
        "Path": tmp_dir,
        "Mode": "overwrite",
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
        **params,
    }
    bconf = helpers.create_block_conf(
        create_dataframe(spark_session, [("abc", 1, 2020, 10)]), props
    )

    # Act
    batch_delta(bconf)

    # Assert
    if calls:
        m_vacuum.assert_called_once_with(bconf.spark, "default", "table", *calls)
    else:
        assert not m_vacuum.called


@patch("getl.blocks.write.entrypoint.HiveTable")
def test_batch_delta_upsert(m_hive_table, tmp_dir, helpers, spark_session):
    """Insert of update delta files when keys match."""
    # Arrange
    props = {
        "Path": tmp_dir,
        "Mode": "upsert",
        "Upsert": {"MergeStatement": "source.file_path = updates.file_path"},
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
    }
    first_df = create_dataframe(
        spark_session, [("path/to/file1", 1, 2020, 10), ("path/to/file2", 4, 2020, 10)]
    )
    second_df = create_dataframe(
        spark_session, [("path/to/file1", 5, 2020, 10), ("path/to/file6", 6, 2020, 10)]
    )

    # Act & Assert: First time we need to create a delta table
    bconf = helpers.create_block_conf(first_df, props)
    batch_delta(bconf)
    assert spark_session.read.load(tmp_dir, format="delta").count() == 2

    # Act & Assert: Second time we  do an upsert when files exist
    bconf = helpers.create_block_conf(second_df, props)
    batch_delta(bconf)
    assert spark_session.read.load(tmp_dir, format="delta").count() == 3


@patch.object(BatchDelta, "write")
@patch("getl.blocks.write.entrypoint.HiveTable")
def test_batch_clean_write(m_hive_table, m_write, s3_mock, helpers):
    """Insert of update delta files when keys match."""
    # Arrange
    props = {
        "Path": "s3://tmp-bucket/",
        "Mode": "clean_write",
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
    }
    helpers.create_s3_files(
        {
            "prefix/to/file1.json": None,
            "prefix/to/file2.json": None,
            "prefix/to/file3.json": None,
        }
    )
    # Act & Assert: Second time we  do an upsert when files exist
    bconf = helpers.create_block_conf(None, props)
    batch_delta(bconf)
    m_write.assert_called_once_with("s3://tmp-bucket/", "overwrite", None, False)
    assert (
        s3_mock.list_objects_v2(Bucket="tmp-bucket", Prefix="prefix")["KeyCount"] == 0
    )


def test_write_batch_json(helpers, spark_session, tmp_path):
    """Batch write json files with partitionBy."""
    # Arrange
    props = {
        "Path": str(tmp_path),
        "Mode": "overwrite",
        # "PartitionBy": {"Columns": ["year", "month"]},
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
    }
    df = create_dataframe(
        spark_session,
        [("abc", 1, 2020, 10), ("qwe", 2, 2021, 11), ("asd", 3, 2021, 12)],
    )

    bconf = helpers.create_block_conf(df, props)

    # Act
    batch_json(bconf)

    df_read = spark_session.read.load(str(tmp_path), format="json", schema=schema)

    assert df.orderBy("file_path").collect() == df_read.orderBy("file_path").collect()


def test_write_batch_json_partitionBy(helpers, spark_session, tmp_path):
    """Batch write json files with partitionBy."""
    # Arrange
    props = {
        "Path": str(tmp_path),
        "Mode": "overwrite",
        "PartitionBy": {"Columns": ["year", "month"]},
        "HiveTable": {"DatabaseName": "default", "TableName": "table"},
    }
    df = create_dataframe(
        spark_session,
        [("abc", 1, 2020, 10), ("qwe", 2, 2021, 11), ("asd", 3, 2021, 12)],
    )

    bconf = helpers.create_block_conf(df, props)

    # Act
    batch_json(bconf)

    assert (tmp_path / "year=2021").exists()

    df_read = spark_session.read.load(
        str(tmp_path), format="json", recursivePath=True, schema=schema
    )

    assert df.orderBy("file_path").collect() == df_read.orderBy("file_path").collect()
