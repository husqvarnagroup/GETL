"""Unit tests for the write entrypoint."""
import sys

import pytest
from getl.blocks.write.batch_delta import BatchDelta
from getl.blocks.write.entrypoint import batch_delta
from getl.common.delta_table import DeltaTable
from getl.common.hive_table import HiveTable
from mock import Mock, patch
from pyspark.sql import types as T


def create_dataframe(spark_session, data):
    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("count", T.IntegerType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# TESTS
@patch("getl.blocks.write.entrypoint.HiveTable")
def test_batch_delta_overwrite(m_hive_table, helpers, spark_session, tmp_dir):
    """Batch write delta files."""
    # Arrange
    props = {"Path": tmp_dir, "Mode": "overwrite"}
    bconf = helpers.create_block_conf(
        create_dataframe(spark_session, [("abc", 1), ("qwe", 2)]), props
    )

    # Act
    batch_delta(bconf)

    # Assert
    assert spark_session.read.load(tmp_dir, format="delta").count() == 2
    assert not m_hive_table.called


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
    props = {"Path": tmp_dir, "Mode": "overwrite", **params}
    spark_session.sql = Mock()
    bconf = helpers.create_block_conf(
        create_dataframe(spark_session, [("abc", 1)]), props
    )

    # Act
    batch_delta(bconf)

    # Assert
    if calls:
        m_optimize.assert_called_once_with(bconf.spark, tmp_dir, *calls)
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
    props = {"Path": tmp_dir, "Mode": "overwrite", **params}
    spark_session.sql = Mock()
    bconf = helpers.create_block_conf(
        create_dataframe(spark_session, [("abc", 1)]), props
    )

    # Act
    batch_delta(bconf)

    # Assert
    if calls:
        m_vacuum.assert_called_once_with(bconf.spark, tmp_dir, *calls)
    else:
        assert not m_vacuum.called


def test_batch_delta_upsert(tmp_dir, helpers, spark_session):
    """Insert of update delta files when keys match."""
    # Arrange
    props = {
        "Path": tmp_dir,
        "Mode": "upsert",
        "Upsert": {"MergeStatement": "source.file_path = updates.file_path"},
    }
    first_df = create_dataframe(
        spark_session, [("path/to/file1", 1), ("path/to/file2", 4)]
    )
    second_df = create_dataframe(
        spark_session, [("path/to/file1", 5), ("path/to/file6", 6)]
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
def test_batch_clean_write(m_write, s3_mock, helpers):
    """Insert of update delta files when keys match."""
    # Arrange
    props = {
        "Path": "s3://tmp-bucket/",
        "Mode": "clean_write",
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
    m_write.assert_called_once_with("s3://tmp-bucket/", "overwrite")
    assert (
        s3_mock.list_objects_v2(Bucket="tmp-bucket", Prefix="prefix")["KeyCount"] == 0
    )
