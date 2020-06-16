"""Unit tests for the custom code index function."""
import pytest
from mock import Mock, patch
from pyspark.sql import functions as F

from getl.block import BlockConfig, BlockLog
from getl.blocks.fileregistry.entrypoint import FolderBased, folder_based


# HELPERS
def setup_params(tmp_dir):
    # Arrange
    prefix = "file_registry/dataset/live"

    return {
        "s3_path": f"s3://tmp-bucket/{prefix}",
        "file_registry_local": f"{tmp_dir}/{prefix}",
    }


def setup_bconf(base_path, spark_session):
    props = {
        "BasePath": base_path,
        "UpdateAfter": "OtherSection",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": "example",
    }

    return BlockConfig("CurrentSection", spark_session, None, props, BlockLog())


def create_file_registry(helpers, spark, files, file_registry_path):
    data = helpers.convert_events_to_datetime(files, "%Y/%m/%d")
    current_df = spark.createDataFrame(data, FolderBased.schema)
    current_df = current_df.where(~F.col("file_path").contains("f4.parquet"))
    current_df.write.save(path=file_registry_path, format="delta", mode="overwrite")


# TESTS
def test_creates_folder_based_obj(spark_session, tmp_dir):
    """Function should return a folder_based object."""
    # Arrange
    props = {
        "BasePath": f"{tmp_dir}/bucket/path/to/filereg",
        "UpdateAfter": "OtherSection",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": "example",
    }
    conf = BlockConfig("CurrentSection", spark_session, None, props)

    # Act
    res = folder_based(conf)

    # Assert
    assert isinstance(res, FolderBased)
    assert res.file_registry_path == f"{tmp_dir}/bucket/path/to/filereg"
    assert res.update_after == "OtherSection"


@patch.object(FolderBased, "_create_hive_table")
def test_folder_based_no_previous_data(m_hive_table, spark_session, helpers, tmp_dir):
    """Test the load method for when there is no previous data."""
    # Arrange
    params = setup_params(tmp_dir)
    conf = setup_bconf(params["file_registry_local"], spark_session)

    # Configure s3 mock
    helpers.create_s3_files(
        {
            "dataset/live/2020/06/f1.parquet.crc": None,
            "dataset/live/2020/07/f2.parquet.crc": None,
        }
    )

    expected = [
        "s3://tmp-bucket/dataset/live/2020/06/f1.parquet.crc",
        "s3://tmp-bucket/dataset/live/2020/07/f2.parquet.crc",
    ]

    # Act
    actual = folder_based(conf).load("s3://tmp-bucket/dataset/live", ".parquet.crc")

    # Assert
    assert len(actual) == 2
    assert expected == sorted(actual)
    assert m_hive_table.called

    # Check that a file registry delta files have been created
    actual_data = (
        spark_session.read.load(params["file_registry_local"], format="delta")
        .orderBy("file_path")
        .collect()
    )
    assert len(actual_data) == 2
    assert actual_data[0][0] == "s3://tmp-bucket/dataset/live/2020/06/f1.parquet.crc"
    assert actual_data[0][1] is None
    assert actual_data[1][0] == "s3://tmp-bucket/dataset/live/2020/07/f2.parquet.crc"
    assert actual_data[1][1] is None


def test_previous_data_with_only_null_values(s3_mock, spark_session, tmp_dir, helpers):
    """When there is only null values and no new files."""
    # Arrange
    params = setup_params(tmp_dir)
    conf = setup_bconf(params["file_registry_local"], spark_session)

    s3_files = [
        ("s3://tmp-bucket/file_registry/dataset/live/2020/06/01/f1.parquet.crc", None),
        ("s3://tmp-bucket/file_registry/dataset/live/2020/07/01/f2.parquet.crc", None),
    ]
    create_file_registry(
        helpers, spark_session, s3_files, params["file_registry_local"]
    )

    # Files in s3 mock
    helpers.create_s3_files(
        {
            "dataset/live/2020/06/01/f1.parquet.crc": None,
            "dataset/live/2020/07/01/f2.parquet.crc": None,
        }
    )

    # Act
    actual = folder_based(conf).load(params["s3_path"], suffix=".parquet.crc")

    # Assert
    base = params["s3_path"]
    check_list = [
        f"{base}/2020/06/01/f1.parquet.crc",
        f"{base}/2020/07/01/f2.parquet.crc",
    ]
    assert all(elem in check_list for elem in actual)


def test_folder_based_load_with_previous_data(spark_session, s3_mock, tmp_dir, helpers):
    """Test the load method for when there is previous unlifed data."""
    # Arrange
    params = setup_params(tmp_dir)
    conf = setup_bconf(params["file_registry_local"], spark_session)

    # Create a existing file registry
    s3_files = [
        ("s3://tmp-bucket/file_registry/dataset/live/2020/06/01/f1.parquet.crc", None)
    ]
    create_file_registry(
        helpers, spark_session, s3_files, params["file_registry_local"]
    )

    # Files in the s3 mock, new files to add
    helpers.create_s3_files({"dataset/live/2020/07/01/f1.parquet.crc": None})

    # Act
    actual = folder_based(conf).load(params["s3_path"], suffix=".parquet.crc")
    # Assert
    expected = [
        params["s3_path"] + "/2020/06/01/f1.parquet.crc",
        params["s3_path"] + "/2020/07/01/f1.parquet.crc",
    ]

    print(actual)
    print(expected)

    assert all(elem in expected for elem in actual)


@pytest.mark.parametrize(
    "path, table",
    [
        ("s3://husqvarna-datalake/file-registry/amc", "amc"),
        ("s3://husqvarna-datalake/file-registry/test", "test"),
    ],
)
def test_create_hive_table(path, table):
    # Arrange
    spark = Mock()
    props = {
        "BasePath": "",
        "UpdateAfter": "",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": table,
    }
    conf = BlockConfig("CurrentSection", spark, None, props, BlockLog())
    fb = FolderBased(conf)

    # Act
    fb._create_hive_table(path)

    # Assert
    assert path in str(spark.sql.call_args[0])
    assert "file_registry_dev" in str(spark.sql.call_args_list[0][0])
    assert table in str(spark.sql.call_args_list[2][0])
