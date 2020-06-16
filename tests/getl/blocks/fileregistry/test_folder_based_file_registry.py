"""Unit tests for the custom code index function."""
import pytest
from mock import Mock, patch
from pyspark.sql import functions as F

from getl.block import BlockConfig, BlockLog
from getl.blocks.fileregistry.entrypoint import FolderBased, folder_based


# HELPERS
def setup_params(tmp_dir, m_file_registry):
    # Arrange
    prefix_source = "dataset/live"
    file_registry_base = "{}/file-registry".format(tmp_dir)
    file_registry_path = "{}/dataset/live".format(file_registry_base)
    m_file_registry.return_value = file_registry_path

    return {
        "s3_path": "s3://tmp-bucket/{}".format(prefix_source),
        "file_registry_path": file_registry_path,
        "file_registry_base": file_registry_base,
    }


def setup_bconf(base_prefix, spark_session):
    props = {
        "BasePrefix": base_prefix,
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
def test_creates_folder_based_obj():
    """Function should return a folder_based object."""
    # Arrange
    props = {
        "BasePrefix": "s3/prefix",
        "UpdateAfter": "OtherSection",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": "example",
    }
    conf = BlockConfig("CurrentSection", None, None, props)

    # Act
    res = folder_based(conf)

    # Assert
    assert isinstance(res, FolderBased)
    assert res.file_registry_prefix == "s3/prefix"
    assert res.update_after == "OtherSection"


@patch.object(FolderBased, "_create_hive_table")
@patch.object(FolderBased, "_create_file_registry_path")
def test_folder_based_no_previous_data(
    m_file_registry, m_hive_table, spark_session, helpers, tmp_dir
):
    """Test the load method for when there is no previous data."""
    # Arrange
    params = setup_params(tmp_dir, m_file_registry)
    conf = setup_bconf(params["file_registry_base"], spark_session)

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
    assert m_file_registry.called

    # Check that a file registry delta files have been created
    actual_data = (
        spark_session.read.load(params["file_registry_path"], format="delta")
        .orderBy("file_path")
        .collect()
    )
    assert len(actual_data) == 2
    assert actual_data[0][0] == "s3://tmp-bucket/dataset/live/2020/06/f1.parquet.crc"
    assert actual_data[0][1] is None
    assert actual_data[1][0] == "s3://tmp-bucket/dataset/live/2020/07/f2.parquet.crc"
    assert actual_data[1][1] is None


@patch.object(FolderBased, "_create_file_registry_path")
def test_previous_data_with_only_null_values(
    m_file_registry, s3_mock, spark_session, tmp_dir, helpers
):
    """When there is only null values and no new files."""
    # Arrange
    params = setup_params(tmp_dir, m_file_registry)
    conf = setup_bconf(params["file_registry_base"], spark_session)

    s3_files = [
        ("s3://tmp-bucket/dataset/live/2020/06/01/f1.parquet.crc", None),
        ("s3://tmp-bucket/dataset/live/2020/07/01/f2.parquet.crc", None),
    ]
    create_file_registry(helpers, spark_session, s3_files, params["file_registry_path"])

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


@patch.object(FolderBased, "_create_file_registry_path")
def test_folder_based_load_with_previous_data(
    m_file_registry, spark_session, s3_mock, tmp_dir, helpers
):
    """Test the load method for when there is previous unlifed data."""
    # Arrange
    params = setup_params(tmp_dir, m_file_registry)
    conf = setup_bconf(params["file_registry_base"], spark_session)

    # Create a existing file registry
    s3_files = [("s3://tmp-bucket/dataset/live/2020/06/01/f1.parquet.crc", None)]
    create_file_registry(helpers, spark_session, s3_files, params["file_registry_path"])

    # Files in the s3 mock, new files to add
    helpers.create_s3_files({"dataset/live/2020/07/01/f1.parquet.crc": None})

    # Act
    actual = folder_based(conf).load(params["s3_path"], suffix=".parquet.crc")
    # Assert
    assert m_file_registry.called
    expected = [
        params["s3_path"] + "/2020/06/01/f1.parquet.crc",
        params["s3_path"] + "/2020/07/01/f1.parquet.crc",
    ]
    assert all(elem in expected for elem in actual)


@pytest.mark.parametrize(
    "s3_path, result",
    [
        ("", "s3://husqvarna-datalake/file-registry"),
        (
            "s3://husqvarna-datalake/raw/amc/live",
            "s3://husqvarna-datalake/file-registry/raw/amc/live",
        ),
    ],
)
def test_create_file_registry_path(s3_path, result):
    props = {
        "BasePrefix": "s3://husqvarna-datalake/file-registry",
        "UpdateAfter": "",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": "example",
    }
    conf = BlockConfig("CurrentSection", None, None, props, BlockLog())
    pbd = FolderBased(conf)

    assert pbd._create_file_registry_path(s3_path) == result


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
        "BasePrefix": "",
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
