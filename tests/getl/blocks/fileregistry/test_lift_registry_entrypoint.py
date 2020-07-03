"""Unit tests for the custom code index function."""
from datetime import datetime, timedelta

import pytest
from mock import Mock, patch
from pyspark.sql import functions as F

from getl.block import BlockConfig, BlockLog
from getl.blocks.fileregistry.entrypoint import S3PrefixScan, s3_prefix_scan


# HELPERS
def create_fr_row(filename, date, params, lift_date=None):
    return (
        f'{params["s3_path"]}/{date.strftime("%Y/%m/%d")}/{filename}',
        date.date(),
        lift_date,
    )


def create_s3_key(filename, date, params):
    return f'{params["prefix_source"]}/{date.strftime("%Y/%m/%d")}/{filename}'


def setup_params(tmp_dir, m_file_registry):
    # Arrange
    prefix_source = "plantlib/live"
    file_registry_base = "{}/file-registry".format(tmp_dir)
    file_registry_path = "{}/plantlib/live".format(file_registry_base)
    m_file_registry.return_value = file_registry_path

    return {
        "three_days_ago": (datetime.now() - timedelta(days=3)),
        "two_days_ago": (datetime.now() - timedelta(days=2)),
        "one_day_ago": (datetime.now() - timedelta(days=1)),
        "today": datetime.now(),
        "prefix_source": prefix_source,
        "s3_path": "s3://tmp-bucket/{}".format(prefix_source),
        "file_registry_path": file_registry_path,
        "file_registry_base": file_registry_base,
    }


def setup_bconf(base_prefix, start_date, spark_session):
    props = {
        "BasePrefix": base_prefix,
        "UpdateAfter": "OtherSection",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": "example",
        "DefaultStartDate": start_date.strftime("%Y-%m-%d"),
        "PartitionFormat": "%Y/%m/%d",
    }

    return BlockConfig("CurrentSection", spark_session, None, props, BlockLog())


def create_file_registry(helpers, spark, files, file_registry_path):
    data = helpers.convert_events_to_datetime(files, "%Y/%m/%d")
    current_df = spark.createDataFrame(data, S3PrefixScan.schema)
    current_df = current_df.where(~F.col("file_path").contains("f4.parquet"))
    current_df.write.save(path=file_registry_path, format="delta", mode="overwrite")


# TESTS
def test_creates_s3_prefix_scan_obj():
    """Function should return a prefix based date object."""
    # Arrange
    props = {
        "BasePrefix": "s3/prefix",
        "UpdateAfter": "OtherSection",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": "example",
        "DefaultStartDate": "2020-01-01",
        "PartitionFormat": "%Y/%m/%d",
    }
    conf = BlockConfig("CurrentSection", None, None, props)

    # Act
    res = s3_prefix_scan(conf)

    # Assert
    assert res.file_registry_prefix == "s3/prefix"
    assert res.update_after == "OtherSection"


@patch.object(S3PrefixScan, "_create_hive_table")
@patch.object(S3PrefixScan, "_create_file_registry_path")
def test_pbd_load_no_previous_data(
    m_file_registry, m_hive_table, spark_session, helpers, tmp_dir
):
    """Test the load method for prefixed based date when there is no prev data."""
    # Arrange
    params = setup_params(tmp_dir, m_file_registry)
    conf = setup_bconf(
        params["file_registry_base"], params["two_days_ago"], spark_session
    )

    # Configure s3 mock
    helpers.create_s3_files(
        {
            create_s3_key("f1.parquet.crc", params["three_days_ago"], params): None,
            create_s3_key("f2.parquet.crc", params["two_days_ago"], params): None,
            create_s3_key("f3.parquet.crc", params["two_days_ago"], params): None,
            create_s3_key("f4.parquet.crc", params["one_day_ago"], params): None,
            create_s3_key("f5.parquet.crc", params["today"], params): None,
            create_s3_key("f6.json", params["today"], params): None,
        }
    )

    # Act
    filepaths = s3_prefix_scan(conf).load(
        "s3://tmp-bucket/plantlib/live", ".parquet.crc"
    )

    # Assert
    assert len(filepaths) == 4

    # Check that a file registry delta files have been created
    data = (
        spark_session.read.load(params["file_registry_path"], format="delta")
        .orderBy("file_path")
        .collect()
    )
    assert len(data) == 4
    assert data[0][0] == "{}/{}/f2.parquet.crc".format(
        params["s3_path"], params["two_days_ago"].strftime("%Y/%m/%d")
    )
    assert data[0][1] == params["two_days_ago"].date()
    assert data[0][2] is None
    assert m_hive_table.called
    assert m_file_registry.called


@patch.object(S3PrefixScan, "_create_file_registry_path")
def test_previous_data_with_only_null_values(
    m_file_registry, s3_mock, spark_session, tmp_dir, helpers
):
    """When there is only null values and no new files."""
    # ARRANGE
    params = setup_params(tmp_dir, m_file_registry)
    conf = setup_bconf(
        params["file_registry_base"], params["two_days_ago"], spark_session
    )

    # Create existing file registry
    s3_files = [
        create_fr_row("f1.parquet.crc", params["one_day_ago"], params),
        create_fr_row("f3.parquet.crc", params["today"], params),
        create_fr_row("f4.parquet.crc", params["today"], params),
    ]
    create_file_registry(helpers, spark_session, s3_files, params["file_registry_path"])

    # Files in the s3 mock
    helpers.create_s3_files(
        {
            create_s3_key("f1.parquet.crc", params["one_day_ago"], params): None,
            create_s3_key("f3.parquet.crc", params["today"], params): None,
            create_s3_key("f4.parquet.crc", params["today"], params): None,
        }
    )

    # ACT
    filepaths = s3_prefix_scan(conf).load(params["s3_path"], suffix=".parquet.crc")

    # ASSERT
    base = "s3://tmp-bucket/plantlib/live"
    check_list = [
        f'{base}/{params["one_day_ago"].strftime("%Y/%m/%d")}/f1.parquet.crc',
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f3.parquet.crc',
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f4.parquet.crc',
    ]
    assert all(elem in check_list for elem in filepaths)


@patch.object(S3PrefixScan, "_create_file_registry_path")
def test_pbd_load_with_previous_data(
    m_file_registry, spark_session, s3_mock, tmp_dir, helpers
):
    """Test the load method for prefixed based date when there is prev data."""
    # ARRANGE
    params = setup_params(tmp_dir, m_file_registry)
    conf = setup_bconf(
        params["file_registry_base"], params["two_days_ago"], spark_session
    )

    # Create existing file registry
    s3_files = [
        create_fr_row(
            "f1.parquet.crc", params["one_day_ago"], params, params["one_day_ago"]
        ),
        create_fr_row(
            "f2.parquet.crc", params["one_day_ago"], params, params["one_day_ago"]
        ),
        create_fr_row("f3.parquet.crc", params["today"], params, params["today"]),
        create_fr_row("f4.parquet.crc", params["today"], params),
    ]
    create_file_registry(helpers, spark_session, s3_files, params["file_registry_path"])

    # Files in the s3 mock
    helpers.create_s3_files(
        {
            create_s3_key("f1.parquet.crc", params["one_day_ago"], params): None,
            create_s3_key("f2.parquet.crc", params["one_day_ago"], params): None,
            create_s3_key("f3.parquet.crc", params["today"], params): None,
            create_s3_key("f4.parquet.crc", params["today"], params): None,
            create_s3_key("f5.parquet.crc", params["today"], params): None,
        }
    )

    # ACT
    filepaths = s3_prefix_scan(conf).load(params["s3_path"], suffix=".parquet.crc")

    # ASSERT
    base = "s3://tmp-bucket/plantlib/live"
    assert m_file_registry.called
    check_list = [
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f4.parquet.crc',
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f5.parquet.crc',
    ]
    assert all(elem in check_list for elem in filepaths)


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
        "DefaultStartDate": "2020-01-01",
        "PartitionFormat": "%Y/%m/%d",
    }
    conf = BlockConfig("CurrentSection", None, None, props, BlockLog())
    pbd = S3PrefixScan(conf)

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
        "DefaultStartDate": "2020-01-01",
        "PartitionFormat": "%Y/%m/%d",
    }
    conf = BlockConfig("CurrentSection", spark, None, props, BlockLog())
    pbd = S3PrefixScan(conf)

    # Act
    pbd.file_registry_path = path
    pbd._create_hive_table()

    # Assert
    assert path in str(spark.sql.call_args[0])
    assert "file_registry_dev" in str(spark.sql.call_args_list[0][0])
    assert table in str(spark.sql.call_args_list[2][0])
