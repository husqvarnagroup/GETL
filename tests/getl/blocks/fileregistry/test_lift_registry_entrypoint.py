"""Unit tests for the custom code index function."""
from datetime import datetime, timedelta

import pytest
from mock import Mock, patch
from pyspark.sql import functions as F

from getl.block import BlockConfig, BlockLog
from getl.blocks.fileregistry.entrypoint import s3_date_prefix_scan
from getl.blocks.fileregistry.s3_date_prefix_scan import (
    S3DatePrefixScan,
    get_dates_in_format,
)


# HELPERS
def create_fr_row(filename, date, params, lift_date=None):
    return (
        f'{params["s3_path"]}/{date.strftime("%Y/%m/%d")}/{filename}',
        date,
        lift_date,
    )


def create_s3_key(filename, date, params):
    return f'{params["prefix_source"]}/{date.strftime("%Y/%m/%d")}/{filename}'


def setup_params(tmp_dir):
    # Arrange
    prefix_source = "plantlib/live"
    file_registry_base = "{}/file-registry".format(tmp_dir)
    file_registry_path = "{}/plantlib/live".format(file_registry_base)

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
        "BasePath": base_prefix,
        "UpdateAfter": "OtherSection",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": "example",
        "DefaultStartDate": start_date.strftime("%Y-%m-%d"),
        "PartitionFormat": "%Y/%m/%d",
    }

    return BlockConfig("CurrentSection", spark_session, None, props, BlockLog())


def create_file_registry(helpers, spark, files, file_registry_path):
    data = helpers.convert_events_to_datetime(files, "%Y/%m/%d")
    current_df = spark.createDataFrame(data, S3DatePrefixScan.schema)
    current_df = current_df.where(~F.col("file_path").contains("f4.parquet"))
    current_df.write.save(path=file_registry_path, format="delta", mode="overwrite")


@patch.object(S3DatePrefixScan, "_create_hive_table")
def test_pbd_load_no_previous_data(m_hive_table, spark_session, helpers, tmp_dir):
    """Test the load method for prefixed based date when there is no prev data."""
    # Arrange
    params = setup_params(tmp_dir)
    conf = setup_bconf(
        params["file_registry_path"], params["two_days_ago"], spark_session
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
    filepaths = s3_date_prefix_scan(conf).load(
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
    assert data[0][1].date() == params["two_days_ago"].date()
    assert data[0][2] is None
    assert m_hive_table.called


def test_previous_data_with_only_null_values(s3_mock, spark_session, tmp_dir, helpers):
    """When there is only null values and no new files."""
    # ARRANGE
    params = setup_params(tmp_dir)
    conf = setup_bconf(
        params["file_registry_base"], params["two_days_ago"], spark_session
    )

    # Create existing file registry
    s3_files = [
        create_fr_row("f1.parquet.crc", params["one_day_ago"], params),
        create_fr_row("f3.parquet.crc", params["today"], params),
        create_fr_row("f4.parquet.crc", params["today"], params),
    ]
    create_file_registry(helpers, spark_session, s3_files, params["file_registry_base"])

    # Files in the s3 mock
    helpers.create_s3_files(
        {
            create_s3_key("f1.parquet.crc", params["one_day_ago"], params): None,
            create_s3_key("f3.parquet.crc", params["today"], params): None,
            create_s3_key("f4.parquet.crc", params["today"], params): None,
        }
    )

    # ACT
    filepaths = s3_date_prefix_scan(conf).load(params["s3_path"], suffix=".parquet.crc")

    # ASSERT
    base = "s3://tmp-bucket/plantlib/live"
    check_list = [
        f'{base}/{params["one_day_ago"].strftime("%Y/%m/%d")}/f1.parquet.crc',
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f3.parquet.crc',
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f4.parquet.crc',
    ]
    assert all(elem in check_list for elem in filepaths)


def test_pbd_load_with_previous_data(spark_session, s3_mock, tmp_dir, helpers):
    """Test the load method for prefixed based date when there is prev data."""
    # ARRANGE
    params = setup_params(tmp_dir)
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
    create_file_registry(helpers, spark_session, s3_files, params["file_registry_base"])

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
    filepaths = s3_date_prefix_scan(conf).load(params["s3_path"], suffix=".parquet.crc")

    # ASSERT
    base = "s3://tmp-bucket/plantlib/live"
    check_list = [
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f4.parquet.crc',
        f'{base}/{params["today"].strftime("%Y/%m/%d")}/f5.parquet.crc',
    ]
    assert all(elem in check_list for elem in filepaths)


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
        "BasePath": path,
        "UpdateAfter": "",
        "HiveDatabaseName": "file_registry_dev",
        "HiveTableName": table,
        "DefaultStartDate": "2020-01-01",
        "PartitionFormat": "%Y/%m/%d",
    }
    conf = BlockConfig("CurrentSection", spark, None, props, BlockLog())
    pbd = S3DatePrefixScan(conf)

    # Act
    pbd._create_hive_table()

    # Assert
    assert path in str(spark.sql.call_args[0])
    assert "file_registry_dev" in str(spark.sql.call_args_list[0][0])
    assert table in str(spark.sql.call_args_list[2][0])


@pytest.mark.parametrize(
    "start,stop,fmt,result",
    [
        (datetime(2020, 8, 31), datetime(2021, 1, 1), "%Y", ["2020", "2021"]),
        (
            datetime(2020, 8, 31),
            datetime(2020, 12, 1),
            "%Y-%m",
            ["2020-08", "2020-09", "2020-10", "2020-11", "2020-12"],
        ),
        (
            datetime(2020, 9, 29, 23, 59),
            datetime(2020, 10, 3),
            "%Y-%m-%d",
            ["2020-09-29", "2020-09-30", "2020-10-01", "2020-10-02", "2020-10-03"],
        ),
        (
            datetime(2020, 9, 15, 8, 59),
            datetime(2020, 9, 15, 13),
            "%Y-%m-%d %H",
            [
                "2020-09-15 08",
                "2020-09-15 09",
                "2020-09-15 10",
                "2020-09-15 11",
                "2020-09-15 12",
                "2020-09-15 13",
            ],
        ),
    ],
)
def test_dates_in_format(start, stop, fmt, result):
    assert list(get_dates_in_format(start, stop, fmt)) == result


def test_dates_in_format_invalid():
    with pytest.raises(ValueError):
        assert list(
            get_dates_in_format(
                datetime(2020, 9, 15), datetime(2020, 9, 25), "same output"
            )
        )
