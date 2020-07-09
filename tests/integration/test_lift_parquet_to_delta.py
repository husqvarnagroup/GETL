"""Integration test for lifting parquet files to delta files."""
import json
import tempfile
from os import walk
from pathlib import Path

import pytest
from mock import patch
from pyspark.sql import functions as F

from getl.blocks.fileregistry.s3_date_prefix_scan import S3DatePrefixScan
from getl.lift import lift

# CONSTANTS
LIFT_YAML = Path(__file__).parent / "lift.yaml"


@pytest.fixture(scope="session")
def generate_data(spark_session):

    sc = spark_session.sparkContext

    data = [
        {"from": "London", "to": "Brussels", "date": "2020-01-5"},
        {"from": "Brussels", "to": "Stockholm", "date": "2020-01-10"},
        {"from": "Stockholm", "to": "HQ", "date": "2020-02-13"},
        {"from": "HQ", "to": "Stockholm", "date": "2020-02-13"},
        {"from": "Stockholm", "to": "HQ", "date": "2020-02-14"},
        {"from": "HQ", "to": "Stockholm", "date": "2020-02-14"},
        {"from": "Stockholm", "to": "HQ", "date": "2020-02-15"},
        {"from": "HQ", "to": "Stockholm", "date": "2020-02-15"},
    ]

    with tempfile.TemporaryDirectory() as tmp_path:
        (
            spark_session.read.option("multiline", "true")
            .json(sc.parallelize([json.dumps(data)]))
            .withColumn("date", F.col("date").cast("date"))
            .withColumn("year", F.date_format(F.col("date"), "YYYY"))
            .withColumn("month", F.date_format(F.col("date"), "MM"))
            .withColumn("day", F.date_format(F.col("date"), "dd"))
            .write.partitionBy("year", "month", "day")
            .save(
                path=f"file://{tmp_path}",
                format="parquet",
                mode="overwrite",
                mergeSchema=True,
            )
        )
        yield str(tmp_path)


def get_file_names(path, suffix="parquet"):
    """Get all file names recursivly from path:"""
    files = {}
    for (dirpath, dirnames, filenames) in walk(path):
        for filename in filenames:
            if filename.endswith(suffix):
                files["{}/{}".format(dirpath, filename)] = None

    return files


@patch("getl.blocks.load.entrypoint._batch_read")
@patch.object(S3DatePrefixScan, "_create_hive_table")
def test_lift_parquet_to_delta(
    m_hive_table, m_batch_read, spark_session, s3_mock, helpers, generate_data, tmp_dir,
):
    """Lift parquet files to delta, with no previus file registry."""
    # Arrange
    base_path_filesystem = generate_data
    read_path = "s3://tmp-bucket{}".format(base_path_filesystem)
    write_path = "{}/files".format(tmp_dir)
    file_registry_path = "{}/file_registry/{}".format(tmp_dir, base_path_filesystem)

    # Mock spark load
    m_batch_read.return_value = spark_session.read.load(
        base_path_filesystem, format="parquet"
    )

    # Configure s3 mock
    helpers.create_s3_files(
        {"lift.yaml": LIFT_YAML.read_text(), **get_file_names(base_path_filesystem)}
    )

    params = {
        "ReadPath": read_path,
        "WritePath": write_path,
        "FileRegistryBasePrefix": file_registry_path,
    }

    # Act
    lift(spark=spark_session, lift_def="s3://tmp-bucket/lift.yaml", parameters=params)

    # Assert
    assert spark_session.read.load(write_path, format="delta").count() == 8
    assert spark_session.read.load(file_registry_path, format="delta").count() == 5
    assert m_hive_table.called
    assert m_batch_read.called


@patch("getl.blocks.load.entrypoint._batch_read")
@patch.object(S3DatePrefixScan, "load")
def test_no_new_data_to_lift(
    m_load, m_batch_read, spark_session, tmp_dir, helpers, generate_data
):
    """Do not lift if there is no new files."""
    # Arrange
    # base_path_filesystem = "{}/parquet".format(BASE_PATH)
    # read_path = "s3://tmp-bucket/{}".format(base_path_filesystem)
    write_path = "{}/files".format(tmp_dir)
    m_load.return_value = []

    # Configure s3 mock
    helpers.create_s3_files({"lift.yaml": LIFT_YAML.read_text()})

    params = {
        "ReadPath": generate_data,
        "WritePath": write_path,
        "FileRegistryBasePrefix": "",
    }

    # Act
    lift(spark=spark_session, lift_def="s3://tmp-bucket/lift.yaml", parameters=params)

    # Assert
    assert not m_batch_read.called


def test_string_lift_def_yaml(spark_session, tmp_dir, generate_data):
    """Try to lift data with a yaml file defined as a string."""
    # Arrange
    str_yaml = """
    Parameters:
        ReadPath:
            Description: The path given for the files in trusted
        CustomFunction:
            Description: The custom function that is needed for life on this plannet

    LiftJob:
        TrustedFiles:
            Type: load::batch_parquet
            Properties:
                Path: ${ReadPath}

        TransformData:
            Type: custom::python_codeblock
            Input:
                - TrustedFiles
            Properties:
                CustomFunction: ${CustomFunction}
                CustomProps:
                    ColumnName: columnKing

    """

    def custom_function(params):
        dataframe = params["dataframes"]["TrustedFiles"]
        return dataframe.withColumn(params["ColumnName"], F.lit(None))

    params = {
        "ReadPath": generate_data,
        "CustomFunction": custom_function,
    }

    # Act
    history = lift(spark=spark_session, lift_def=str_yaml, parameters=params)

    # Assert
    dataframe = history.get("TransformData")
    assert "columnKing" in dataframe.columns
    assert dataframe.count() == 8
