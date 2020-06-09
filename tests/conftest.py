"""ConfTest fixture for SparkSession and logger."""
import logging
import os
from datetime import datetime
from pathlib import Path

import boto3
import pytest
from moto import mock_s3
from pyspark.sql import SparkSession

from getl.block import BlockConfig, BlockLog
from getl.blocks.custom.entrypoint import python_codeblock


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(autouse=True)
def set_timezone():
    os.environ["TZ"] = "UTC"


@pytest.fixture(scope="session")
def spark_session():
    """Return a sparksession fixture."""
    delta_jar = "./tests/testing-jars/delta-core_2.11-0.4.0.jar"

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pysparktest")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.driver.memory", "2g")
        .config("spark.jars", delta_jar)
        .getOrCreate()
    )

    spark.sparkContext.addPyFile(delta_jar)
    quiet_py4j()

    yield spark


@mock_s3
@pytest.fixture(scope="function")
def s3_mock():
    """Mock boto3 using moto library."""
    mock_s3().start()
    yield boto3.client("s3")


@pytest.fixture(scope="function")
def tmp_dir(tmpdir):
    """Return a tmp dir folder to write to that is then cleaned up."""
    yield str(tmpdir)


class Helpers:
    def __init__(self, _s3_mock, _spark_session):
        self.s3_mock = _s3_mock
        self.spark_session = _spark_session

    def execute_code_block(self, dataframe, input_name, code, extra_props={}, pkg=[]):
        """Wrapper for executing a python code block."""
        if not callable(code):
            self.create_s3_files({"custom.py": Path(code).read_text()})
            code_props = {"CustomCodePath": "s3://tmp-bucket/custom.py"}

        else:
            code_props = {"CustomFunction": code}

        props = {**code_props, "Packages": pkg, "CustomProps": extra_props}

        bconf = self.create_block_conf(dataframe, props, [input_name], input_name)

        return python_codeblock(bconf)

    def create_block_conf(
        self,
        dataframe,
        props,
        block_input="PrevSection",
        prev_input_name="PrevSection",
        spark=None,
        history=BlockLog(),
        file_registry=BlockLog(),
    ):
        """Create a block config."""
        spark = spark if spark else self.spark_session
        history.add(BlockConfig(prev_input_name, spark, "", {}), dataframe)

        return BlockConfig(
            "CurrentBlock",
            spark,
            block_input=block_input,
            props=props,
            history=history,
            file_registry=file_registry,
        )

    def convert_event_to_datetime(self, event, time_format):
        """Process the test result events."""
        result = []
        for item in event:
            if self.is_date(item, time_format):
                result.append(self.is_date(item, time_format))
            else:
                result.append(item)

        return tuple(result)

    def convert_events_to_datetime(self, events, time_format="%Y-%m-%d %H:%M:%S"):
        """Convert all strings in events to datetime objects."""
        return [self.convert_event_to_datetime(event, time_format) for event in events]

    def is_date(self, date_text, time_format):
        """Converts var to timestamp if it has correct time format"""
        try:
            return datetime.strptime(date_text, time_format)
        except Exception:
            return False

    def relative_path(self, current_file, path):
        """Create a relative path from where the files is."""
        path_to_current_file = os.path.realpath(current_file)
        current_directory = os.path.split(path_to_current_file)[0]
        return str(Path(current_directory) / path)

    def create_s3_files(self, files, bucket="tmp-bucket") -> None:
        """Create files in S3 bucket."""
        self.s3_mock.create_bucket(Bucket=bucket)
        for key, body in files.items():
            body = body if body else b"Here we have some test data"
            self.s3_mock.put_object(Bucket=bucket, Key=key, Body=body)


@pytest.fixture(scope="function")
def helpers(s3_mock, spark_session):
    return Helpers(s3_mock, spark_session)
