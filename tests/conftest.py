"""ConfTest fixture for SparkSession and logger."""
import logging
import os
from datetime import datetime
from pathlib import Path

import boto3
import mysql.connector
import oyaml
import psycopg2
import pytest
from moto import mock_s3
from pyspark.sql import SparkSession

from getl.block import BlockConfig, BlockLog
from getl.blocks.custom.entrypoint import python_codeblock

if os.environ.get("TZ") != "UTC":
    raise ValueError("Environmental variable 'TZ' must be set to 'UTC'")

BASE_DIR = Path(__file__).parent.parent


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session():
    """Return a sparksession fixture."""
    spark_builder = (
        SparkSession.builder.master("local[*]")
        .appName("pysparktest")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.driver.memory", "2g")
    )
    spark_jars = []

    # Get latest delta core:
    # https://mvnrepository.com/artifact/io.delta/delta-core
    spark_jars.append("./tests/testing-jars/delta-core_2.12-0.8.0.jar")
    spark_jars.append("./tests/testing-jars/spark-xml_2.12-0.9.0.jar")

    spark_builder = (
        spark_builder.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = spark_builder.config("spark.jars", ",".join(spark_jars)).getOrCreate()
    for jar in spark_jars:
        spark.sparkContext.addPyFile(jar)

    quiet_py4j()

    yield spark


@pytest.fixture(scope="function")
def s3_mock():
    """Mock boto3 using moto library."""
    with mock_s3():
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
        self.s3_mock.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west1"}
        )
        for key, body in files.items():
            body = body if body else b"Here we have some test data"
            self.s3_mock.put_object(Bucket=bucket, Key=key, Body=body)


@pytest.fixture(scope="function")
def helpers(s3_mock, spark_session):
    return Helpers(s3_mock, spark_session)


@pytest.fixture(
    scope="module", params=["postgres10", "postgres11", "postgres12", "postgres13"]
)
def postgres_port(request):
    docker_compose = BASE_DIR / "docker-compose.yaml"
    assert docker_compose.exists(), "docker-compose.yaml not found"
    dc = oyaml.safe_load(docker_compose.read_text())
    return dc["services"][request.param]["ports"][0].split(":")[0]


@pytest.fixture(scope="module")
def postgres_connection_details(postgres_port):
    return {
        "dsn": f"postgres://localhost:{postgres_port}/testdb",
        "user": "dbadmin",
        "password": "mintkaka2010",
    }


@pytest.fixture(scope="function")
def postgres_connection(postgres_connection_details):
    with psycopg2.connect(**postgres_connection_details) as conn:
        yield conn


@pytest.fixture
def postgres_cursor(postgres_connection):
    with postgres_connection.cursor() as cursor:
        yield cursor


@pytest.fixture(scope="module", params=["mysql56", "mysql57", "mysql8"])
def mysql_port(request):
    docker_compose = BASE_DIR / "docker-compose.yaml"
    assert docker_compose.exists(), "docker-compose.yaml not found"
    dc = oyaml.safe_load(docker_compose.read_text())
    return dc["services"][request.param]["ports"][0].split(":")[0]


@pytest.fixture(scope="module")
def mysql_connection_details(mysql_port):
    return {
        "host": "localhost",
        "port": mysql_port,
        "database": "testdb",
        "user": "dbadmin",
        "password": "mintkaka2010",
    }


@pytest.fixture(scope="function")
def mysql_connection(mysql_connection_details):
    with mysql.connector.connect(**mysql_connection_details) as conn:
        yield conn


@pytest.fixture(scope="function")
def mysql_cursor(mysql_connection):
    with mysql_connection.cursor() as cursor:
        yield cursor
