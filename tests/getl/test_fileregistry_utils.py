from datetime import datetime

from pyspark.sql import types as T

import getl.fileregistry.fileregistry_utils as fr_utils


def test_doesnt_exist(spark_session, tmp_path):
    assert (
        fr_utils.fetch_file_registry(str(tmp_path / "unknown"), spark_session) is None
    )


def test_empty(spark_session, tmp_path):
    assert fr_utils.fetch_file_registry(str(tmp_path), spark_session) is None


def test_no_data(spark_session, tmp_path):
    file_registry_path = str(tmp_path)

    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("prefix_date", T.TimestampType(), True),
            T.StructField("date_lifted", T.TimestampType(), True),
        ]
    )
    dataframe = spark_session.createDataFrame([], schema)
    dataframe.write.save(path=file_registry_path, format="delta", mode="overwrite")

    assert fr_utils.fetch_file_registry(file_registry_path, spark_session) is None


def test_has_data(spark_session, tmp_path):
    file_registry_path = str(tmp_path)

    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("prefix_date", T.TimestampType(), True),
            T.StructField("date_lifted", T.TimestampType(), True),
        ]
    )
    dataframe = spark_session.createDataFrame(
        [("/path/to/file", datetime.now(), None)], schema
    )
    dataframe.write.save(path=file_registry_path, format="delta", mode="overwrite")

    assert fr_utils.fetch_file_registry(file_registry_path, spark_session) is not None
