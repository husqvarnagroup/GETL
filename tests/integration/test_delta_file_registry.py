import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Tuple

import pyspark
import pytest
from pyspark.sql import SparkSession, functions as F

from getl.lift import lift

minversion = pytest.mark.skipif(
    pyspark.__version__ < "3.0",
    reason="file_registry:::delta_diff not supported for pyspark 2.x",
)

LIFT_YAML = """
Parameters:
    ReadPath:
        Description: The path given for the files in trusted
    DefaultStartDate:
        Description: Start date
    FileRegistryPath:
        Description: The path to the file registry

FileRegistry:
    DeltaFR:
        Type: fileregistry::delta_diff
        Properties:
            BasePath: ${FileRegistryPath}
            DefaultStartDate: ${DefaultStartDate}
            UpdateAfter: Read
            JoinOnFields:
                - id

LiftJob:
    Read:
        Type: load::batch_delta
        Properties:
            Path: ${ReadPath}
            FileRegistry: DeltaFR

"""


@dataclass
class DataWriter:
    spark_session: SparkSession
    path: str
    columns: List[str]

    def write(self, data: List[Tuple]):
        self.spark_session.createDataFrame(data, self.columns).write.save(
            path=self.path, format="delta", mode="overwrite",
        )


@minversion
def test_delta_diff(spark_session, tmp_path):
    delta_path = str(tmp_path / "delta_test")
    fr_path = str(tmp_path / "file_registry")

    data_writer = DataWriter(spark_session, delta_path, ["id", "name"])
    data_writer.write([(0, "Z")])

    df_history = spark_session.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
    start_date = (
        df_history.orderBy(F.col("timestamp").asc())
        .select("timestamp")
        .first()
        .timestamp
    )

    params = {
        "ReadPath": delta_path,
        "FileRegistryPath": fr_path,
        "DefaultStartDate": f"{start_date:%Y-%m-%d %H:%M:%S}",
    }
    data_writer.write([(1, "A"), (2, "B")])

    # Act
    history = lift(spark=spark_session, lift_def=LIFT_YAML, parameters=params)

    # Assert
    dataframe = history.get("Read")
    data = sorted(map(tuple, dataframe.collect()))
    expected = [
        (1, "A"),
        (2, "B"),
    ]
    assert data == expected

    # Change data and relift
    data_writer.write([(2, "B"), (3, "C")])

    history = lift(spark=spark_session, lift_def=LIFT_YAML, parameters=params)

    # Assert
    dataframe = history.get("Read")
    data = sorted(map(tuple, dataframe.collect()))
    expected = [
        (3, "C"),
    ]
    assert data == expected

    # Relift with no change in data

    history = lift(spark=spark_session, lift_def=LIFT_YAML, parameters=params)

    # Assert
    dataframe = history.get("Read")
    data = sorted(map(tuple, dataframe.collect()))
    expected = []
    assert data == expected


@minversion
def test_start_date_before_first_commit(spark_session, tmp_path):
    delta_path = str(tmp_path / "delta_test")
    fr_path = str(tmp_path / "file_registry")

    data_writer = DataWriter(spark_session, delta_path, ["id", "name"])
    start_date = datetime.utcnow() - timedelta(seconds=1)
    params = {
        "ReadPath": delta_path,
        "FileRegistryPath": fr_path,
        "DefaultStartDate": f"{start_date:%Y-%m-%d %H:%M:%S}",
    }
    data_writer.write([(1, "A"), (2, "B")])

    # Act
    with pytest.raises(ValueError) as exc_info:
        lift(spark=spark_session, lift_def=LIFT_YAML, parameters=params)
    assert re.match(
        r"^Start date is earlier than first available timestamp: .*, start time is set to .*$",
        str(exc_info.value),
    )
