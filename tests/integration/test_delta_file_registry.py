from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Tuple

from pyspark.sql import SparkSession, functions as F

from getl.lift import lift

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
            path=self.path,
            format="delta",
            mode="overwrite",
        )


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


def test_start_date_before_first_commit_loads_all_data(spark_session, tmp_path):
    delta_path = str(tmp_path / "delta_test")
    fr_path = str(tmp_path / "file_registry")

    data_writer = DataWriter(spark_session, delta_path, ["id", "name"])
    start_date = datetime.utcnow() - timedelta(seconds=1)
    params = {
        "ReadPath": delta_path,
        "FileRegistryPath": fr_path,
        "DefaultStartDate": f"{start_date:%Y-%m-%d %H:%M:%S}",
    }
    expected = [(1, "A"), (2, "B")]
    data_writer.write(expected)

    # Act
    history = lift(spark=spark_session, lift_def=LIFT_YAML, parameters=params)

    # Assert
    dataframe = history.get("Read")
    data = sorted(map(tuple, dataframe.collect()))
    assert data == expected
