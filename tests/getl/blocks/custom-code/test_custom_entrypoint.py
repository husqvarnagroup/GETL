"""Unit tests for the custom code index function."""
import json
from pathlib import Path

from pyspark.sql import functions as F

BASE_DIR = Path(__file__).parent / "data"


def generate_test_data(path: Path):
    for i in range(10):
        json_file = path / f"{i}.json"
        with json_file.open("w") as f:
            json.dump({"name": "Alfred number {i}"}, f)
            f.write("\n")
            json.dump({"name": "Bobbette number {i}"}, f)


# TESTS
def test_custom_code_from_s3(helpers, spark_session, s3_mock, tmp_path):
    """Resolve code that is passed as a remote files in s3."""
    # Arrange
    generate_test_data(tmp_path)
    code_path = str(BASE_DIR / "custom.py")
    dataframe = spark_session.read.load(str(tmp_path), format="json")

    # Act
    dataframe = helpers.execute_code_block(
        dataframe, "PrevSection", code_path, pkg=["peewee"]
    )

    # Assert
    assert "newColumn" in dataframe.columns


def test_custom_code_passed_as_function(helpers, spark_session, s3_mock, tmp_path):
    """Resolve code that is passed as a function."""
    # Arrange
    generate_test_data(tmp_path)
    dataframe = spark_session.read.load(str(tmp_path), format="json")

    # Define custom function
    def new_column(params):
        dataframe = params["dataframes"]["PrevSection"]
        return dataframe.withColumn(params["ColumnName"], F.lit(None))

    # Act
    dataframe = helpers.execute_code_block(
        dataframe, "PrevSection", new_column, extra_props={"ColumnName": "newColumn"}
    )

    # Assert
    assert "newColumn" in dataframe.columns
