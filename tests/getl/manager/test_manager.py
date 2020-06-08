"""Unit tests for the manager class that is the controller of the ETL process."""
import json
from collections import OrderedDict
from pathlib import Path

from getl.manager import Manager


def generate_test_data(path: Path):
    path.mkdir()
    for i in range(10):
        json_file = path / f"{i}.json"
        with json_file.open("w") as f:
            json.dump({"name": "Alfred number {i}"}, f)
            f.write("\n")
            json.dump({"name": "Bobbette number {i}"}, f)


def test_load_and_write(spark_session, tmp_path, helpers):
    """Load json file and write it to another location as parquet."""
    # Arrange
    helpers.create_s3_files(
        {
            "schema.json": json.dumps(
                {
                    "fields": [
                        {
                            "metadata": {},
                            "name": "name",
                            "nullable": False,
                            "type": "string",
                        }
                    ]
                }
            )
        }
    )
    json_path = tmp_path / "json"
    generate_test_data(json_path)

    manager = Manager(spark_session)
    lift_definition = OrderedDict(
        [
            (
                "LoadFromRaw",
                {
                    "Type": "load::stream_json",
                    "Properties": {
                        "Path": str(tmp_path / "json"),
                        "SchemaPath": "s3://tmp-bucket/schema.json",
                    },
                },
            ),
            (
                "WriteToTrusted",
                {
                    "Input": "LoadFromRaw",
                    "Type": "write::stream_delta",
                    "Properties": {
                        "Path": str(tmp_path / "delta"),
                        "OutputMode": "append",
                    },
                },
            ),
        ]
    )

    # Act
    manager.execute_lift_job(lift_definition)

    # Assert
    assert (
        spark_session.read.load(str(tmp_path / "delta"), format="delta").count() == 20
    )
