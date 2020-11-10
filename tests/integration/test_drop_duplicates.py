import tempfile

import pytest

from getl.lift import lift

LIFT_YAML = """
Parameters:
    ReadPath:
        Description: The path given for the files in trusted

LiftJob:
    Read:
        Type: load::batch_parquet
        Properties:
            Path: ${ReadPath}

    DropColumns:
        Type: transform::generic
        Input: Read
        Properties:
            Functions:
                - transform.drop_duplicates:
                    columns: [from, to]

    DropAll:
        Type: transform::generic
        Input: Read
        Properties:
            Functions:
                - transform.drop_duplicates

"""


@pytest.fixture(scope="session")
def generate_data(spark_session):
    data = [
        ("London", "Brussels", 500),
        ("London", "Brussels", 490),
        ("Brussels", "Stockholm", 1000),
        ("Brussels", "Stockholm", 1000),
        ("Stockholm", "Brussels", 1100),
        ("London", "Stockholm", 1300),
    ]

    with tempfile.TemporaryDirectory() as tmp_path:
        (
            spark_session.createDataFrame(data, ["from", "to", "cost"]).write.save(
                path=f"file://{tmp_path}",
                format="parquet",
                mode="overwrite",
                mergeSchema=True,
            )
        )
        yield str(tmp_path)


def test_drop_duplicates_selected_columns(spark_session, tmp_dir, generate_data):
    """Try to lift data with a yaml file defined as a string."""
    # Arrange
    params = {
        "ReadPath": generate_data,
    }

    # Act
    history = lift(spark=spark_session, lift_def=LIFT_YAML, parameters=params)

    # Assert
    dataframe = history.get("DropColumns")
    assert dataframe.count() == 4
    expected = [
        ("Brussels", "Stockholm"),
        ("London", "Brussels"),
        ("London", "Stockholm"),
        ("Stockholm", "Brussels"),
    ]
    assert sorted(map(tuple, dataframe.select("from", "to").collect())) == expected


def test_drop_duplicates_all(spark_session, tmp_dir, generate_data):
    """Try to lift data with a yaml file defined as a string."""
    # Arrange

    params = {
        "ReadPath": generate_data,
    }

    # Act
    history = lift(spark=spark_session, lift_def=LIFT_YAML, parameters=params)

    # Assert
    dataframe = history.get("DropAll")
    assert dataframe.count() == 5
    expected = [
        ("Brussels", "Stockholm", 1000),
        ("London", "Brussels", 490),
        ("London", "Brussels", 500),
        ("London", "Stockholm", 1300),
        ("Stockholm", "Brussels", 1100),
    ]
    assert (
        sorted(map(tuple, dataframe.select("from", "to", "cost").collect())) == expected
    )
