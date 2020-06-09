"""Unit tests helper functions for lift file."""
import oyaml as yaml

import getl.lift_definition as liftdef


# TESTS
def test_retrive_yaml_from_s3(helpers):
    """Get the yaml file from s3."""
    # Arrange
    path = "s3://tmp-bucket/raw_to_trusted.yaml"
    with open("tests/getl/data/lift/test.yaml") as yaml_file:
        yaml_file = yaml_file.read()
        helpers.create_s3_files({"raw_to_trusted.yaml": yaml_file})

    # Act
    raw_yaml = liftdef.fetch_lift_definition(path)

    # Arrange
    assert isinstance(raw_yaml, dict)
    assert "Parameters" in raw_yaml
    assert "LiftJob" in raw_yaml


def test_process_yaml_from_string():
    """Get the yaml file from s3."""
    # Arrange
    str_yaml = """
    Parameters:
        ReadPath:
            Description: The path given for the files in trusted

    LiftJob:
        TrustedFiles:
            Type: load::batch_parquet
            Properties:
                FileRegistry: PrefixBasedDate
                Path: ${ReadPath}
    """

    # Act
    raw_yaml = liftdef.fetch_lift_definition(str_yaml)

    # Arrange
    assert isinstance(raw_yaml, dict)
    assert "Parameters" in raw_yaml
    assert "LiftJob" in raw_yaml


def test_resolve_yaml_parameters():
    """Resolve parameters within the yaml."""
    # Arrange
    params = {
        "PathToRawFiles": "path1",
        "TrustedPlantShowPath": False,
        "TrustedSearchPath": "test",
    }
    with open("tests/getl/data/lift/test.yaml") as yaml_file:
        lift_def = yaml.safe_load(yaml_file.read())

    # Act
    resolved_lift_def = liftdef._replace_variables(lift_def, params)
    lift_job = resolved_lift_def["LiftJob"]
    file_sources = resolved_lift_def["FileSources"]["PrefixBasedDate"]

    # Assert
    assert lift_job["RawFiles"]["Path"] == "path1"
    assert lift_job["WriteToShowPlant"]["Path"] is False
    assert lift_job["WriteToSearch"]["Path"] == "test"
    assert lift_job["WriteToSearch"]["Path"] == "test"
    assert file_sources["Properties"]["FileSourceBasePrefix"] == "test"
