"""Unit test for GETL utils function."""

from unittest import mock

import pytest
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType

from getl.common.s3path import S3Path
from getl.common.utils import (
    copy_and_cleanup,
    copy_keys,
    delete_files,
    json_to_spark_schema,
)
from tests.getl.data.utils.example_schema import create_json_schema


@mock.patch("getl.common.utils.StructType")
def test_json_to_spark_schema_correct_params(m_struct):
    """json_to_spark_schema is called with right parameters in the right order."""
    # Arrange & Act
    json_to_spark_schema(create_json_schema())

    # Assert
    m_struct.fromJson.assert_called_with(create_json_schema())


def test_json_to_spark_schema():
    """json_to_spark_schema should load the json schema as StructType."""
    # Arrange & Act
    result_schema = json_to_spark_schema(create_json_schema())

    # Assert
    assert isinstance(result_schema, StructType)


@pytest.mark.parametrize(
    "invalid_schema, missed_key",
    [
        ("missing_name", "name"),
        ("missing_type", "type"),
        ("missing_nullable", "nullable"),
        ("missing_type_and_name", "name"),
        ("missing_metadata", "metadata"),
    ],
)
def test_json_to_spark_schema_invalid(invalid_schema, missed_key):
    """json_to_spark_schema should raise KeyError for missing key."""
    # Arrange & Act
    with pytest.raises(KeyError) as key_error:
        json_to_spark_schema(create_json_schema(invalid_schema))

    # Assert
    assert "Missing key: '{0}'. Valid format: {1}".format(
        missed_key, "All schema columns must have a name, type and nullable key"
    ) in str(key_error)


@pytest.mark.parametrize("invalid_json", ["invalid", {"invalid"}])
def test_json_to_spark_invalid_json(invalid_json):
    """json_to_spark_schema should raise TypeError for invalid json."""
    # Arrange & Act
    with pytest.raises(TypeError) as type_error:
        json_to_spark_schema(invalid_json)

    # Assert
    assert "Invalid json was provided" in str(type_error)


@pytest.mark.parametrize(
    "paths, bucket, files",
    [
        ([], "landingzone", []),
        (
            [
                "s3://landingzone/amc-connect/file.json",
                "s3a://landingzone/amc-connect/test/file.json",
            ],
            "landingzone",
            ["amc-connect/test/file.json", "amc-connect/test/file.json"],
        ),
    ],
)
def test_delete_files_success(s3_mock, paths, bucket, files, helpers):
    """delete_files should remove files successfully."""
    # Arrange
    helpers.create_s3_files({f: f for f in files}, bucket=bucket)

    # Act & Assert
    assert delete_files(paths) is None

    for _file in files:
        with pytest.raises(ClientError) as excinfo:
            s3_mock.get_object(Bucket=bucket, Key=_file)

        assert "NoSuchKey" in str(excinfo)


@pytest.mark.parametrize(
    "paths,bucket",
    [
        ([], "landingzone"),
        (
            [
                "s3://landingzone/amc-connect/file.json",
                "s3a://landingzone/amc-connect/test/file.json",
            ],
            "landingzone",
        ),
    ],
)
def test_delete_files_success_nofile(s3_mock, paths, bucket):
    """delete_files should run successfully even when files not found."""
    # Arrange
    s3_mock.create_bucket(
        Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west1"}
    )

    # Act & Assert
    assert delete_files(paths) is None


def test_copy_keys_passes_correct_parameters(s3_mock):
    """copy_keys is called with right parameters and in right order."""
    s3_mock.create_bucket(
        Bucket="landingzone",
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )
    s3_mock.create_bucket(
        Bucket="datalake", CreateBucketConfiguration={"LocationConstraint": "eu-west1"}
    )
    S3Path("s3://landingzone/amc-connect/fake/key.json").write_text(
        '{"hello": "world"}'
    )

    # Act
    copy_keys(
        [("landingzone/amc-connect/fake/key.json", "datalake/amc/raw/fake/key.json")]
    )

    assert (
        S3Path("s3://datalake/amc/raw/fake/key.json").read_text()
        == '{"hello": "world"}'
    )


@pytest.mark.parametrize(
    "transactions,source_bucket,target_bucket,files",
    [
        ([], "tmp-bucket", "tmp-bucket", {"create_files": [], "check_files": []}),
        (
            [("landingzone/amc-connect/file.json", "datalake/amc/raw/file.json")],
            "landingzone",
            "datalake",
            {
                "create_files": ["amc-connect/file.json"],
                "check_files": ["amc/raw/file.json"],
            },
        ),
        (
            [
                ("landingzone/amc-connect/file.json", "datalake/amc/raw/file.json"),
                ("landingzone/amc-connect/file2.json", "datalake/amc/raw/file2.json"),
                (
                    "landingzone/amc-connect/test/file.json",
                    "datalake/amc/raw/test/file.json",
                ),
            ],
            "landingzone",
            "datalake",
            {
                "create_files": [
                    "amc-connect/file.json",
                    "amc-connect/file2.json",
                    "amc-connect/test/file.json",
                ],
                "check_files": [
                    "amc/raw/file.json",
                    "amc/raw/file2.json",
                    "amc/raw/test/file.json",
                ],
            },
        ),
    ],
)
def test_copy_keys_successful(
    helpers, s3_mock, transactions, source_bucket, target_bucket, files
):
    """copy_keys should copy files to target location."""
    # Arrange
    helpers.create_s3_files({f: f for f in files["create_files"]}, bucket=source_bucket)
    if source_bucket != target_bucket:
        s3_mock.create_bucket(
            Bucket=target_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
        )

    # Act & Assert
    assert copy_keys(transactions) is None

    for target_file in files["check_files"]:
        res = s3_mock.get_object(Bucket=target_bucket, Key=target_file)
        assert res["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.parametrize(
    "transactions,source_bucket,target_bucket,error_msg",
    [
        (
            [("landingzone/amc-connect/file.json", "datalake/amc/raw/file.json")],
            "landingzone",
            "datalake",
            "Not Found",
        ),
        (
            [("landingzone/amc-connect/file.json", "datalake/amc/raw/file.json")],
            "wrong_src_bkt",
            "wrong_tgt_bkt",
            "The specified bucket landingzone does not exist",
        ),
    ],
)
def test_copy_keys_throws_exceptions(
    s3_mock, transactions, source_bucket, target_bucket, error_msg
):
    """copy_keys throws exception when files or bucket not found."""
    # Arrange
    s3_mock.create_bucket(
        Bucket=target_bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )
    s3_mock.create_bucket(
        Bucket=source_bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )

    # Act & Assert
    with pytest.raises(FileNotFoundError) as file_not_found:
        copy_keys(transactions)

    assert error_msg in str(file_not_found)


@mock.patch("getl.common.utils.delete_files")
@mock.patch("getl.common.utils.copy_keys")
def test_copy_and_cleanup_pass_parameters(m_copy, m_delete):
    """copy_keys is called with right parameters and in right order."""
    # Arrange & Act
    copy_and_cleanup([("bucket/key", "bucket/key2")])

    # Assert
    m_copy.assert_called_once_with([("bucket/key", "bucket/key2")])
    m_delete.assert_called_once_with(["bucket/key"])


@mock.patch("getl.common.utils.delete_files")
@mock.patch("getl.common.utils.copy_keys")
def test_copy_and_cleanup_call_order(m_copy, m_delete):
    """copy_keys is called with copy and delete functions in right order."""
    # Arrange & Act
    manager = mock.Mock()
    manager.attach_mock(m_copy, "c")
    manager.attach_mock(m_delete, "d")

    copy_and_cleanup([("from", "to")])

    # Assert
    expected_call = [mock.call.c([("from", "to")]), mock.call.d(["from"])]
    assert manager.mock_calls == expected_call
