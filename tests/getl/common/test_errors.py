from contextlib import contextmanager

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_s3


@mock_s3
@pytest.fixture(scope="function")
def s3_mock():
    """Mock boto3 using moto library."""
    mock_s3().start()
    yield boto3.client("s3")


@contextmanager
def handle_client_error():
    """Raises other exceptions depending on the error code.

    Converts the following codes to a different exception:
    - NoSuchBucket: FileNotFoundError
    - 404: FileNotFoundError
    """
    try:
        yield
    except ClientError as client_error:
        # LOGGER.error(str(client_error))
        error = client_error.response["Error"]
        if error["Code"] == "NoSuchBucket":
            raise FileNotFoundError(
                "The specified bucket {} does not exist".format(error["BucketName"])
            )

        if error["Code"] == "NoSuchKey":
            raise FileNotFoundError(error["Message"])

        raise client_error


def test_no_bucket(s3_mock):
    with pytest.raises(FileNotFoundError):
        with handle_client_error():
            s3_mock.get_object(Bucket="tmp-bucket", Key="tmp-key")


def test_no_key(s3_mock):
    s3_mock.create_bucket(
        Bucket="tmp-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )

    with pytest.raises(FileNotFoundError):
        with handle_client_error():
            print(s3_mock.get_object(Bucket="tmp-bucket", Key="tmp-key"))


def test_has_key(s3_mock):
    s3_mock.create_bucket(
        Bucket="tmp-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )
    s3_mock.put_object(Bucket="tmp-bucket", Key="tmp-key", Body="tmp-body")

    with handle_client_error():
        assert (
            s3_mock.get_object(Bucket="tmp-bucket", Key="tmp-key")["Body"]
            .read()
            .decode()
            == "tmp-body"
        )


def test_has_key2(s3_mock):
    s3_mock.create_bucket(
        Bucket="tmp-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )
    s3_mock.put_object(Bucket="tmp-bucket", Key="tmp-key", Body=b"tmp-body")

    with handle_client_error():
        assert (
            s3_mock.get_object(Bucket="tmp-bucket", Key="tmp-key")["Body"]
            .read()
            .decode()
            == "tmp-body"
        )
