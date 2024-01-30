import os
from contextlib import contextmanager

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"


@pytest.fixture(scope="function")
def aws_mock(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-1")


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


def test_no_bucket(aws_mock):
    with pytest.raises(FileNotFoundError):
        with handle_client_error():
            aws_mock.get_object(Bucket="tmp-bucket", Key="tmp-key")


def test_no_key(aws_mock):
    aws_mock.create_bucket(
        Bucket="tmp-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )

    with pytest.raises(FileNotFoundError):
        with handle_client_error():
            print(aws_mock.get_object(Bucket="tmp-bucket", Key="tmp-key"))


def test_has_key(aws_mock):
    aws_mock.create_bucket(
        Bucket="tmp-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )
    aws_mock.put_object(Bucket="tmp-bucket", Key="tmp-key", Body="tmp-body")

    with handle_client_error():
        assert (
            aws_mock.get_object(Bucket="tmp-bucket", Key="tmp-key")["Body"]
            .read()
            .decode()
            == "tmp-body"
        )


def test_has_key2(aws_mock):
    aws_mock.create_bucket(
        Bucket="tmp-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west1"},
    )
    aws_mock.put_object(Bucket="tmp-bucket", Key="tmp-key", Body=b"tmp-body")

    with handle_client_error():
        assert (
            aws_mock.get_object(Bucket="tmp-bucket", Key="tmp-key")["Body"]
            .read()
            .decode()
            == "tmp-body"
        )
