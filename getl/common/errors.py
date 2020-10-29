"""Custom errors for the GETL module."""
from contextlib import contextmanager
from typing import Dict, Tuple, Type

from botocore.exceptions import ClientError
from pyspark.sql.utils import AnalysisException

# Format: ExceptionClass, Message
_ERROR_MAP: Dict[str, Tuple[Type[Exception], str]] = {
    "NoSuchBucket": (
        FileNotFoundError,
        "The specified bucket {BucketName} does not exist",
    ),
    "NoSuchKey": (FileNotFoundError, "{Message}"),
    "404": (FileNotFoundError, "{Message}"),
}


@contextmanager
def handle_client_error():
    """Raises other exceptions depending on the error code.

    Converts the following codes to a different exception:
    - NoSuchBucket: FileNotFoundError
    - NoSuchKey: FileNotFoundError
    """
    try:
        yield
    except ClientError as client_error:
        # LOGGER.error(str(client_error))
        error = client_error.response["Error"]
        try:
            exception_class, msg = _ERROR_MAP[error["Code"]]
        except KeyError:
            pass
        else:
            raise exception_class(msg.format(**error))

        raise client_error


@contextmanager
def handle_delta_files_dont_exist():
    try:
        yield
    except AnalysisException as spark_exception:
        exceptions = [
            "Incompatible format detected",
            "doesn't exist",
            "is not a Delta table",
        ]

        if any(e in str(spark_exception) for e in exceptions):
            return
        raise spark_exception


class NoDataToProcess(Exception):
    """Should be thrown when there is not more data to process."""
