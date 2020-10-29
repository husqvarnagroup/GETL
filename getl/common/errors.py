"""Custom errors for the GETL module."""
from contextlib import contextmanager
from typing import Dict, Tuple, Type

from botocore.exceptions import ClientError

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


def delta_files_exists_exception(spark_exception):
    """Check if the spark exception indicates that the delta files does not exist."""
    exceptions = [
        "Incompatible format detected",
        "doesn't exist",
        "is not a Delta table",
    ]

    return any(e in str(spark_exception) for e in exceptions)


class NoDataToProcess(Exception):
    """Should be thrown when there is not more data to process."""
