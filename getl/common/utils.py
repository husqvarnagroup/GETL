"""
The Utils module containing utility functions for lift jobs.

Utilities supported:
1. json_to_spark_schema: Converts a json schema to spark schema.
2. delete_files: Deletes a list of s3 files provided.
3. copy_keys: Copies files between S3 buckets.
4. copy_and_cleanup: Copies files between S3 and removes them from source.


"""
from typing import Dict, List, Tuple, TypeVar

from pyspark.sql.types import StructType

from getl.logging import get_logger

from .s3path import S3Path

LOGGER = get_logger(__name__)
JsonSchemaType = TypeVar("T", int, float, str)


def json_to_spark_schema(json_schema: Dict[str, JsonSchemaType]) -> StructType:
    """
    Return Spark Schema for a JSON schema.

    Args:
        json_schema (Dict[str, JSON_SCHEMA_TYPE]): schema in json format.

    Returns:
        StructType: Spark Schema for the corresponding JSON schema.

    Raises:
        KeyError: Missing Schema key fields Name/Field/Nullable
        TypeError: Invalid json was provided

    """
    try:
        return StructType.fromJson(json_schema)
    except KeyError as key_error:
        LOGGER.error(str(key_error))
        raise KeyError(
            "Missing key: {0}. Valid format: {1}".format(
                str(key_error),
                "All schema columns must have a name, type and nullable key",
            )
        )
    except TypeError as key_error:
        LOGGER.error(str(key_error))
        raise TypeError("Invalid json was provided")


def delete_files(paths: List[str]) -> None:
    """Delete list of files from S3 bucket.

     Args:
        paths  (List[str]): A list of paths pointing out to a key

    Returns:
        None

    Raises:
        PermissionError: When requested to deleted files from raw layer

    Sample Use:
        delete_files(['s3://landingzone/amc-connect/file.txt', 's3://datalake/amc/raw/file.txt'])

    """
    # TODO: remove me
    if any("husqvarna-datalake/raw/" in path for path in paths):
        raise PermissionError(
            "Access Denied: Not possible to remove files from raw layer"
        )

    for path in paths:
        s3path = S3Path(path)
        s3path.delete()


def copy_and_cleanup(paths: List[Tuple[str]]) -> None:
    """Move files from source S3 bucket to the destination bucket.

     Args:
        paths (List[Tuple[str]]): a list that represents [('source', 'target')...]

    Returns:
        None

    Calls:
        copy_keys to copy files between buckets
        delete_files for source cleanup

    Sample Use:
        copy_keys([('landingzone/amc-connect/file.txt', 'datalake/amc/raw/file.txt')])

    """
    copy_keys(paths)
    delete_files([t[0] for t in paths])


def copy_keys(paths: List[Tuple[str]]) -> None:
    """Copy files from source S3 bucket to the destination bucket.

    Args:
        paths (List[Tuple[str]]): a list that represents [('source', 'target')...]

    Returns:
        None

    Raises:
        FileNotFoundError: When any of requested files are not found in S3

    Sample Use:
        copy_keys([('landingzone/amc-connect/file.txt', 'datalake/amc/raw/file.txt')])

    """
    # TODO: Remove me

    for path in paths:
        source = S3Path(path[0])
        target = S3Path(path[1])

        source.copy(target)
