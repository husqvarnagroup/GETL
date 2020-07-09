"""Entry point for the metadata blocks."""
from types import FunctionType

from getl.block import BlockConfig
from getl.fileregistry.base import FileRegistry
from getl.fileregistry.s3_date_prefix_scan import S3DatePrefixScan
from getl.fileregistry.s3_full_scan import S3FullScan


def resolve(func: FunctionType, bconf: BlockConfig) -> FileRegistry:
    """Resolve the incoming request for the file registry block."""
    return func(bconf)


def s3_date_prefix_scan(bconf: BlockConfig) -> FileRegistry:
    """Find all new files based on date format YYYY/MM/DD

    ```
    S3DatePrefixScan:
        Type: fileregistry::s3_date_prefix_scan
        Properties:
            BasePath: s3://datalake/file-registry
            UpdateAfter: WriteToDatabase
            HiveDatabaseName: file_registry
            HiveTableName: dataset-a
    ```
    """
    return S3DatePrefixScan(bconf)


def s3_full_scan(bconf: BlockConfig) -> FileRegistry:
    """Do a full scan for new files under a prefix in s3

    ```
    S3FullScan:
        Type: fileregistry::s3_full_scan
        Properties:
            BasePath: s3://datalake/file-registry/dateset-a
            UpdateAfter: WriteToDatabase
            HiveDatabaseName: file_registry
            HiveTableName: dataset-a
    ```
    """
    return S3FullScan(bconf)
