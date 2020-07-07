"""Entry point for the metadata blocks."""
from types import FunctionType

from getl.block import BlockConfig
from getl.blocks.fileregistry.base import FileRegistry
from getl.blocks.fileregistry.s3_full_scan import S3FullScan
from getl.blocks.fileregistry.s3_prefix_scan import S3PrefixScan


def resolve(func: FunctionType, bconf: BlockConfig) -> FileRegistry:
    """Resolve the incoming request for the file registry block."""
    return func(bconf)


def s3_prefix_scan(bconf: BlockConfig) -> FileRegistry:
    """Find all new files based on date format YYYY/MM/DD

    S3PrefixScan:
        Type: fileregistry::s3_prefix_scan
        Properties:
            BasePath: s3://datalake/file-registry
            UpdateAfter: WriteToDatabase
            HiveDatabaseName: file_registry
            HiveTableName: dataset-a
    """
    return S3PrefixScan(bconf)


def s3_full_scan(bconf: BlockConfig) -> FileRegistry:
    """Do a full scan for new files under a prefix in s3

    S3FullScan:
        Type: fileregistry::s3_full_scan
        Properties:
            BasePath: s3://datalake/file-registry/dateset-a
            UpdateAfter: WriteToDatabase
            HiveDatabaseName: file_registry
            HiveTableName: dataset-a
    """
    return S3FullScan(bconf)
