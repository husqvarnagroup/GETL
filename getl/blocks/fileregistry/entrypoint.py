"""Entry point for the metadata blocks."""
from types import FunctionType

from getl.block import BlockConfig
from getl.blocks.fileregistry.base import FileRegistry
from getl.blocks.fileregistry.prefix_based_date import PrefixBasedDate
from getl.blocks.fileregistry.s3_full_scan import S3FullScan


def resolve(func: FunctionType, bconf: BlockConfig) -> FileRegistry:
    """Resolve the incoming request for the file registry block."""
    return func(bconf)


def prefix_based_date(bconf: BlockConfig) -> FileRegistry:
    """Find all new files based on date format YYYY/MM/DD

    PrefixBasedDate:
        Type: fileregistry::prefix_based_date
        Properties:
            TableName: plantlib
            BasePrefix: s3://husqvarna-datalake/file-registry
            UpdateAfter: WriteToDatabase
    """
    return PrefixBasedDate(bconf)


def s3_full_scan(bconf: BlockConfig) -> FileRegistry:
    """Find all new files in a folder

    PrefixBasedDate:
        Type: fileregistry::folder_based
        Properties:
            TableName: plantlib
            BasePrefix: s3://husqvarna-datalake/file-registry
            UpdateAfter: WriteToDatabase
    """
    return S3FullScan(bconf)
