"""Entry point for the metadata blocks."""
from types import FunctionType

from getl.block import BlockConfig
from getl.fileregistry.base import FileRegistry
from getl.fileregistry.delta_diff import DeltaDiff
from getl.fileregistry.s3_date_prefix_scan import S3DatePrefixScan
from getl.fileregistry.s3_full_scan import S3FullScan


def resolve(func: FunctionType, bconf: BlockConfig) -> FileRegistry:
    """Resolve the incoming request for the file registry block."""
    return func(bconf)


def s3_date_prefix_scan(bconf: BlockConfig) -> FileRegistry:
    """Find all new files in S3 based with a date partition format i.e. YYYY/MM/DD

    With the parameter *PartitionFormat* you can specify multiple different date formats that
    will be used to scan an S3 prefix.

    Take the following example, you can have files stored in the following way on S3 YYYY/MM/DD/HH.
    If we give the *PartitionFormat* parameter the value `%Y/%m`, we use
    [python strftime codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes),
    then it will scan files on a monthly basis. Set it to `%Y/%m/%d/%H` and it
    will scan files on a hourly basis instead.

    :param str BasePath: s3 prefix to where you want the file registry
    :param str UpdateAfter: After what lift block should the new files be marked as processed
    :param str DefaultStartDate: At what date should the file registry start to search for new files
    :param str PartitionFormat: Describe how are the files partitioned and how the file registry
    will search for new files.
    :param str HiveDatabaseName: The hive database name for the file registry
    :param str HiveTableName: The hive table name for the file registry


    ```
    S3DatePrefixScan:
        Type: fileregistry::s3_date_prefix_scan
        Properties:
            BasePath: s3://datalake/file-registry
            UpdateAfter: WriteToDatabase
            DefaultStartDate: 2019-01-01
            PartitionFormat: %Y/%m/d
            HiveDatabaseName: file_registry
            HiveTableName: dataset-a
    ```
    """
    return S3DatePrefixScan(bconf)


def s3_full_scan(bconf: BlockConfig) -> FileRegistry:
    """Do a full scan for new files under a prefix in s3

    :param str BasePath: s3 prefix to where you want the file registry
    :param str UpdateAfter: After what lift block should the new files be marked as processed
    :param str HiveDatabaseName: The hive database name for the file registry
    :param str HiveTableName: The hive table name for the file registry

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


def delta_diff(bconf: BlockConfig) -> FileRegistry:
    """Retrieve a dataset with the new rows compared from the last time lifted.

    :param str BasePath: s3 prefix to where you want the file registry
    :param str UpdateAfter: After what lift block should the new files be marked as processed
    :param str DefaultStartDate: At what date should the file registry start to search for new files (format %Y-%m-%d %H:%M:%S)
    :param list JoinOnFields: Join on these fields to exclude the existing rows in a previous version

    Only available for pyspark 3.0 and above!

    ```
    DeltaDiff:
        Type: fileregistry::delta_diff
        Properties:
            BasePath: s3://datalake/file-registry/dateset-a
            UpdateAfter: WriteToDatabase
            DefaultStartDate: 2019-01-01
            JoinOnFields: [id, timestamp]
    ```
    """
    return DeltaDiff(bconf)
