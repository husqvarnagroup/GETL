"""File registry that works with YYYY/MM/DD prefixed files in s3."""
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List

from pyspark.sql import DataFrame, functions as F, types as T

import getl.blocks.fileregistry.fileregistry_utils as fr_utils
from getl.block import BlockConfig
from getl.blocks.fileregistry.base import FileRegistry
from getl.common.delta_table import DeltaTable
from getl.common.hive_table import HiveTable
from getl.common.s3path import S3Path
from getl.logging import get_logger

LOGGER = get_logger(__name__)
FileRegistryRow = namedtuple("FileRegistryRow", "file_path, prefix_date, date_lifted")


class S3PrefixScan(FileRegistry):
    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("prefix_date", T.DateType(), True),
            T.StructField("date_lifted", T.TimestampType(), True),
        ]
    )

    def __init__(self, bconf: BlockConfig):
        self.file_registry_path = None  # Will be set at a later point
        self.file_registry_prefix = bconf.props["BasePrefix"]
        self.update_after = bconf.props["UpdateAfter"]
        self.hive_database_name = bconf.props["HiveDatabaseName"]
        self.hive_table_name = bconf.props["HiveTableName"]
        self.default_start = datetime.strptime(
            bconf.props["DefaultStartDate"], "%Y-%m-%d"
        ).date()
        self.partition_format = bconf.props["PartitionFormat"]
        self.spark = bconf.spark

    def update(self) -> None:
        fr_utils.update_date_lifted(self.delta_table)

    def load(self, s3_path: str, suffix: str) -> List[str]:
        self.file_registry_path = self._create_file_registry_path(
            self.file_registry_prefix, s3_path
        )
        self._get_or_create()

        list_of_rows = self._get_new_s3_files(s3_path, suffix)
        updated_dataframe = self._update_file_registry(list_of_rows)
        list_of_rows = self._get_files_to_lift(updated_dataframe)

        LOGGER.info("Found %s new keys in s3", len(list_of_rows))

        return list_of_rows

    def _get_or_create(self):
        dataframe = fr_utils.fetch_file_registry(self.file_registry_path, self.spark)

        if not dataframe:
            LOGGER.info("No registry found create one at %s", self.file_registry_path)
            self._create_file_registry(self.file_registry_path, [])
        else:
            LOGGER.info("File registry found at %s", self.file_registry_path)

        self.last_date = self._get_last_prefix_date(dataframe)
        self.delta_table = DeltaTable(self.file_registry_path, self.spark)

    @staticmethod
    def _get_files_to_lift(dataframe: DataFrame) -> List[str]:
        """Get a list of s3 paths from the file registry that needs to be lifted."""
        data = (
            dataframe.where(F.col("date_lifted").isNull()).select("file_path").collect()
        )

        return [row.file_path for row in data]

    def _update_file_registry(self, list_of_rows: List[FileRegistryRow]) -> DataFrame:
        """Update the file registry and do not insert duplicates."""
        updates_df = self._rows_to_dataframe(list_of_rows)

        # Update the file registry
        return self.delta_table.insert_all(
            updates_df, "source.file_path = updates.file_path"
        )

    def _get_new_s3_files(self, s3_path: str, suffix: str) -> List[FileRegistryRow]:
        """Get all new files in s3 from start to now as a dataframe."""
        list_of_rows = []

        for date in self._get_dates_from(self.last_date):
            base_s3path = S3Path(s3_path) / date.strftime(self.partition_format)

            keys = list(base_s3path.glob(suffix))
            LOGGER.info("Search prefix %s for files. Found: %s", base_s3path, len(keys))

            list_of_rows.extend(FileRegistryRow(str(key), date, None) for key in keys)

        return list_of_rows

    def _get_dates_from(self, start: datetime) -> List[datetime]:
        """Get prefixes between the given dates."""
        end = datetime.now().date()
        step = timedelta(days=1)

        while start <= end:
            yield start
            start += step

    def _get_last_prefix_date(self, dataframe: DataFrame) -> str:
        """Return the latest date_lifted column from the dataframe."""
        date = (dataframe.agg(F.max("prefix_date")).collect())[0][0]

        if not date:
            date = self.default_start

        return date

    def _create_file_registry(self) -> DataFrame:
        """When there is now existing file registry create one."""
        dataframe = self.spark.createDataFrame([], self.schema)
        dataframe.write.save(
            path=self.file_registry_path, format="delta", mode="overwrite"
        )
        self._create_hive_table(self.file_registry_path)

    def _create_hive_table(self):
        hive = HiveTable(self.spark, self.hive_database_name, self.hive_table_name)
        hive.create(
            self.file_registry_path,
            db_schema="""
            file_path STRING,
            date_lifted TIMESTAMP
        """,
        )

    def _rows_to_dataframe(self, rows: List[FileRegistryRow]) -> DataFrame:
        """Create a dataframe from a list of paths with the file registry schema."""
        data = [(row.file_path, row.date_lifted) for row in rows]
        return self.spark.createDataFrame(data, self.schema)


def _create_file_registry_path(file_registry_prefix, s3_path: str) -> str:
    LOGGER.info(
        "Combining base prefix: %s with read path: %s", file_registry_prefix, s3_path,
    )
    file_registry_s3_path = S3Path(file_registry_prefix)
    s3_prefix = S3Path(s3_path).key

    return str(file_registry_s3_path / s3_prefix)
