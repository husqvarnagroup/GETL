"""File registry that works with YYYY/MM/DD prefixed files in s3."""
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List

from pyspark.sql import DataFrame, functions as F, types as T

import getl.blocks.fileregistry.utils as fr_utils
from getl.block import BlockConfig
from getl.blocks.fileregistry.base import FileRegistry
from getl.common.delta_table import DeltaTable
from getl.common.utils import fetch_filepaths_from_prefix
from getl.logging import get_logger

LOGGER = get_logger(__name__)
FileRegistryRow = namedtuple("FileRegistryRow", "file_path, prefix_date, date_lifted")


class S3DatePrefixScan(FileRegistry):
    """File registry that works with YYYY/MM/DD prefixes in s3."""

    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("prefix_date", T.DateType(), True),
            T.StructField("date_lifted", T.TimestampType(), True),
        ]
    )

    def __init__(self, bconf: BlockConfig) -> None:
        self.file_registry_path = bconf.get("BasePath")
        self.update_after = bconf.get("UpdateAfter")
        self.default_start = datetime.strptime(
            bconf.get("DefaultStartDate"), "%Y-%m-%d"
        ).date()
        self.partition_format = bconf.get("PartitionFormat")
        self.spark = bconf.spark
        self.delta_table = fr_utils.get_or_create(
            self.file_registry_path, df_schema=self.schema, db_schema="", conf=bconf
        )

    def update(self) -> None:
        """Update file registry column date_lifted to current date."""
        fr_utils.update_date_lifted(self.delta_table)

    def load(self, s3_path: str, suffix: str) -> List[str]:
        """Fetch new filepaths that have not been lifted from s3."""

        # Get files from S3
        list_of_rows = self._get_new_s3_files(s3_path, suffix)

        # Update the metadata store with the new keys
        updated_dataframe = self._update_file_registry(list_of_rows)

        # Make sure that we do not lift the same files twice
        list_of_rows = self._get_files_to_lift(updated_dataframe)

        # Log how many new files we found
        LOGGER.info("Found %s new keys in s3", len(list_of_rows))

        return [row.file_path for row in list_of_rows]

    ###########
    # PRIVATE #
    ###########
    @staticmethod
    def _get_files_to_lift(dataframe: DataFrame) -> List[str]:
        """Get a list of s3 paths from the file registry that needs to be lifted."""
        data = (
            dataframe.where(F.col("date_lifted").isNull())
            .select("file_path", "prefix_date", "date_lifted")
            .collect()
        )

        return [
            FileRegistryRow(row.file_path, row.prefix_date, row.date_lifted)
            for row in data
        ]

    def _update_file_registry(self, list_of_rows: List[FileRegistryRow]) -> DataFrame:
        """Update the file registry and do not insert duplicates."""
        updates_df = self._rows_to_dataframe(list_of_rows)

        # Update the file registry
        delta_table = DeltaTable(self.file_registry_path, self.spark)
        return delta_table.insert_all(
            updates_df, "source.file_path = updates.file_path"
        )

    def _get_new_s3_files(
        self, s3_path: str, start: datetime, suffix: str
    ) -> List[FileRegistryRow]:
        """Get all new files in s3 from start to now as a dataframe."""
        list_of_rows = []

        for date in self._get_dates_from(start):
            # Create prefix to search for files in s3
            prefix = "{}/{}".format(s3_path, date.strftime(self.partition_format))

            # Keys found under the prefix
            keys = list(
                fetch_filepaths_from_prefix(prefix, suffix, prepend_bucket=True)
            )
            LOGGER.info("Search prefix %s for files found: %s", prefix, len(keys))

            # Convert keys with date into a file registry row
            list_of_rows = [
                FileRegistryRow(key, date, None) for key in keys
            ] + list_of_rows

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
        date = (dataframe.groupby().agg(F.max("prefix_date")).collect())[0][0]

        if not date:
            date = self.default_start

        return date
