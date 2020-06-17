"""File registry that works with a prefix in S3."""
from collections import namedtuple
from typing import List

from pyspark.sql import DataFrame, functions as F, types as T

import getl.blocks.fileregistry.fileregistry_utils as fr_utils
from getl.block import BlockConfig
from getl.blocks.fileregistry.base import FileRegistry
from getl.common.delta_table import DeltaTable
from getl.common.hive_table import HiveTable
from getl.common.utils import fetch_filepaths_from_prefix
from getl.logging import get_logger

# pylint: disable=E1101,W0221
LOGGER = get_logger(__name__)
FileRegistryRow = namedtuple("FileRegistryRow", "file_path, date_lifted")


class S3FullScan(FileRegistry):
    """File registry that works with any prefix in S3."""

    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("date_lifted", T.TimestampType(), True),
        ]
    )

    def __init__(self, bconf: BlockConfig) -> None:
        self.file_registry_path = bconf.get("BasePath")
        self.update_after = bconf.get("UpdateAfter")
        self.hive_database_name = bconf.get("HiveDatabaseName")
        self.hive_table_name = bconf.get("HiveTableName")
        self.spark = bconf.spark
        self.delta_table = self._get_or_create(self.file_registry_path)

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
    def _get_or_create(self, file_registry_path: str) -> "DeltaTable":
        """Get or create a delta table instance for a file registry."""
        dataframe = fr_utils.fetch_file_registry(file_registry_path, self.spark)

        # If file registry is found
        if not dataframe:
            LOGGER.info("No registry found create one at %s", self.file_registry_path)
            self._create_file_registry(self.file_registry_path, [])
        else:
            LOGGER.info("File registry found at %s", self.file_registry_path)

        return DeltaTable(file_registry_path, self.spark)

    @staticmethod
    def _get_files_to_lift(dataframe: DataFrame) -> List[str]:
        """Get a list of S3 paths from the file registry that needs to be lifted."""
        data = (
            dataframe.where(F.col("date_lifted").isNull())
            .select("file_path", "date_lifted")
            .collect()
        )

        return [FileRegistryRow(row.file_path, row.date_lifted) for row in data]

    def _update_file_registry(self, list_of_rows: List[FileRegistryRow]) -> DataFrame:
        """Update the file registry and do not insert duplicates."""
        updates_df = self._rows_to_dataframe(list_of_rows)

        # Update the file registry
        return self.delta_table.insert_all(
            updates_df, "source.file_path = updates.file_path"
        )

    @staticmethod
    def _get_new_s3_files(s3_path: str, suffix: str) -> List[FileRegistryRow]:
        """Get all files in S3 as a dataframe."""
        list_of_rows = []

        # Keys found under the s3_path
        keys = list(
            fetch_filepaths_from_prefix(f"{s3_path}/", suffix, prepend_bucket=True)
        )
        LOGGER.info("Search %s for files. Found: %s files", s3_path, len(keys))

        # Convert keys into a file registry row
        list_of_rows = [FileRegistryRow(key, None) for key in keys] + list_of_rows

        return list_of_rows

    def _create_file_registry(
        self, file_registry_path: str, rows_of_paths: List[str]
    ) -> DataFrame:
        """When there is now existing file registry create one."""
        dataframe = self._rows_to_dataframe(rows_of_paths)

        # Create hive table
        dataframe.write.save(path=file_registry_path, format="delta", mode="overwrite")
        self._create_hive_table(file_registry_path)

    def _create_hive_table(self, file_registry_path: str):
        hive = HiveTable(self.spark, self.hive_database_name, self.hive_table_name)
        hive.create(
            file_registry_path,
            db_schema="""
            file_path STRING,
            date_lifted TIMESTAMP
        """,
        )

    def _rows_to_dataframe(self, rows: List[FileRegistryRow]) -> DataFrame:
        """Create a dataframe from a list of paths with the file registry schema."""
        data = [(row.file_path, row.date_lifted) for row in rows]
        return self.spark.createDataFrame(data, self.schema)
