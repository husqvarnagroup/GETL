"""File registry that works with a prefix in S3."""
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.utils import AnalysisException

from getl.block import BlockConfig
from getl.blocks.fileregistry.base import FileRegistry
from getl.common.delta_table import DeltaTable
from getl.common.hive_table import HiveTable
from getl.common.utils import extract_bucket_and_prefix, fetch_filepaths_from_prefix
from getl.logging import get_logger

# pylint: disable=E1101,W0221
LOGGER = get_logger(__name__)
FileRegistryRow = namedtuple("FileRegistryRow", "file_path, date_lifted")


class FolderBased(FileRegistry):
    """File registry that works with any prefix in S3."""

    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("date_lifted", T.TimestampType(), True),
        ]
    )

    def __init__(self, bconf: BlockConfig):
        self.file_registry_prefix = bconf.props["BasePrefix"]
        self.update_after = bconf.props["UpdateAfter"]
        self.hive_database_name = bconf.props["HiveDatabaseName"]
        self.hive_table_name = bconf.props["HiveTableName"]
        self.spark = bconf.spark
        self.file_registry_path = None

    def update(self) -> None:
        """Update file registry column date_lifted to current date."""
        delta_table = DeltaTable(self.file_registry_path, self.spark)
        delta_table.delta_table.update(
            F.col("date_lifted").isNull(),
            {"date_lifted": "'{}'".format(str(datetime.now()))},
        )

    def load(self, s3_path: str, suffix: str) -> List[str]:
        """Fetch new filepaths that have not been lifted from s3."""
        self.file_registry_path = self._create_file_registry_path(s3_path)
        dataframe = self._fetch_file_registry(self.file_registry_path)

        # If file registry is found
        if dataframe:
            LOGGER.info("File registry found at %s", self.file_registry_path)

            # Get files from S3
            list_of_rows = self._get_new_s3_files(s3_path, suffix)

            # Update the metadata store with the new keys
            updated_dataframe = self._update_file_registry(list_of_rows)

            # Make sure that we do not lift the same files twice
            list_of_rows = self._get_files_to_lift(updated_dataframe)

        else:
            LOGGER.info("No registry found create one at %s", self.file_registry_path)
            list_of_rows = self._get_new_s3_files(s3_path, suffix)
            self._create_file_registry(self.file_registry_path, list_of_rows)

        LOGGER.info("Found %s new keys in s3", len(list_of_rows))
        return [row.file_path for row in list_of_rows]

    ###########
    # PRIVATE #
    ###########
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
        delta_table = DeltaTable(self.file_registry_path, self.spark)
        return delta_table.insert_all(
            updates_df, "source.file_path = updates.file_path"
        )

    @staticmethod
    def _get_new_s3_files(s3_path: str, suffix: str) -> List[FileRegistryRow]:
        """Get all files in S3 as a dataframe."""
        list_of_rows = []

        # Keys found under the s3_path
        keys = list(
            fetch_filepaths_from_prefix(s3_path + "/", suffix, prepend_bucket=True)
        )
        LOGGER.info("Search %s for files. Found: %s files", s3_path, len(keys))

        # Convert keys into a file registry row
        list_of_rows = [FileRegistryRow(key, None) for key in keys] + list_of_rows

        return list_of_rows

    def _fetch_file_registry(self, path: str) -> DataFrame:
        try:
            return self.spark.read.load(path, format="delta")
        except AnalysisException as spark_exception:
            exceptions = ["Incompatible format detected", "doesn't exist"]

            if not any([e in str(spark_exception) for e in exceptions]):
                raise spark_exception

    def _create_file_registry(
        self, file_registry_path: str, rows_of_paths: List[str]
    ) -> DataFrame:
        """When there is now existing file registry create one."""
        dataframe = self._rows_to_dataframe(rows_of_paths)

        # Create hive table
        self._create_hive_table(file_registry_path)
        dataframe.write.save(path=file_registry_path, format="delta", mode="overwrite")

    def _create_hive_table(self, file_registry_path: str):
        hive = HiveTable(self.spark, self.hive_database_name, self.hive_table_name)
        hive.create(
            file_registry_path,
            db_schema="""
            file_path STRING NOT NULL,
            date_lifted TIMESTAMP
        """,
        )

    def _create_file_registry_path(self, s3_path: str) -> str:
        LOGGER.info(
            "Combining base prefix: %s with read path: %s",
            self.file_registry_prefix,
            s3_path,
        )
        file_registry_bucket, file_registry_prefix = extract_bucket_and_prefix(
            self.file_registry_prefix
        )
        _, prefix = extract_bucket_and_prefix(s3_path)

        return "s3://{}".format(
            Path(file_registry_bucket) / file_registry_prefix / prefix
        )

    def _rows_to_dataframe(self, rows: List[FileRegistryRow]) -> DataFrame:
        """Create a dataframe from a list of paths with the file registry schema."""
        data = [(row.file_path, row.date_lifted) for row in rows]
        return self.spark.createDataFrame(data, self.schema)
