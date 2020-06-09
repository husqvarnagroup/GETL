"""File registry that works with YYYY/MM/DD prefixed files in s3."""
from collections import namedtuple
from datetime import datetime, timedelta
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

LOGGER = get_logger(__name__)
FileRegistryRow = namedtuple("FileRegistryRow", "file_path, prefix_date, date_lifted")


class PrefixBasedDate(FileRegistry):
    """File registry that works with YYYY/MM/DD prefixes in s3."""

    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("prefix_date", T.DateType(), True),
            T.StructField("date_lifted", T.TimestampType(), True),
        ]
    )

    def __init__(self, bconf: BlockConfig):
        self.file_registry_prefix = bconf.props["BasePrefix"]
        self.update_after = bconf.props["UpdateAfter"]
        self.hive_database_name = bconf.props["HiveDatabaseName"]
        self.hive_table_name = bconf.props["HiveTableName"]
        self.default_start = datetime.strptime(
            bconf.props["DefaultStartDate"], "%Y-%m-%d"
        ).date()
        self.partition_format = bconf.props["PartitionFormat"]
        self.spark = bconf.spark
        self.file_registry_path = None

    def update(self) -> None:
        """Update file registry column date_lifted to current date."""
        dt = DeltaTable(self.file_registry_path, self.spark)
        dt.delta_table.update(
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

            # Get latest date from metadata store
            last_date = self._get_last_prefix_date(dataframe)
            LOGGER.info("Last prefix date was %s", last_date)

            # Get new files from s3
            list_of_rows = self._get_new_s3_files(s3_path, last_date, suffix)

            # Update the metadata store with the new keys
            updated_dataframe = self._update_file_registry(list_of_rows)

            # Make sure that we do not lift the same files twice
            list_of_rows = self._get_files_to_lift(updated_dataframe)

        else:
            LOGGER.info("No registry found create one at %s", self.file_registry_path)
            list_of_rows = self._get_new_s3_files(s3_path, self.default_start, suffix)
            self._create_file_registry(self.file_registry_path, list_of_rows)

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
            prefix_date DATE NOT NULL,
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
        bucket, prefix = extract_bucket_and_prefix(s3_path)

        return "s3://{}".format(
            Path(file_registry_bucket) / file_registry_prefix / prefix
        )

    def _rows_to_dataframe(self, rows: List[FileRegistryRow]) -> DataFrame:
        """Create a dataframe from a list of paths with the file registry schema."""
        data = [(row.file_path, row.prefix_date, row.date_lifted) for row in rows]
        return self.spark.createDataFrame(data, self.schema)
