"""Module for handling delta table operations."""

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from getl.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class DeltaTable:
    """Handle operations with delta tables."""

    path: str
    spark: SparkSession
    hive_database_name: Optional[str] = None
    hive_table_name: Optional[str] = None

    def __post_init__(self) -> None:
        """Initialize the delta table."""
        self.delta_table = self._create_delta_table()

    def upsert_all(self, updates_df: DataFrame, merge_statement: str) -> DataFrame:
        """Update and insert all rows and columns."""
        return self._merge(updates_df, merge_statement, update=True)

    def insert_all(self, updates_df: DataFrame, merge_statement: str) -> DataFrame:
        """Insert all new rows that do not match the merge_statement."""
        return self._merge(updates_df, merge_statement, update=False)

    def _merge(self, updates_df, merge_statement: str, update: bool) -> DataFrame:
        merged_df = (
            self.delta_table.alias("source")
            .merge(updates_df.alias("updates"), merge_statement)
            .whenNotMatchedInsertAll()
        )
        if update:
            merged_df = merged_df.whenMatchedUpdateAll()
        merged_df.execute()
        return self.delta_table.toDF()

    def _create_delta_table(self):
        """Create a delta table from a path"""
        import delta.tables

        try:
            return delta.tables.DeltaTable.forPath(self.spark, self.path)
        except AnalysisException:
            if self.hive_database_name and self.hive_table_name:
                try:
                    return delta.tables.DeltaTable.forName(
                        self.spark, f"{self.hive_database_name}.{self.hive_table_name}"
                    )
                except AnalysisException as error:
                    LOGGER.error(error.desc)
                    raise error
