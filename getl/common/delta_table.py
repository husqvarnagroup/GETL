"""Module for handling delta table operations."""
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from getl.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class DeltaTable:
    """Handle operations with delta tables."""

    path: str
    spark: SparkSession

    def __post_init__(self) -> None:
        """Initialize the delta table."""
        self.delta_table = self._create_delta_table()

    def upsert_all(self, updates_df: DataFrame, merge_statement: str) -> DataFrame:
        """Update and insert all rows and columns."""
        (
            self._merge(updates_df, merge_statement)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return self.delta_table.toDF()

    def insert_all(self, updates_df: DataFrame, merge_statement: str) -> DataFrame:
        """Insert all new rows that do not match the merge_statement."""
        (self._merge(updates_df, merge_statement).whenNotMatchedInsertAll().execute())
        return self.delta_table.toDF()

    def _merge(self, updates_df, merge_statement: str):
        return self.delta_table.alias("source").merge(
            updates_df.alias("updates"), merge_statement
        )

    def _create_delta_table(self) -> "DeltaTable":
        """Create a delta table from a path"""
        self.lib = __import__("delta.tables", fromlist=[""])
        return self.lib.DeltaTable.forPath(self.spark, self.path)
