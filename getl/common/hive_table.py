"""Module for handling hive table operations."""
from dataclasses import dataclass

from getl.logging import get_logger
from pyspark.sql import SparkSession

LOGGER = get_logger(__name__)


@dataclass
class HiveTable:
    """Manages hive tables."""

    spark: SparkSession
    database_name: str
    table_name: str

    def create(self, location: str, db_schema: str = "") -> None:
        """Create hive table."""
        LOGGER.info('Create Hive table: "%s.%s"', self.database_name, self.table_name)
        create_table = f"CREATE TABLE IF NOT EXISTS {self.table_name}"

        # Add a db schema if its specified
        if db_schema:
            create_table = f"{create_table} ({db_schema})"

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        self.spark.sql(f"USE {self.database_name}")
        self.spark.sql(
            f"""
            {create_table}
            USING DELTA LOCATION "{location}"
        """
        )