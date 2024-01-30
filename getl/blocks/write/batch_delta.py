"""Module for writing files to s3 as delta files."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException, ParseException

from getl.common.delta_table import DeltaTable
from getl.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class BatchDelta:
    """Handels writes to s3 as delta format."""

    dataframe: DataFrame
    spark: SparkSession
    _format: str = "delta"

    def write(
        self,
        path: str,
        mode: str,
        columns: list = None,
        merge_schema: bool = False,
        *,
        database_name: str,
        table_name: str,
    ) -> None:
        """Write to delta files to a location."""
        if columns:
            self.dataframe.write.saveAsTable(
                f"{database_name}.{table_name}",
                path=path,
                format=self._format,
                mode=mode,
                partitionBy=columns,
                mergeSchema=merge_schema,
            )
        else:
            self.dataframe.write.saveAsTable(
                f"{database_name}.{table_name}",
                path=path,
                format=self._format,
                mode=mode,
                mergeSchema=merge_schema,
            )

    def upsert(
        self,
        path: str,
        dbname: str,
        tablename: str,
        merge_statement: str,
        columns: list = None,
        *,
        database_name: str,
        table_name: str,
    ) -> None:
        """Write data to path if not exists otherwise do an upsert."""
        if not self._dataset_exists(path):
            self.write(
                path,
                "overwrite",
                columns,
                database_name=database_name,
                table_name=table_name,
            )
        else:
            delta_table = DeltaTable(
                path,
                spark=self.spark,
                hive_database_name=dbname,
                hive_table_name=tablename,
            )
            delta_table.upsert_all(self.dataframe, merge_statement)

    def clean_write(
        self,
        path: str,
        columns: list = None,
        merge_schema: bool = False,
        *,
        database_name: str,
        table_name: str,
    ) -> None:
        """Write as delta table with overwrite mode."""
        self.write(
            path,
            "overwrite",
            columns,
            merge_schema,
            database_name=database_name,
            table_name=table_name,
        )

    def _dataset_exists(self, path: str) -> bool:
        """Validate if the dataset exists."""
        try:
            return self.spark.read.load(path, format=self._format).count() > 0
        except AnalysisException as spark_exception:
            exceptions = [
                "Incompatible format detected",
                "doesn't exist",
                "is not a Delta table",
                "Path does not exist:",
            ]

            if not any([e in str(spark_exception) for e in exceptions]):
                raise spark_exception

            return False

    @staticmethod
    def optimize(
        spark: SparkSession, dbname: str, tablename: str, zorder_by: str = None
    ) -> None:
        """Optimize delta table."""
        log_desc = f"Optimize table: {dbname}.{tablename}"
        optimize_sql = f"OPTIMIZE {dbname}.{tablename}"

        try:
            if zorder_by:
                LOGGER.info(f"{log_desc} With zorder_by: {zorder_by}")
                spark.sql(f"{optimize_sql} ZORDER BY ({zorder_by})")

            else:
                LOGGER.info(log_desc)
                spark.sql(optimize_sql)

        except ParseException:
            LOGGER.warning("Optimize command is not supported in this environmnet")

    @staticmethod
    def vacuum(
        spark: SparkSession, dbname: str, tablename: str, retain_hours: int = 168
    ):
        """Remove old versions of the delta table, by default retain the last 7 days of versions."""
        LOGGER.info(f"Vacuum the delta table {dbname}.{tablename}")
        try:
            if retain_hours < 168:
                # Delta Lake has a safety check to prevent you from running a dangerous vacuum command.
                # Turn off this safety check by setting the Apache Spark configuration
                # property spark.databricks.delta.retentionDurationCheck.enabled to false
                LOGGER.warning(
                    "We do not recommend that you set a retention interval shorter than 7 days"
                )
                spark.sql(
                    "set spark.databricks.delta.retentionDurationCheck.enabled = false"
                )
            spark.sql(f"VACUUM {dbname}.{tablename} RETAIN {retain_hours} HOURS")
        except ParseException:
            LOGGER.warning("Vacuum command is not supported in this environmnet")
