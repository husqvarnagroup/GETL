"""Module for writing files to s3 as delta files."""
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException, ParseException

from getl.common.delta_table import DeltaTable
from getl.common.s3path import S3Path
from getl.logging import get_logger

LOGGER = get_logger(__name__)


@dataclass
class BatchDelta:
    """Handels writes to s3 as delta format."""

    dataframe: DataFrame
    spark: SparkSession
    _format: str = "delta"

    def write(self, path: str, mode: str, columns: list = None) -> None:
        """Write to delta files to a location."""
        if columns:
            self.dataframe.write.save(
                path=path, format=self._format, mode=mode, partitionBy=columns
            )
        else:
            self.dataframe.write.save(path=path, format=self._format, mode=mode)

    def upsert(self, path: str, merge_statement: str, columns: list = None) -> None:
        """Write data to path if not exists otherwise do an upsert."""
        if not self._dataset_exists(path):
            self.write(path, "overwrite", columns)
        else:
            delta_table = DeltaTable(path, spark=self.spark)
            delta_table.upsert_all(self.dataframe, merge_statement)

    def clean_write(self, path: str, columns: list = None) -> None:
        """Write to delta files to a clean location."""
        for s3path in S3Path(path).glob():
            s3path.delete()
        self.write(path, "overwrite", columns)

    def _dataset_exists(self, path: str) -> bool:
        """Validate if the dataset exists."""
        try:
            return self.spark.read.load(path, format=self._format).count() > 0
        except AnalysisException as spark_exception:
            exceptions = [
                "Incompatible format detected",
                "doesn't exist",
                "is not a Delta table",
            ]

            if not any([e in str(spark_exception) for e in exceptions]):
                raise spark_exception

            return False

    @staticmethod
    def optimize(spark: SparkSession, path: str, zorder_by: str = None) -> None:
        """Optimize delta table."""
        log_desc = f"Optimize data located at: {path}."
        optimize_sql = f'OPTIMIZE "{path}"'

        try:
            if zorder_by:
                LOGGER.info("%s With zorder_by: %s", log_desc, zorder_by)
                spark.sql(f"{optimize_sql} ZORDER BY ({zorder_by})")

            else:
                LOGGER.info(log_desc)
                spark.sql(optimize_sql)

        except ParseException:
            LOGGER.warning("Optimize command is not supported in this environmnet")

    @staticmethod
    def vacuum(spark: SparkSession, path: str, retain_hours: int = 168):
        """Remove old versions of the delta table, by default retain the last 7 days of versions."""
        LOGGER.info(f"Vacuum the delta tables at: {path}")
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
            spark.sql(f'VACUUM "{path}" RETAIN {retain_hours} HOURS')
        except ParseException:
            LOGGER.warning("Vacuum command is not supported in this environmnet")
