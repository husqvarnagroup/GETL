from datetime import datetime
from typing import Union

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException, ParseException

from getl.common.delta_table import DeltaTable
from getl.common.errors import delta_files_exists_exception
from getl.logging import get_logger

LOGGER = get_logger(__name__)


def update_date_lifted(
    delta_table: "DeltaTable", spark: SparkSession, database_name, table_name
) -> None:
    """Update date lifted column with the current date."""
    delta_table.delta_table.update(
        F.col("date_lifted").isNull(),
        {"date_lifted": "'{}'".format(str(datetime.now()))},
    )
    # Optimze and vacuum Databricks tables
    try:
        LOGGER.info("Optimize file-registry")
        spark.sql(f"OPTIMIZE {database_name}.{table_name}")
    except ParseException:
        LOGGER.warning("Optimize command is not supported in this environmnet")
    try:
        LOGGER.info("Vacuum the file-registry delta table")
        delta_table.delta_table.vacuum()
    except ParseException:
        LOGGER.warning("Vacuum command is not supported in this environmnet")
        pass


def fetch_file_registry(path: str, spark: SparkSession) -> Union[DataFrame, None]:
    """Return a dataframe if one can be found otherwise None."""
    try:
        dataframe = spark.read.load(path, format="delta")
        if dataframe.rdd.isEmpty():
            return None
        return dataframe
    except AnalysisException as spark_exception:
        if not delta_files_exists_exception(spark_exception):
            raise spark_exception

        return None
