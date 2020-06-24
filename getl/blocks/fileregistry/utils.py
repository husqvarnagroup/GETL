"""Utility functions for the file regitries."""
from collections import namedtuple
from datetime import datetime
from typing import List, Union

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.utils import AnalysisException

from getl.block import BlockConfig
from getl.common.delta_table import DeltaTable
from getl.common.hive_table import HiveTable
from getl.logging import get_logger

LOGGER = get_logger(__name__)


def update_date_lifted(delta_table: "DeltaTable") -> None:
    """Update date lifted column with the current date."""
    delta_table.delta_table.update(
        F.col("date_lifted").isNull(),
        {"date_lifted": "'{}'".format(str(datetime.now()))},
    )


def fetch_file_registry(path: str, spark: SparkSession) -> Union[DataFrame, None]:
    """Retrun a dataframe if one can be found otherwise None."""
    try:
        return spark.read.load(path, format="delta")
    except AnalysisException as spark_exception:
        exceptions = ["Incompatible format detected", "doesn't exist"]

        if not any([e in str(spark_exception) for e in exceptions]):
            raise spark_exception


def get_or_create(
    file_registry_path: str, df_schema: T.StructType, db_schema: str, conf: BlockConfig
) -> "DeltaTable":
    """Get or create a delta table instance for a file registry."""
    dataframe = fetch_file_registry(file_registry_path, conf)

    # If file registry is found
    if not dataframe:
        LOGGER.info("No registry found create one at %s", file_registry_path)
        empty_df = rows_to_dataframe([], df_schema, conf)
        create_file_registry(file_registry_path, empty_df, db_schema, conf)
    else:
        LOGGER.info("File registry found at %s", file_registry_path)

    return DeltaTable(file_registry_path, conf.spark)


def create_file_registry(
    file_registry_path: str,
    dataframe: DataFrame,
    db_schema: str,
    df_schema: T.StructType,
    conf: BlockConfig,
) -> DataFrame:
    """Create a hivet table and a file registry as delta files."""

    # Create hive table
    create_hive_table(file_registry_path, db_schema, conf)
    dataframe.write.save(path=file_registry_path, format="delta", mode="overwrite")


def create_hive_table(
    file_registry_path: str, db_schema: str, conf: BlockConfig
) -> None:
    hive = HiveTable(
        conf.spark, conf.get("HiveDatabaseName"), conf.get("HiveTableName")
    )
    hive.create(
        file_registry_path,
        db_schema="""
        file_path STRING NOT NULL,
        prefix_date DATE NOT NULL,
        date_lifted TIMESTAMP
    """,
    )


def rows_to_dataframe(
    rows: List[namedtuple], schema: T.StructType, conf: BlockConfig
) -> DataFrame:
    """Create a dataframe from a list of paths with the file registry schema."""
    data = [tuple(row) for row in rows]
    return conf.spark.createDataFrame(data, schema)
