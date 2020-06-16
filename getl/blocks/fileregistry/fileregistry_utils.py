from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from getl.common.delta_table import DeltaTable


def update_date_lifted(file_registry_path: str, spark: SparkSession) -> None:
    """Update date lifted column with the current date."""
    delta_table = DeltaTable(file_registry_path, spark)
    delta_table.delta_table.update(
        F.col("date_lifted").isNull(),
        {"date_lifted": "'{}'".format(str(datetime.now()))},
    )
