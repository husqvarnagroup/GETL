from datetime import datetime

from pyspark.sql import functions as F

from getl.common.delta_table import DeltaTable


def update_date_lifted(delta_table: "DeltaTable") -> None:
    """Update date lifted column with the current date."""
    delta_table.delta_table.update(
        F.col("date_lifted").isNull(),
        {"date_lifted": "'{}'".format(str(datetime.now()))},
    )
