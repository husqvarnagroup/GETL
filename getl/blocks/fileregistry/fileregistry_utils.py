from datetime import datetime
from typing import Union

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException

from getl.common.delta_table import DeltaTable


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
