"""Custom code test."""
import peewee
from pyspark.sql import functions as F


def resolve(params):
    """Add new column to dataframe."""
    dataframe = params["dataframes"]["PrevSection"]

    return dataframe.withColumn("newColumn", F.lit(100))
