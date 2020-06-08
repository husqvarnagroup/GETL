"""Module for all the date related operations a dataframe."""
from pyspark.sql import DataFrame, functions as F, types as T


def unixtime_to_utcz(
    dataframe: DataFrame, from_column: str, to_column: str
) -> DataFrame:
    """Convert a unix timestamp.

    :param int from_column: source column
    :param str to_column: destination column, string formatted as `yyyy-MM-dd'T'HH:mm:sssZ`
    """
    return dataframe.withColumn(
        to_column, F.from_unixtime(F.col(from_column), "yyyy-MM-dd'T'HH:mm:sssZ")
    )


def year(dataframe: DataFrame, from_column: str, to_column: str) -> DataFrame:
    """Extract the year from a timestamp into a new column.

    Add a year column from a date column

    :param date from_column: source column
    :param int to_column: destination column, representing the year of the from_column
    """
    return dataframe.withColumn(
        to_column, F.year(F.col(from_column)).cast(T.StringType())
    )


def month(dataframe: DataFrame, from_column: str, to_column: str) -> DataFrame:
    """Extract the month from a timestamp into a new column.
    Add a year column from a date column

    :param date from_column: source column
    :param int to_column: destination column, representing the month of the from_column
    """
    return dataframe.withColumn(
        to_column, F.month(F.col(from_column)).cast(T.StringType())
    )


def dayofmonth(dataframe: DataFrame, from_column: str, to_column: str) -> DataFrame:
    """Extract the day of the month from a timestamp into a new column.

    :param date from_column: source column
    :param int to_column: destination column, representing the day of month of the from_column
    """
    return dataframe.withColumn(
        to_column, F.dayofmonth(F.col(from_column)).cast(T.StringType())
    )


def date(dataframe: DataFrame, from_column: str, to_column: str) -> DataFrame:
    """Extract the date from a timestamp into a new column.

    :param str from_column: source column
    :param date to_column: destination column, cast from_column to a `DateType`
    """
    return dataframe.withColumn(to_column, F.col(from_column).cast(T.DateType()))
