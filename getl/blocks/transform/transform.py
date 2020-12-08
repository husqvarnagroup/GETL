"""Performs spark transformation operations."""
import logging
from typing import Dict, List, Optional, Tuple, TypeVar

from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.utils import AnalysisException

LOGGING = logging.getLogger(__name__)
PREDICATE = Tuple[str, str, str]
LOGICALPREDICATE = Tuple[PREDICATE, str, PREDICATE]
PredicateType = TypeVar("T", PREDICATE, LOGICALPREDICATE)
A = TypeVar("A", str, int, bool)


def select(dataframe: DataFrame, cols: List[Dict[str, str]]) -> DataFrame:
    """Select columns mentioned in cols argument and apply renaming/casting transformations if any.

    :param list cols: list of columns

    **Columns**

    :param str col: name of the column
    :param bool add_new_column: add new column, default false
    :param str alias: set alias for column
    :param str cast: cast column to type
    :param str default_value: set the default value of the column

    If add_new_columns is true, add missing columns with None values.
    """
    list_of_columns = []

    for col in cols:
        dataframe, column_name = _process_column(dataframe, **col)
        list_of_columns.append(column_name)

    return dataframe.select(*list_of_columns)


def explode(dataframe: DataFrame, col: str, new_col: str = None) -> DataFrame:
    """Explode a list in a cell to many rows in the dataframe

    :param str col: name of the column to explode
    :param str new_col: name of the new column to explode to, could be exploded column
    """
    tmp_new_col = new_col if new_col else col

    return dataframe.withColumn(tmp_new_col, F.explode(col))


def rename_column(dataframe: DataFrame, col: str, new_name: str) -> DataFrame:
    """Return DF with the column renamed and with the columns in the same order.

    :param str col: name of the column
    :param str new_name: new name of the column
    """
    if not _column_present(dataframe, col):
        raise AttributeError("Column '{}' not found in df".format(col))

    return dataframe.withColumnRenamed(col, new_name)


def cast_column(dataframe: DataFrame, col: str, new_type: T) -> DataFrame:
    """Return DF with the column cast to new type and with the columns in the same order.

    :param str col: name of the column
    :param T new_type: type of the column
    """
    if not _column_present(dataframe, col):
        raise AttributeError("Column '{}' not found in df".format(col))

    return dataframe.withColumn(col, F.col(col).cast(new_type))


def join(
    left_df: DataFrame, right_df: DataFrame, cols: List[str], join_type="left"
) -> DataFrame:
    """Return a joined DF.

    :param undefined TODO: unclear how this works
    """
    return left_df.join(right_df, cols, join_type)


def union(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
    """Return union of DFs.

    :param undefined TODO: unclear how this works
    """
    try:
        return left_df.union(right_df)
    except AnalysisException as exception:
        LOGGING.error(str(exception))
        raise ValueError(str(exception))


def where(dataframe: DataFrame, predicate: PredicateType) -> DataFrame:
    """Apply where to DF and returns rows satifying the specified condition.

    Note: Column names with special characters like '.' and '-' must be escaped with ´ ´
    Example: ``payload.attributes.`plant-id` `` (escaping hyphen)

    :param PredicateType predicate: the predicate

    **PredicateType**

    PredicateType consists of a list with 3 string values.

    Examples:

    ```
    SectionName:
        Type: transform::generic
        Input: InputBlock
        Properties:
        Functions:
            - where:
                predicate: [DeviceName, '!=', 'null']
    ```
    """
    try:
        return dataframe.where(_predicate_to_sql(predicate))
    except AnalysisException as analysis_exception:
        LOGGING.error(str(analysis_exception))
        raise ValueError(str(analysis_exception))


def filter_dataframe(dataframe: DataFrame, param: PredicateType) -> DataFrame:
    """Apply filter to DF and filters out(removes) rows satifying the specified condition."""
    return dataframe.subtract(where(dataframe, param))


def concat(
    dataframe: DataFrame, from_columns: List[str], to_column: str, delimiter: str = "_"
) -> DataFrame:
    """Concatenate columns with delimiter and return concatenated column

    :param list from_columns: list of column names
    :param str to_column: destination column
    :param str delimiter: the delimiter between each column, default `_`
    """

    def cast_list_items_to_string(cols):
        return [F.col(col).cast(T.StringType()) for col in cols]

    def add_delimiter(lst, item):
        result = [F.lit(item)] * (len(lst) * 2 - 1)
        result[0::2] = lst
        return result

    processed_list = add_delimiter(cast_list_items_to_string(from_columns), delimiter)
    return dataframe.withColumn(to_column, F.concat(*processed_list))


def drop_duplicates(
    dataframe: DataFrame, columns: Optional[List[str]] = None
) -> DataFrame:
    """Drop duplicates in the dataframe

    :param list columns=: list of columns names to make unique, default takes all columns
    """

    if columns:
        return (
            dataframe.select(F.concat_ws("-", *columns).alias("temp"), "*")
            .dropDuplicates(["temp"])
            .drop("temp")
        )
    return dataframe.dropDuplicates()


def _predicate_to_sql(predicate: PredicateType, sql: str = "") -> str:
    """Convert user predicate input to a valid SQL query string."""
    _validate_param(predicate)

    if _is_predicate(predicate):
        return _process_predicate(predicate, sql)

    return "({} {} {})".format(
        _predicate_to_sql(predicate[0], sql),
        predicate[1],
        _predicate_to_sql(predicate[2], sql),
    )


def _process_predicate(predicate: PredicateType, sql: str):
    """Process each predicate into a sql query string."""

    def wrap_hypen_with_quotes(string: str):
        """Take i.e. family.father-status and tansform into family.`father-status`."""
        return ".".join(
            ["`{}`".format(s) if "-" in s else s for s in string.split(".")]
        )

    def _get_null_operation(operand: str) -> str:
        return "is null" if operand == "==" else "is not null"

    def _is_null_statement(predicate: PredicateType) -> bool:
        return predicate[2] == "null"

    if _is_null_statement(predicate):
        return "{} {}".format(
            wrap_hypen_with_quotes(predicate[0]), _get_null_operation(predicate[1])
        ).strip()

    return "{} {} {} {}".format(
        sql,
        wrap_hypen_with_quotes(predicate[0]),
        predicate[1],
        _format_variable(predicate[1], predicate[2]),
    ).strip()


def _is_predicate(predicate: PredicateType) -> bool:
    """Check the format of predicate and return boolean."""
    return not _is_logical_predicate(predicate)


def _is_logical_predicate(predicate: PredicateType) -> bool:
    """Check the format of logical predicate and return boolean."""
    return tuple(map(type, predicate)) == (tuple, str, tuple)


def _validate_param(predicate: PredicateType) -> None:
    """Validate predicate and logical predicate and raise value error if not."""
    if _is_predicate(predicate):
        _validate_predicate(predicate)

    if _is_logical_predicate(predicate):
        _validate_logical_predicate(predicate)


def _validate_logical_predicate(predicate) -> None:
    """Raise value error if the Logical Predicate operand is not AND/OR."""
    if predicate[1].lower() not in ("and", "or"):
        raise ValueError(
            "Only 'AND/OR' allowed in LogicalPredicate. But '{}' was provided".format(
                predicate[1]
            )
        )


def _validate_predicate(predicate: PredicateType) -> None:
    """Raise value error if the parameters does not confirm to allowed data types."""
    allowable_types = [int, float, str, list, bool]
    if not tuple(map(type, predicate)) in [(str, str, dt) for dt in allowable_types]:
        raise ValueError(
            "Expected format: (tuple, str, tuple) or any of, {}. But, got {}".format(
                [(str, str, dt) for dt in allowable_types], predicate
            )
        )


def _column_present(dataframe: DataFrame, column: str) -> bool:
    """"Validate if the column exists in the dataframe."""
    try:
        dataframe[column]
        return True
    except AnalysisException:
        return False


def _format_variable(operand: str, variable: A) -> str:
    """Convert constant to SQL format according to its datatype."""

    def add_quotes_around_string(variable, operand):
        # Do not add quote around the variable for null checks
        if (
            isinstance(variable, str)
            and variable.lower() != "null"
            and "is" not in operand.lower()
        ):
            return "'{}'".format(variable)

        return variable

    def add_quotes_around_list(variable):
        if isinstance(variable, list):
            if len(variable) > 1:
                return tuple(variable)
            return "('{}')".format(variable[0])

        return variable

    return add_quotes_around_list(add_quotes_around_string(variable, operand))


def _validate_column_exists(dataframe: DataFrame, col: str) -> None:
    """Throw an error if the column does not exist."""
    if not _column_present(dataframe, col):
        msg = f"Column '{col}' is not present in the dataframes columns: {dataframe.columns}"
        raise ValueError(msg)


def _add_new_column(
    dataframe: DataFrame, column_name: str, default_value: str
) -> DataFrame:
    """Add a new column to the dataframe."""
    if default_value == "array()":
        return dataframe.withColumn(column_name, F.array().cast("array<string>"))

    return dataframe.withColumn(column_name, F.lit(None))


def _process_column(
    dataframe: DataFrame,
    col: str,
    add_new_column: bool = False,
    alias: str = None,
    cast: str = None,
    default_value: str = None,
) -> Tuple[DataFrame, str]:
    """Process a column and return its column name."""

    def validate_cast(col: str) -> None:
        if "." in col:
            raise ValueError(
                f"Can not cast nested column {col} please use the alias parameter also."
            )

    # Throw only error if column does not exists and if we should not add it
    if not add_new_column:
        _validate_column_exists(dataframe, col)

    # Add new column if none is present
    if not _column_present(dataframe, col):
        dataframe = _add_new_column(dataframe, col, default_value)

    # Rename the columns
    if alias:
        dataframe = dataframe.withColumn(alias, F.col(col))
        col = alias

    # Cast the column
    if cast:
        validate_cast(col)
        dataframe = cast_column(dataframe, col, cast)

    return dataframe, col
