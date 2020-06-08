"""A Helper that can supply schema of various types to test load GETL."""

import json

from pyspark.sql import DataFrame, types as T


def create_princess_df(spark_session) -> DataFrame:
    """Return a valid DF of disney princesses."""
    princesses = [
        {
            "name": "Cinderella",
            "age": 16,
            "happy": False,
            "items": {"weakness": "thorns", "created": "2020-10-14"},
        },
        {
            "name": "Snow white",
            "age": 17,
            "happy": True,
            "items": {"weakness": "apple", "created": "2020-10-14"},
        },
        {
            "name": "Belle",
            "age": 18,
            "happy": False,
            "items": {"weakness": "roses", "created": "2020-10-14"},
        },
        {
            "name": "Jasmine",
            "age": 19,
            "happy": True,
            "items": {"weakness": "jafar", "created": "2020-10-14"},
        },
    ]

    return (
        spark_session.read.option("multiline", "true")
        .json(spark_session.sparkContext.parallelize([json.dumps(princesses)]))
        .select("name", "age", "happy", "items")
    )


def create_princess_age_null_df(spark_session) -> DataFrame:
    """Return a valid DF of disney princesses."""
    princesses = [
        {"name": "Cinderella", "age": 16},
        {"name": "Snow white", "age": 17},
        {"name": "Belle", "age": None},
        {"name": "Jasmine", "age": None,},
    ]

    return (
        spark_session.read.option("multiline", "true")
        .json(spark_session.sparkContext.parallelize([json.dumps(princesses)]))
        .select("name", "age")
    )


def create_json_schema(schema_type: str = "valid") -> dict:
    """Return a json schema."""
    mapper = {
        "valid": create_valid_schema,
        "missing": schema_missing_field,
        "extra": schema_extra_field,
        "castable": schema_different_castable_data_field,
        "uncastable": schema_different_uncastable_data_field,
        "missing_non_nullable": schema_extra_missing_non_nullable_field,
    }

    return json.loads(mapper[schema_type]().json())


def create_valid_schema() -> T.StructType:
    """Return a spark schema."""
    return T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("empid", T.IntegerType(), True),
            T.StructField("happy", T.BooleanType(), True),
        ]
    )


def schema_missing_field() -> T.StructType:
    """Return an sample spark schema with a missing field."""
    return T.StructType(
        [
            T.StructField("name", T.IntegerType(), True),
            T.StructField("empid", T.StringType(), True),
        ]
    )


def schema_extra_field() -> T.StructType:
    """Return an sample spark schema with an extra field defined."""
    return T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("empid", T.IntegerType(), True),
            T.StructField("happy", T.BooleanType(), True),
            T.StructField("extra", T.BooleanType(), True),
        ]
    )


def schema_extra_missing_non_nullable_field() -> T.StructType:
    """Return an sample spark schema with an extra field defined."""
    return T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("empid", T.IntegerType(), True),
            T.StructField("happy", T.BooleanType(), True),
            T.StructField("extra", T.BooleanType(), False),
        ]
    )


def schema_different_castable_data_field() -> T.StructType:
    """Return an sample spark schema with castable change in datatype."""
    return T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("empid", T.StringType(), True),
            T.StructField("happy", T.BooleanType(), True),
        ]
    )


def schema_different_uncastable_data_field() -> T.StructType:
    """Return an sample spark schema with uncastable change in datatype."""
    return T.StructType(
        [
            T.StructField("name", T.IntegerType(), True),
            T.StructField("empid", T.StringType(), True),
            T.StructField("happy", T.BooleanType(), True),
        ]
    )
