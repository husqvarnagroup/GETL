"""Unit test for GETL transform function."""
# pylint: disable=W0212

import json
from unittest import mock

import pytest
from pyspark.sql import DataFrame, types as T

import getl.blocks.transform.transform as tr
from tests.getl.data.samples import create_princess_age_null_df, create_princess_df

###############
# WHERE tests #
###############


@pytest.mark.spark
@pytest.mark.parametrize(
    "predicate, princess_names",
    [
        (("age", "<", 18), ["Cinderella", "Snow white"]),
        (("name", "like", "Cin%"), ["Cinderella"]),
        (("name", "like", "%in%"), ["Cinderella", "Jasmine"]),
        (("happy", "==", True), ["Snow white", "Jasmine"]),
        ((("happy", "==", True), "and", ("age", "<", 18)), ["Snow white"]),
        ((("happy", "==", False), "and", ("age", "<", 17)), ["Cinderella"]),
    ],
)
def test_where_success(predicate, princess_names, spark_session):
    """Where function can filter DF successfully."""
    # Act
    result = tr.where(create_princess_df(spark_session), predicate)

    # Assert
    assert result.count() == len(princess_names)


@pytest.mark.spark
def test_where_success_nested_df(spark_session):
    """Where function can query nested DF with column name containing an hyphen."""
    # Arrange
    dataframe = spark_session.read.option("multiline", "true").json(
        "./tests/getl/data/json_sample.json"
    )

    # Act
    result = tr.where(dataframe, ("gear.shoulder-pads", "==", "sturdy leather"))

    # Assert
    assert isinstance(result, DataFrame)
    assert result.count() == 1


@pytest.mark.spark
@pytest.mark.parametrize(
    "predicate, col_name",
    [
        (("age1", "==", 1), "age1"),
        (("name1", "==", "b"), "name1"),
        (("name1", ">", "b"), "name1"),
        (("age1", ">", 2), "age1"),
        (("age1", ">", "c"), "age1"),
        ((("name1", "in", ["c", "z"]), "or", ("yes", "!=", False)), "name1"),
        ((("age1", ">", 2), "and", ("age", ">", "c")), "age1"),
    ],
)
def test_where_throws_col_not_found(predicate, col_name, spark_session):
    """Spark throws column not found exception."""
    # Arrange
    error_message = f"cannot resolve '`{col_name}`' given input columns: ["
    # Act
    with pytest.raises(ValueError) as col_not_found:
        tr.where(create_princess_df(spark_session), predicate)

    # Assert
    assert error_message in str(col_not_found.value)


@pytest.mark.parametrize(
    "logical_predicate, operator",
    [
        ((("name1", "in", ["c", "z"]), "&", ("yes", "!=", False)), "&"),
        ((("age1", "|", 2), "|", ("age", ">", "c")), "|"),
    ],
)
@mock.patch("getl.blocks.transform.transform.DataFrame")
def test_where_throws_invalid_operator(m_df, logical_predicate, operator):
    """Where throws invalid operator when passing an invalid LogicalPredicate."""
    # Arrange
    error_message = "Only 'AND/OR' allowed in LogicalPredicate. But '{}' was provided".format(
        operator
    )

    # Act
    with pytest.raises(ValueError) as invalid_operator:
        tr.where(m_df, logical_predicate)

    # Assert
    assert error_message in str(invalid_operator.value)


@pytest.mark.spark
@pytest.mark.parametrize(
    "predicate, princess_names",
    [
        (("age", "!=", "null"), ["Cinderella", "Snow white"]),
        (("age", "==", "null"), ["Belle", "Jasmine"]),
    ],
)
def test_where_age_null(predicate, princess_names, spark_session):
    """Where function can filter DF successfully."""
    # Act
    result = tr.where(create_princess_age_null_df(spark_session), predicate)

    # Assert
    assert result.count() == len(princess_names)
    collect = result.collect()
    assert sorted([collect[0]["name"], collect[1]["name"]]) == princess_names


###################
# PREDICATE tests #
###################
@pytest.mark.parametrize(
    "args,q_string",
    [
        (("age", "<", 18), "age < 18"),
        (("name", "like", "Cin%"), "name like 'Cin%'"),
        (("name", "in", ["Cin"]), "name in ('Cin')"),
        (("name", "in", ["Cin", "Jas"]), "name in ('Cin', 'Jas')"),
        (("happy", "==", True), "happy == True"),
        (("happy", "!=", False), "happy != False"),
        (("age", "!=", "null"), "age is not null"),
        (("age", "==", "null"), "age is null"),
        (
            (("happy", "==", True), "and", ("age", "<", 18)),
            "(happy == True and age < 18)",
        ),
        (
            (("happy", "==", False), "and", ("age", "<", 17)),
            "(happy == False and age < 17)",
        ),
        (
            ("family.father-status", "like", "alive"),
            "family.`father-status` like 'alive'",
        ),
    ],
)
def test_predicate_to_sql_success(args, q_string):
    """predicate_to_sql returns query string successfully after parsing input parms."""
    assert tr._predicate_to_sql(args) == q_string


@pytest.mark.parametrize(
    "args,error",
    [
        (("age", "<"), None),
        (("name", "Cin%"), None),
        ((True, "==", "happy"), None),
        (
            (("happy", "==", True, "last"), "and", ("age", "<", 18)),
            "('happy', '==', True, 'last')",
        ),
        ((("happy", "==", False), "and", (17, " <", "age")), "(17, ' <', 'age')"),
        (
            [
                (
                    "where",
                    [
                        (
                            ("happy", "==", True),
                            "and",
                            ("age", "<", 18),
                            "and",
                            ("colors", "in", ["green"]),
                        )
                    ],
                )
            ],
            None,
        ),
    ],
)
def test_predicate_to_sql_raises_value_error(args, error):
    """predicate_to_sql raises value error for illegal operand datatypes."""
    # Arrange
    allowable_types = [int, float, str, list, bool]
    error_message = "Expected format: (tuple, str, tuple) or any of, {}. But, got {}".format(
        [(str, str, dt) for dt in allowable_types], args if error is None else error
    )

    # Act # Assert
    with pytest.raises(ValueError) as value_error:
        tr._predicate_to_sql(args)

    assert error_message in str(value_error.value)


################
# FILTER tests #
################


@pytest.mark.parametrize(
    "predicate, call_parm",
    [
        (("age", "==", 1), "age == 1"),
        ((("age", ">", 2), "and", ("age", ">", "c")), "(age > 2 and age > 'c')"),
    ],
)
@mock.patch("getl.blocks.transform.transform.DataFrame")
def test_filter_passes_right_parameters(m_df, predicate, call_parm):
    """transform_filter passes right parameters and in right order."""
    # Assign
    m_df.where.return_value = "m_where_df"
    tr.filter_dataframe(m_df, predicate)

    # Act & Assert
    m_df.where.assert_called_with(call_parm)
    m_df.subtract.assert_called_with("m_where_df")


@pytest.mark.spark
@pytest.mark.parametrize(
    "predicate, princess_names",
    [
        (("age", "<", 18), ["Belle", "Jasmine"]),
        (("name", "like", "Cin%"), ["Belle", "Jasmine", "Snow white"]),
        (("name", "like", "%in%"), ["Belle", "Snow white"]),
        (("happy", "==", True), ["Belle", "Cinderella"]),
        (
            (("happy", "==", True), "and", ("age", "<", 18)),
            ["Belle", "Jasmine", "Cinderella"],
        ),
        (
            (("happy", "==", False), "and", ("age", "<", 17)),
            ["Belle", "Jasmine", "Snow white"],
        ),
    ],
)
def test_filter_success(predicate, princess_names, spark_session):
    """Filter functions returns DF successfully."""
    # Arrange
    q_string = ""
    if len(princess_names) > 1:
        q_string = "name in {}".format(tuple(princess_names))
    else:
        q_string = "name in ('{}')".format(princess_names[0])

    # Act
    result = tr.filter_dataframe(create_princess_df(spark_session), predicate)

    # Assert
    assert result.where(q_string).count() == len(princess_names)


################
# SELECT tests #
################


@pytest.mark.spark
@pytest.mark.parametrize(
    "cols, col_count",
    [
        ([{"col": "name"}, {"col": "age"}], "DataFrame[name: string, age: bigint]"),
        (
            [
                {"col": "name"},
                {"col": "age"},
                {"col": "new_col", "add_new_column": True},
            ],
            ["DataFrame[name: string, age: bigint, new_col: null]"],
        ),
        (
            [
                {"col": "device", "default_value": "array()", "add_new_column": True},
                {
                    "col": "conf",
                    "default_value": "array()",
                    "add_new_column": True,
                    "alias": "test",
                },
            ],
            [
                "DataFrame[device: array<string>, test: array<string>]",
                "DataFrame[device: array<null>, test: array<null>]",
            ],
        ),
        (
            [{"col": "age", "alias": "years", "cast": "string"}],
            ["DataFrame[years: string]"],
        ),
        (
            [{"col": "name", "alias": "id"}, {"col": "name", "alias": "firstname"}],
            ["DataFrame[id: string, firstname: string]"],
        ),
        ([{"col": "items.weakness", "alias": "hello"}], "DataFrame[hello: string]"),
        (
            [{"col": "items.created", "alias": "created", "cast": "date"}],
            ["DataFrame[created: date]"],
        ),
    ],
)
def test_select_success(cols, col_count, spark_session):
    """Select returns DF successfully."""
    # Act
    result = tr.select(create_princess_df(spark_session), cols)

    # Assert
    assert str(result) in col_count


@pytest.mark.spark
@pytest.mark.parametrize(
    "cols, error",
    [
        (
            [{"col": "col"}],
            "Column 'col' is not present in the dataframes columns: ['name', 'age', 'happy', 'items']",
        ),
        (
            [{"col": "name"}, {"col": "age"}, {"col": "col"}],
            "Column 'col' is not present in the dataframes columns: ['name', 'age', 'happy', 'items']",
        ),
        (
            [{"col": "items.created", "cast": "date"}],
            "Can not cast nested column items.created please use the alias parameter also.",
        ),
    ],
)
def test_select_fail(cols, error, spark_session):
    """Select returns a failure.

    If the add_cols are set to False and we try to select unknown columns raise an error
    """
    # Act
    with pytest.raises(ValueError) as column_not_found:
        tr.select(create_princess_df(spark_session), cols)

    # Assert
    assert error in str(column_not_found)


@pytest.mark.spark
@pytest.mark.parametrize(
    "cols, validate_cols",
    [
        (
            [{"col": "age", "cast": "string"}, {"col": "happy", "cast": "integer"}],
            {"age": T.StringType(), "happy": T.IntegerType()},
        )
    ],
)
def test_select_cast_returns_df_successfully(cols, validate_cols, spark_session):
    """Select casts columns and returns DF successfully."""
    # Act
    result = tr.select(create_princess_df(spark_session), cols)

    # Assert
    for f in result.schema.fields:
        assert f.dataType == validate_cols[f.name]


@pytest.mark.spark
@pytest.mark.parametrize(
    "cols, validate_cols",
    [
        (
            [{"col": "age", "alias": "years"}, {"col": "happy", "alias": "glad"}],
            {"age": "years", "happy": "glad"},
        )
    ],
)
def test_select_rename_returns_df_successfully(cols, validate_cols, spark_session):
    """Select rename columns and returns DF successfully."""
    # Act
    result = tr.select(create_princess_df(spark_session), cols)

    # Assert
    for f in result.schema.fields:
        assert f.name in validate_cols.values()


##############
# CAST tests #
##############
@pytest.mark.spark
@pytest.mark.parametrize(
    "col,new_type, validation_type",
    [
        ("age", T.StringType(), ("age", "string")),
        ("name", T.DecimalType(), ("name", "decimal(10,0)")),
        ("happy", T.StringType(), ("name", "string")),
    ],
)
def test_cast_returns_df_successfully(col, new_type, validation_type, spark_session):
    """cast_column returns DF successfully after type cast."""
    # Act
    df_result = tr.cast_column(create_princess_df(spark_session), col, new_type)

    # Assert
    assert isinstance(df_result, DataFrame)
    assert validation_type in df_result.dtypes


@pytest.mark.spark
def test_cast_attribute_error(spark_session):
    """cast_column returns attribute error when column not found."""
    with pytest.raises(AttributeError) as column_not_found:
        tr.cast_column(
            create_princess_df(spark_session), "columnNotPresent", T.DateType()
        )

    assert "Column 'columnNotPresent' not found in df" in str(column_not_found)


################
# RENAME tests #
################


@pytest.mark.spark
@pytest.mark.parametrize("col, new_name", [("age", "years")])
def test_rename_returns_df_successfully(col, new_name, spark_session):
    """rename_column returns DF successfully after renaming column."""
    # Act
    result = tr.rename_column(create_princess_df(spark_session), col, new_name)

    # Assert
    assert isinstance(result, DataFrame)
    assert new_name in result.columns
    assert col not in result.columns


#################
# EXPLODE tests #
#################


@pytest.mark.spark
@pytest.mark.parametrize(
    "col, new_col, res_col",
    [("items.Princess", "princess", "princess"), ("items.Princess", None, "items")],
)
def test_explode_cell(col, new_col, res_col, spark_session):
    """Explode cell to many rows."""
    # Arrange
    princesses = [{"items": [{"Princess": "Cinderella"}, {"Princess": "Snow"}]}]

    df = (
        spark_session.read.option("multiline", "true")
        .json(spark_session.sparkContext.parallelize([json.dumps(princesses)]))
        .select("items")
    )

    # Act
    result = tr.explode(df, col, new_col).collect()

    # Assert
    result[0][res_col] == "Cinderella"
    result[1][res_col] == "Snow"


################
# CONCAT tests #
################


@pytest.mark.spark
@pytest.mark.parametrize(
    "from_columns, to_column, validation_list",
    [
        (
            ["name", "age", "happy"],
            "concat_col",
            [
                "Cinderella_16_false",
                "Snow white_17_true",
                "Belle_18_false",
                "Jasmine_19_true",
            ],
        )
    ],
)
def test_concat_success(from_columns, to_column, validation_list, spark_session):
    """concat function returns concatenated columns successfully"""
    # Act
    result = tr.concat(create_princess_df(spark_session), from_columns, to_column)

    # Assert
    assert (
        result.select(to_column).rdd.map(lambda row: row[0]).collect()
        == validation_list
    )
    assert to_column in result.columns


###############
# UNION tests #
###############


@pytest.mark.spark
def test_union_returns_df_successfully(spark_session):
    """Union returns DF successfully."""
    # Arrange
    dataframe = create_princess_df(spark_session)
    name_list = ["Snow white", "Belle", "Jasmine", "Cinderella"]

    # Act
    result = tr.union(dataframe, dataframe)

    # Assert
    assert isinstance(result, DataFrame)

    # Each princesses name occur twice after union
    for name in name_list:
        q_string = "name == '{}'".format(name)
        assert result.where(q_string).count() == 2


@pytest.mark.spark
def test_union_fail_incompatible_types(spark_session):
    """Union should fail if the types are different."""
    # Arrange
    error_msg = "Union can only be performed on tables with the compatible column types"
    df1 = create_princess_df(spark_session).select("name", "age", "happy")
    df2 = df1.select(["name", "age", df1.happy.cast(T.StringType())])

    # Act & Assert
    with pytest.raises(ValueError) as union_error:
        tr.union(df1, df2)

    assert error_msg in str(union_error)


@pytest.mark.spark
def test_union_fail_incorrect_num_col(spark_session):
    """Union should fail if we have the incorrect number of columns."""
    # Arrange
    error_msg = "Union can only be performed on tables with the same number of columns"
    df1 = create_princess_df(spark_session)
    df2 = df1.drop("name")

    # Act
    with pytest.raises(ValueError) as union_error:
        tr.union(df1, df2)

    # Assert
    assert error_msg in str(union_error)


##############
# JOIN tests #
##############


@pytest.mark.spark
@pytest.mark.parametrize(
    "col_list, join_type",
    [
        (["name"], "left"),
        (["name", "age"], "right"),
        (["name", "age"], "inner"),
        (["name", "age"], "outer"),
    ],
)
def test_join_returns_df_successfully(col_list, join_type, spark_session):
    """Join returns DF successfully."""
    # Arrange
    dataframe = create_princess_df(spark_session)

    # Act
    result = tr.join(dataframe, dataframe, col_list, join_type)

    # Assert
    assert isinstance(result, DataFrame)


@pytest.mark.spark
@pytest.mark.parametrize(
    "col_list, join_type",
    [
        (["name"], "left"),
        (["name", "age"], "right"),
        (["name", "age"], "inner"),
        (["name", "age"], "outer"),
    ],
)
@mock.patch("getl.blocks.transform.transform.DataFrame")
def test_join_called_in_right_order(m_df, col_list, join_type):
    """Join is called with right parameters and in right order."""
    tr.join(m_df, m_df, col_list, join_type)
    m_df.join.assert_called_with(m_df, col_list, join_type)
