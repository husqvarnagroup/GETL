"""Unit test for GETL load method."""
from os import environ

from mock import Mock
from pyspark.sql import types as T

from getl.blocks.load.entrypoint import batch_csv, batch_json, batch_xml, resolve

environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages com.databricks:spark-xml_2.11:0.9.0 pyspark-shell"
SCHEMA = T.StructType(
    [
        T.StructField("name", T.StringType(), True),
        T.StructField("empid", T.IntegerType(), True),
        T.StructField("happy", T.BooleanType(), True),
        T.StructField("sad", T.BooleanType(), True),
        T.StructField("extra", T.BooleanType(), True),
    ]
)


# FUNCTIONS
def test_batch_json(spark_session, helpers):
    """batch_json should be able to load json files to a dataframe."""
    # Arrange
    helpers.create_s3_files({"schema.json": SCHEMA.json()})

    conf = helpers.create_block_conf(
        "",
        {
            "Path": helpers.relative_path(__file__, "./data/sample.json"),
            "JsonSchemaPath": "s3://tmp-bucket/schema.json",
            "Alias": "alias",
        },
    )

    # Act
    result_df = resolve(batch_json, conf)

    # Assert
    assert result_df.collect()[0][0] == "Mark Steelspitter"
    assert result_df.collect()[1][0] == "Mark Two"
    assert result_df.collect()[2][1] == 11
    assert result_df.count() == 3


def test_batch_json_fileregistry(spark_session, helpers):
    """batch_json should be able to load json files with file registry."""
    # Arrange
    file_path = helpers.relative_path(__file__, "./data/sample.json")
    file_registry_mock = Mock()
    file_registry_mock.get.return_value.load.return_value = [file_path]
    helpers.create_s3_files({"schema.json": SCHEMA.json()})

    conf = helpers.create_block_conf(
        "",
        {
            "Path": "base_path",
            "JsonSchemaPath": "s3://tmp-bucket/schema.json",
            "FileRegistry": "SuperReg",
        },
        file_registry=file_registry_mock,
    )

    # Act
    result_df = resolve(batch_json, conf)

    # Assert
    assert result_df.collect()[0][0] == "Mark Steelspitter"
    assert result_df.count() == 3
    file_registry_mock.get.assert_called_with("SuperReg")
    file_registry_mock.get.return_value.load.assert_called_with("base_path", ".json")


def test_batch_json_no_schema(spark_session, helpers):
    """batch_json should be able to load json files and inferSchema."""
    # Arrange
    conf = helpers.create_block_conf(
        "",
        {
            "Path": helpers.relative_path(__file__, "./data/sample.json"),
            "Alias": "alias",
        },
    )

    # Act
    result_df = resolve(batch_json, conf)

    # Assert
    assert result_df.collect()[0][0] == 9
    assert result_df.collect()[1][3] == "Mark Two"
    assert not result_df.collect()[2][2]
    assert result_df.count() == 3


def test_batch_xml(spark_session, helpers):
    """Check if the batch_xml loader can load XML documents."""
    helpers.create_s3_files({"schema.xml": SCHEMA.json()})

    conf = helpers.create_block_conf(
        "",
        {
            "Path": helpers.relative_path(__file__, "./data/employee.xml"),
            "JsonSchemaPath": "s3://tmp-bucket/schema.xml",
            "RowTag": "employee",
        },
    )

    # Act
    result_df = resolve(batch_xml, conf)

    # Assert
    assert result_df.collect()[0][0] == "name1"
    assert result_df.count() == 3


def test_batch_xml_no_schema(spark_session, helpers):
    """Test batch_xml can load XML doc without a given schema."""
    conf = helpers.create_block_conf(
        "",
        {
            "Path": helpers.relative_path(__file__, "./data/employee.xml"),
            "RowTag": "employee",
        },
    )

    # Act
    result_df = resolve(batch_xml, conf)

    # Assert
    assert result_df.collect()[0][0] == 123
    assert result_df.collect()[1][2] == "name2"
    assert result_df.collect()[2][1] == "false"
    assert result_df.count() == 3


def test_batch_xml_batching(spark_session, helpers):
    """Check if the batch_xml loader can load XML documents."""
    helpers.create_s3_files({"schema.xml": SCHEMA.json()})

    conf = helpers.create_block_conf(
        "",
        {
            "Path": [
                helpers.relative_path(__file__, "./data/employee.xml"),
                helpers.relative_path(__file__, "./data/employee_2.xml"),
            ],
            "JsonSchemaPath": "s3://tmp-bucket/schema.xml",
            "RowTag": "employee",
        },
    )

    # Act
    result_df = resolve(batch_xml, conf)

    # Assert
    assert result_df.collect()[0][0] == "name1"
    assert result_df.count() == 4


def test_batch_xml_batching_new_column(spark_session, helpers):
    """Check if the batch_xml loader can load XML documents."""
    helpers.create_s3_files({"schema.xml": SCHEMA.json()})

    conf = helpers.create_block_conf(
        "",
        {
            "Path": [
                helpers.relative_path(__file__, "./data/employee.xml"),
                helpers.relative_path(__file__, "./data/employee_2.xml"),
                helpers.relative_path(__file__, "./data/employee_3.xml"),
            ],
            "JsonSchemaPath": "s3://tmp-bucket/schema.xml",
            "RowTag": "employee",
        },
    )

    # Act
    result_df = resolve(batch_xml, conf)

    # Assert
    assert result_df.collect()[4][3] is False
    assert result_df.count() == 5


def test_batch_xml_fileregistry(spark_session, helpers):
    """Check if the batch_xml loader can load XML documents with a file registry."""
    file_path = helpers.relative_path(__file__, "./data/employee.xml")
    file_registry_mock = Mock()
    file_registry_mock.get.return_value.load.return_value = [file_path]
    helpers.create_s3_files({"schema.xml": SCHEMA.json()})

    conf = helpers.create_block_conf(
        "",
        {
            "Path": "base_path",
            "JsonSchemaPath": "s3://tmp-bucket/schema.xml",
            "RowTag": "employee",
            "FileRegistry": "SuperReg",
        },
        file_registry=file_registry_mock,
    )

    # Act
    result_df = resolve(batch_xml, conf)

    # Assert
    assert result_df.collect()[0][0] == "name1"
    assert result_df.count() == 3
    file_registry_mock.get.assert_called_with("SuperReg")
    file_registry_mock.get.return_value.load.assert_called_with("base_path", ".xml")


def test_batch_csv(spark_session, helpers):
    conf = helpers.create_block_conf(
        "",
        {
            "Path": helpers.relative_path(__file__, "./data/sample.csv"),
            "Options": {"inferSchema": True, "header": True},
        },
    )

    # Act
    result_df = resolve(batch_csv, conf)

    # Assert
    data = result_df.collect()

    assert data[0]["name"] == "Mark Steelspitter"
    assert data[0]["empid"] == 9
    assert data[0]["happy"] is True
    assert data[2]["name"] == "Mark Second"
    assert data[2]["empid"] == 11
    assert data[2]["happy"] is False
    assert result_df.count() == 3
