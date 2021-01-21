"""Entrypoint for the load block."""
import functools
import json
from typing import Iterator, List

from pyspark.sql import DataFrame, SparkSession, types as T

from getl.block import BlockConfig
from getl.common.errors import NoDataToProcess, handle_delta_files_dont_exist
from getl.common.s3path import S3Path
from getl.common.utils import json_to_spark_schema


def resolve(func, conf: BlockConfig) -> DataFrame:
    """Resolve the incoming request for the load type."""
    dataframe = func(conf)

    # Set an alias on the dataframe if param is passed
    if conf.exists("Alias"):
        dataframe.alias(conf.get("Alias"))

    return dataframe


def batch_csv(conf: BlockConfig) -> DataFrame:
    """Load csv data in batch.

    :param str Path: path to the csv files
    :param dict Options: [options](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv) to be passed to the csv reader
    :param str Alias=: an alias for the dataframe that is loaded

    ```
    SectionName:
        Type: load::batch_csv
        Properties:
            Path: s3://bucket-name/trusted/live
            Options:
                header: True
                inferSchema: True
            Alias: settings
    ```

    """
    return _batch_read(
        conf.spark,
        _process_path(conf, suffix=".csv"),
        file_format="csv",
        options=conf.get("Options", {}),
    )


def batch_parquet(conf: BlockConfig) -> DataFrame:
    """Load parquet data in batch.

    :param str Path: path to the data files
    :param str FileRegistry=: name of the fileregistry to use
    :param str Alias=: an alias for the dataframe that is loaded

    ```
    SectionName:
        Type: load::batch_parquet
        Properties:
            Path: s3://bucket-name/trusted/live
            FileRegistry: S3DatePrefixScan
            Alias: settings
    ```

    """
    return _batch_read(
        conf.spark, _process_path(conf, suffix=".parquet"), file_format="parquet"
    )


def batch_json(conf: BlockConfig) -> DataFrame:
    """Load json data in batch.

    :param str Path: path to the data files
    :param str FileRegistry=: name of the fileregistry to use
    :param str Alias=: an alias for the dataframe that is loaded
    :param str Suffix=.json: the suffix of the file
    :param str JsonSchemaPath=: the file schema in json format, if no schema is submitted, inferSchema will be set to true
    :param dict Options: [options](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.json) to be passed to the reader


    ```
    SectionName:
        Type: load::batch_json
        Properties:
            Path: s3://bucket-name/trusted/live
            FileRegistry: S3DatePrefixScan
            Alias: settings
            Suffix: .json
            JsonSchemaPath: s3://bucket-name/schema.json
    ```

    """
    paths = _process_path(conf, suffix=conf.get("Suffix", ".json"))
    schema_path = conf.get("JsonSchemaPath", None)
    options = conf.get("Options", {})
    if schema_path:
        schema = json.loads(S3Path(schema_path).read_text())
        options["schema"] = json_to_spark_schema(schema)
    else:
        options["inferSchema"] = "true"

    return _batch_read(conf.spark, paths, file_format="json", options=options)


def batch_xml(conf: BlockConfig) -> DataFrame:
    """Load xml data in batch.

    :param str Path: path to the data files
    :param str RowTag: the row tag that indicates a new item in the xml structure
    :param str FileRegistry=: name of the fileregistry to use
    :param str Alias=: an alias for the dataframe that is loaded
    :param str Suffix=.xml: the suffix of the file
    :param int BatchSize=200: the amount of data to process in one go
    :param str JsonSchemaPath=: the file schema in json format

    ```
    SectionName:
        Type: load::batch_xml
        Properties:
            Path: s3://bucket-name/trusted/live
            FileRegistry: S3DatePrefixScan
            Alias: settings
            RowTag: employee
            Suffix: .xml
            BatchSize: 200
            JsonSchemaPath: s3://bucket-name/schema.json
    ```

    """

    def get_batches(file_paths: List[str], batch_size: int) -> Iterator[str]:
        """Batches the file paths"""
        for i in range(0, len(file_paths), batch_size):
            yield file_paths[i : i + batch_size]

    def xml_batch_read(path: str, options: dict) -> DataFrame:
        return _batch_read(conf.spark, path, file_format="xml", options=options,)

    # Get the paths to process
    path = _process_path(conf, suffix=conf.get("Suffix", ".xml"))
    batch_size = conf.get("BatchSize", 200)

    options = {"rowTag": conf.get("RowTag")}
    # Get the file schema
    schema_path = conf.get("JsonSchemaPath", None)
    if schema_path:
        schema = json.loads(S3Path(schema_path).read_text())
        options["schema"] = json_to_spark_schema(schema)
    else:
        options["inferSchema"] = "true"

    # If we have a list of files transform it into batches of comma seperated strings
    # The XML modules can only take multiple files in the following way file1,file2,file3
    if isinstance(path, list):
        dfs = [
            xml_batch_read(",".join(batch), options)
            for batch in get_batches(path, batch_size)
        ]
        return functools.reduce(DataFrame.unionByName, dfs)

    return xml_batch_read(path, options)


def batch_delta(conf: BlockConfig) -> DataFrame:
    """Load delta data in batch.

    :param str Path: path to the data files
    :param str Alias=: an alias for the dataframe that is loaded

    ```
    SectionName:
        Type: load::batch_delta
        Properties:
            Path: s3://bucket-name/trusted/live
            Alias: settings
    ```

    """
    with handle_delta_files_dont_exist():
        path = conf.get("Path")
        if conf.exists("FileRegistry"):
            file_registry = conf.file_registry.get(conf.get("FileRegistry"))
            return file_registry.load_new_rows_only(path)

        df = conf.spark.read.load(path, format="delta")
        return df

    return conf.spark.createDataFrame(
        conf.spark.sparkContext.emptyRDD(), T.StructType([])
    )


def stream_json(bconf: BlockConfig) -> DataFrame:
    """Load json data as a stream.

    :param str Path: path to the data files
    :param str SchemaPath: path to the schema
    :param str Alias=: an alias for the dataframe that is loaded

    ```
    SectionName:
        Type: load::stream_json
        Properties:
            SchemaPath: ${PathToSchema}
            Path: ${PathToRawFiles}
            Alias: settings
    ```

    """
    json_schema = json.loads(S3Path(bconf.props["SchemaPath"]).read_text())

    dataframe = bconf.spark.readStream.schema(json_to_spark_schema(json_schema)).json(
        bconf.props["Path"]
    )

    return dataframe


def jdbc(bconf: BlockConfig) -> DataFrame:
    """Load data from RDS

    :param str Driver: database driver
    :param str ConnUrl: connection url to the database server
    :param str Table: table name with the data
    :param str User: database user
    :param str Password: database password
    :param str Query: SQL query to access the data
    :param str Alias=: an alias for the dataframe that is loaded

    ```
    SectionName:
        Type: load::jdbc
        Properties:
            Driver: 'org.sqlite.JDBC'
            ConnUrl: ${DBUrl}
            Table: ${DBTable}
            User: ${DBUser}
            Password: ${DBPassword}
            Alias: settings
            Query: >-
               SELECT * FROM TABLE
               WHERE name == me
    ```
    """
    dataframe = (
        bconf.spark.read.format("jdbc")
        .option("driver", bconf.props["Driver"])
        .option("url", bconf.props["ConnUrl"])
        .option("user", bconf.props["User"])
        .option("password", bconf.props["Password"])
        .option("query", bconf.props["Query"])
        .load()
    )

    return dataframe


def _batch_read(
    spark: SparkSession, paths: List[str], file_format: str, options: dict = {}
):
    """Retrives data on batch."""
    return spark.read.load(paths, format=file_format, **options)


def _process_path(conf: BlockConfig, suffix) -> List[str]:
    """Process the path and retrive new once from file registry if possible."""
    paths = conf.get("Path")

    if conf.exists("FileRegistry") and isinstance(paths, str):
        file_registry = conf.file_registry.get(conf.get("FileRegistry"))
        paths = file_registry.load(paths, suffix)

        if not paths:
            raise NoDataToProcess

    return paths
