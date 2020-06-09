"""Entrypoint for the load block."""
import functools
import json
from typing import List

from getl.block import BlockConfig
from getl.common.errors import NoDataToProcess
from getl.common.utils import fetch_s3_file, json_to_spark_schema
from pyspark.sql import DataFrame, SparkSession, types as T
from pyspark.sql.utils import AnalysisException


def resolve(func, conf: BlockConfig) -> DataFrame:
    """Resolve the incoming request for the load type."""
    dataframe = func(conf)

    # Set an alias on the dataframe if param is passed
    if conf.exists("Alias"):
        dataframe.alias(conf.get("Alias"))

    return dataframe


def batch_parquet(conf: BlockConfig) -> DataFrame:
    """Load parquet data in batch.

    :param str Path: path to the data files
    :param str FileRegistry: optional, name of the fileregistry to use
    :param str Alias: alias of some sort?

    ```
    SectionName:
        Type: load::batch_parquet
        Properties:
            Path: s3://husvarna-datalake/trusted/amc/live
            FileRegistry: PrefixBasedDate
            Alias: settings
    ```

    """
    return _batch_read(
        conf.spark, _process_path(conf, suffix=".parquet"), file_format="parquet"
    )


def batch_json(conf: BlockConfig) -> DataFrame:
    """Load json data in batch.

    :param str Path: path to the data files
    :param str FileRegistry: optional, name of the fileregistry to use
    :param str Alias: alias of some sort?
    :param str Suffix: the suffix of the file, default .json


    ```
    SectionName:
        Type: load::batch_json
        Properties:
            Path: s3://husvarna-datalake/trusted/amc/live
            FileRegistry: PrefixBasedDate
            Alias: settings
            Suffix: .json
    ```

    """
    paths = _process_path(conf, suffix=conf.get("Suffix", ".json"))
    schema = json.loads(fetch_s3_file(conf.get("JsonSchemaPath")))

    return _batch_read(
        conf.spark,
        paths,
        file_format="json",
        options={"schema": json_to_spark_schema(schema)},
    )


def batch_xml(conf: BlockConfig) -> DataFrame:
    """Load xml data in batch.

    :param str Path: path to the data files
    :param str FileRegistry: optional, name of the fileregistry to use
    :param str Alias: alias of some sort?
    :param str Suffix: the suffix of the file, default .xml
    :param int BatchSize: the amount of data to process in 1 go

    ```
    SectionName:
        Type: load::batch_xml
        Properties:
            Path: s3://husvarna-datalake/trusted/amc/live
            FileRegistry: PrefixBasedDate
            Alias: settings
            RowTag: employee
            Suffix: .xml # Default .xml
            BatchSize: 200 # Default 200
    ```

    """

    def get_batches(file_paths: List[str], batch_size: int) -> List[str]:
        """Batches the file paths"""
        for i in range(0, len(file_paths), batch_size):
            yield file_paths[i : i + batch_size]

    def xml_batch_read(path: str, schema: dict) -> DataFrame:
        return _batch_read(
            conf.spark,
            path,
            file_format="xml",
            options={
                "rowTag": conf.get("RowTag"),
                "schema": json_to_spark_schema(schema),
            },
        )

    # Get the paths to process and the schema
    path = _process_path(conf, suffix=conf.get("Suffix", ".xml"))
    schema = json.loads(fetch_s3_file(conf.get("JsonSchemaPath")))
    batch_size = conf.get("BatchSize", 200)

    # If we have a list of files transform it into batches of comma seperated strings
    # The XML modules can only take multiple fiels in the following way file1,file2,file3
    if isinstance(path, list):
        dfs = [
            xml_batch_read(",".join(batch), schema)
            for batch in get_batches(path, batch_size)
        ]
        return functools.reduce(DataFrame.unionByName, dfs)

    return xml_batch_read(path, schema)


def batch_delta(conf: BlockConfig) -> DataFrame:
    """Load delta data in batch.

    :param str Path: path to the data files
    :param str FileRegistry: optional, name of the fileregistry to use
    :param str Alias: alias of some sort?

    ```
    SectionName:
        Type: load::batch_delta
        Properties:
            Path: s3://husvarna-datalake/trusted/amc/live
            Alias: settings
    ```

    """
    try:
        paths = conf.props["Path"]
        return _batch_read(conf.spark, paths, file_format="delta")

    except AnalysisException as spark_exception:
        exceptions = ["Incompatible format detected", "doesn't exist"]

        if not any([e in str(spark_exception) for e in exceptions]):
            raise spark_exception

        # Return empty dataframe if no one was found
        return conf.spark.createDataFrame(
            conf.spark.sparkContext.emptyRDD(), T.StructType([])
        )


def stream_json(bconf: BlockConfig) -> DataFrame:
    """Load json data as a stream.

    :param str Path: path to the data files
    :param str SchemaPath: path to the schema
    :param str FileRegistry: optional, name of the fileregistry to use
    :param str Alias: alias of some sort?

    ```
    SectionName:
        Type: load::stream_json
        Properties:
            SchemaPath: ${PathToSchema}
            Path: ${PathToRawFiles}
            Alias: settings
    ```

    """
    json_schema = json.loads(fetch_s3_file(bconf.props["SchemaPath"]))

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
    :param str Alias: alias of some sort?
    :param str Query: SQL query to access the data

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


def _batch_read(spark: SparkSession, paths, file_format, options={}):
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
