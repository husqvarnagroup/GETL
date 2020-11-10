"""Entrypoint for the write block."""
from pyspark.sql import DataFrame

from getl.block import BlockConfig
from getl.blocks.write.batch_delta import BatchDelta
from getl.common.hive_table import HiveTable
from getl.common.upsert import handle_postgres_upsert
from getl.logging import get_logger

LOGGER = get_logger(__name__)
UPSERT_MODE = "upsert"
CLEAN_WRITE_MODE = "clean_write"


def resolve(func, conf: BlockConfig) -> DataFrame:
    """Resolve the incoming request for the write type."""
    return func(conf)


def batch_jdbc(conf: BlockConfig) -> DataFrame:
    """Batch save data with jdbc driver.

    :param str Mode: the mode to write with such as append or overwrite
    :param str Driver: the driver
    :param str ConnUrl: the connection url
    :param str Table: table to write to
    :param str User: username to database
    :param str Password: password to database
    :param str NumPartitions: number of partitions to write with

    ```
    SectionName:
        Type: write::batch_jdbc
        Input: OtherSectionName
        Properties:
            Mode: append
            Driver: 'org.sqlite.JDBC'
            ConnUrl: 'jdbc:postgresql://localhost:5432/productapi'
            Table: 'table_name'
            User: 'username'
            Password: 'password'
            NumPartitions: 10
    ```

    """
    dataframe = conf.history.get(conf.input)

    (
        dataframe.write.mode(conf.get("Mode"))
        .format("jdbc")
        .option("driver", conf.get("Driver"))
        .option("url", conf.get("ConnUrl"))
        .option("dbtable", conf.get("Table"))
        .option("user", conf.get("User"))
        .option("password", conf.get("Password"))
        .option("numPartitions", conf.get("NumPartitions"))
        .option("isolationLevel", "READ_COMMITTED")
        .save()
    )

    return dataframe


def batch_json(conf: BlockConfig) -> DataFrame:
    """Write delta data down to some location.

    :param str Path: location to write to
    :param str Mode: the mode to write with such as append or overwrite
    :param str PartitionBy.Columns=: Partition the delta table on one or multiple columns
    ```
    SectionName:
        Type: write::batch_json
        Input: OtherSectionName
        Properties:
            Path: s3://path/to/files
            Mode: overwrite
            PartitionBy:
                Columns: [year, month, day]
    ```
    """
    path = conf.get("Path")
    mode = conf.get("Mode", None)
    columns = conf.get("PartitionBy.Columns", None)
    dataframe = conf.history.get(conf.input)

    df = dataframe.write.mode(mode)

    if columns:
        df = df.partitionBy(columns)

    df.json(path)

    return dataframe


def batch_postgres_upsert(conf: BlockConfig) -> DataFrame:
    """Batch upsert data with psycopg2-binary python package.
    This package can be installed with the *postgres* extra:

    ```sh
    pip install getl[postgres]
    ```

    :param str ConnUrl: the connection url
    :param str Table: table to write to
    :param str User: username to database
    :param str Password: password to database
    :param list[str] Columns: the columns to try and insert from the dataframe, the database columns and dataframe columns bust match
    :param list[str] ConflictColumns: when conflict on these columns occure, update instead
    :param list[str] UpdateColumns=: columns to update in case of a conflict, the default value is all the Columns excluding the ConflictColumns

    ```
    SectionName:
        Type: write::batch_psycopg2_upsert
        Input: OtherSectionName
        Properties:
            ConnUrl: 'postgresql://localhost:5432/productapi'
            Table: 'table_name'
            User: 'username'
            Password: 'password'
            Columns: ['file_path', 'count']
            ConflictColumns: ['file_path']
            UpdateColumns: ['count']
    ```

    """
    dataframe = conf.history.get(conf.input)

    handle_postgres_upsert(
        dataframe=dataframe,
        dsn=conf.get("ConnUrl"),
        user=conf.get("User"),
        password=conf.get("Password"),
        table=conf.get("Table"),
        columns=conf.get("Columns"),
        conflict_columns=conf.get("ConflictColumns"),
        update_columns=conf.get("UpdateColumns", None),
    )
    return dataframe


def batch_delta(conf: BlockConfig) -> DataFrame:
    """Write delta data down to some location.

    :param str Path: location to write to
    :param str Mode: the mode to write with such as append or overwrite
    :param list[str] PartitionBy.Columns=: partition the delta table on one or multiple columns
    :param bool Optimize.Enabled=False: enable optimze on delta table (Only works on databricks)
    :param str Optimize.ZorderBy=None: the column names to optimize on
    :param bool Vacuum.Enabled=False: enable vacuum on delta table (Only works on databricks)
    :param int Vacuum.RetainHours=168: number of days we keep version, default is 7 days, cannot be set lower
    :param str Upsert.MergeStatement=: statement to merge the new data `updates.{col}` with the old data `source.{col}`,
    this option only has an effect when the `Mode: upsert` has been set
    :param str HiveTable.DatabaseName=: name of hive table
    :param str HiveTable.TableName=: name of the hive table
    :param str HiveTable.Schema=: schema of the hive table

    ```
    SectionName:
        Type: write::batch_delta
        Input: OtherSectionName
        Properties:
            Path: s3://path/to/files
            Mode: upsert
            PartitionBy:
                Columns: [year, month, day]
            Optimize:
                Enabled: False
                ZorderBy: column_name, column_name_2
            Vacuum:
                Enabled: False
                RetainHours: 168
            Upsert:
                MergeStatement: source.eventId == updates.eventId
            HiveTable:
                DatabaseName: dbname
                TableName: dbtable
                Schema: >-
                    file_path STRING NOT NULL
                    date_lifted TIMESTAMP
    ```
    """
    path = conf.get("Path")
    mode = conf.get("Mode")
    columns = conf.get("PartitionBy.Columns", None)
    dataframe = conf.history.get(conf.input)
    batch = BatchDelta(dataframe, conf.spark)
    htable = None

    # If hive variables exists create hive table
    if conf.exists("HiveTable"):
        dbname = conf.get("HiveTable.DatabaseName")
        tablename = conf.get("HiveTable.TableName")
        schema = conf.get("HiveTable.Schema", "")

        htable = HiveTable(spark=conf.spark, database_name=dbname, table_name=tablename)
        htable.create(path, schema, columns)

    # Chose one of the batch delta modes
    if mode == UPSERT_MODE:
        batch.upsert(path, conf.get("Upsert.MergeStatement"), columns)
    elif mode == CLEAN_WRITE_MODE:
        batch.clean_write(path, columns)
    else:
        batch.write(path, mode, columns)

    # Optimize the delta files
    if conf.get("Optimize.Enabled", False):
        BatchDelta.optimize(conf.spark, path, conf.get("Optimize.ZorderBy", None))

    # Vacuum the delta files so that we do not keep all versions indefinitly
    if conf.get("Vacuum.Enabled", False):
        BatchDelta.vacuum(conf.spark, path, conf.get("Vacuum.RetainHours", 7 * 24))

    return dataframe


def stream_delta(conf: BlockConfig) -> DataFrame:
    """Write data as a stream to delta files.

    :param str Path: Where to write the data
    :param str OutputMode: How to write the data

    ```
    SectionName:
        Type: write::stream_delta
        Input: OtherSectionName
        Properties:
            Path: s3://path/to/files
            OutputMode: overwrite
    ```

    """
    dataframe = conf.history.get(conf.input)
    (
        dataframe.writeStream.trigger(once=True)
        .format("delta")
        .option("checkpointLocation", "{}/checkpoint".format(conf.props["Path"]))
        .outputMode(conf.props["OutputMode"])
        .start(conf.props["Path"])
    ).awaitTermination()

    return dataframe
