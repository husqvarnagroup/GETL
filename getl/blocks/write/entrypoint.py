"""Entrypoint for the write block."""
from pyspark.sql import DataFrame

from getl.block import BlockConfig
from getl.blocks.write.batch_delta import BatchDelta
from getl.common.hive_table import HiveTable
from getl.logging import get_logger

LOGGER = get_logger(__name__)
UPSERT_MODE = "upsert"
CLEAN_WRITE_MODE = "clean_write"


def resolve(func, conf: BlockConfig) -> DataFrame:
    """Resolve the incoming request for the write type."""
    return func(conf)


def batch_jdbc(conf: BlockConfig) -> DataFrame:
    """Batch save data with jdbc driver.

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


def batch_delta(conf: BlockConfig) -> DataFrame:
    """Write delta data down to some location.

    ```
    SectionName:
        Type: write::batch_delta
        Input: OtherSectionName
        Properties:
            Path: s3://path/to/files
            Mode: upsert
            Optimize:
                Enabled: False # Default
                ZorderBy: column_name
            Vacuum:
                Enabled: False # Default
                RetainHours: 168 # Default 7 days, cannot be set lower
            Upsert:
                SourceColumn: eventId
                UpdateColumn: eventId
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
    dataframe = conf.history.get(conf.input)
    batch = BatchDelta(dataframe, conf.spark)
    htable = None

    # If hive variables exists create hive table
    if conf.exists("HiveTable"):
        dbname = conf.get("HiveTable.DatabaseName")
        tablename = conf.get("HiveTable.TableName")
        schema = conf.get("HiveTable.Schema", "")

        htable = HiveTable(spark=conf.spark, database_name=dbname, table_name=tablename)
        htable.create(path, schema)

    # Chose one of the batch delta modes
    if mode == UPSERT_MODE:
        batch.upsert(path, conf.get("Upsert.MergeStatement"))
    elif mode == CLEAN_WRITE_MODE:
        batch.clean_write(path)
    else:
        batch.write(path, mode)

    # Optimize the delta files
    if conf.get("Optimize.Enabled", False):
        BatchDelta.optimize(conf.spark, path, conf.get("Optimize.ZorderBy", None))

    # Vacuum the delta files so that we do not keep all versions indefinitly
    if conf.get("Vacuum.Enabled", False):
        BatchDelta.vacuum(conf.spark, path, conf.get("Vacuum.RetainHours", 7 * 24))

    return dataframe


def stream_delta(conf: BlockConfig) -> DataFrame:
    """Write data as a stream to delta files.

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
