from datetime import datetime

from pyspark.sql import DataFrame, functions as F, types as T

import getl.fileregistry.fileregistry_utils as fr_utils
from getl.block import BlockConfig
from getl.fileregistry.base import FileRegistry


class DeltaDiff(FileRegistry):
    schema = T.StructType([T.StructField("version_date", T.TimestampType(), False)])

    def __init__(self, bconf: BlockConfig) -> None:
        self.file_registry_path = bconf.get("BasePath")

        self.start_date = datetime.strptime(
            bconf.get("DefaultStartDate"), "%Y-%m-%d %H:%M:%S"
        )
        self.current_date = datetime.utcnow()

        self.join_on_fields = bconf.get("JoinOnFields")

        self.spark = bconf.spark

        dataframe = fr_utils.fetch_file_registry(self.file_registry_path, self.spark)
        if dataframe:
            self.start_date = dataframe.first().version_date

    def update(self):
        self.spark.createDataFrame([(self.current_date,)], self.schema).write.save(
            self.file_registry_path, format="delta", mode="overwrite"
        )

    def load_new_rows_only(self, path: str) -> DataFrame:
        try:
            df_last = self.df_from_timestamp(path, self.start_date)
        except ValueError:
            # Load all data instead
            return self.spark.read.load(path, format="delta")
        else:
            df_current = self.df_from_timestamp(path, self.current_date)
            return df_current.join(df_last, self.join_on_fields, how="anti")

    def df_from_timestamp(self, path, timestamp):
        version_to_load = self.get_version_at_timestamp(path, timestamp)
        return self.spark.read.load(path, format="delta", versionAsOf=version_to_load)

    def get_version_at_timestamp(self, path, timestamp):
        df_history = self.spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
        version_to_load = (
            df_history.filter(F.col("timestamp") <= timestamp)
            .orderBy(F.col("timestamp").desc())
            .select("version")
            .limit(1)
            .collect()
        )
        if not version_to_load:
            first_start_time_available = (
                df_history.orderBy(F.col("timestamp").asc()).first().timestamp
            )
            raise ValueError(
                f"Start date is earlier than first available timestamp: {first_start_time_available}, start time is set to {timestamp}"
            )
        return version_to_load[0].version
