"""Testing the module delta tables."""

from pyspark.sql import types as T

from getl.common.delta_table import DeltaTable


def create_dataframe(spark_session, data):
    schema = T.StructType(
        [
            T.StructField("file_path", T.StringType(), True),
            T.StructField("file_desc", T.StringType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema)


def test_upsert_all(spark_session, tmp_dir):
    """Correct parameters are passed to the upsert all function."""
    # ARRANGE
    create_dataframe(
        spark_session,
        [
            ("path/to/file1", "about stuff"),
            ("path/to/file2", "gloomhaven is a nice place"),
        ],
    ).write.save(tmp_dir, format="delta")

    update_df = create_dataframe(
        spark_session,
        [
            ("path/to/file2", "gloomhaven is a bad place"),
            ("path/to/file3", "my little haven"),
        ],
    )

    delta_table = DeltaTable(path=tmp_dir, spark=spark_session)

    # ACT
    dataframe = delta_table.upsert_all(
        update_df, merge_statement="source.file_path = updates.file_path"
    )

    # ASSER
    assert dataframe.collect()[0][1] == "gloomhaven is a bad place"
    assert dataframe.collect()[1][1] == "my little haven"
    assert dataframe.collect()[2][1] == "about stuff"
