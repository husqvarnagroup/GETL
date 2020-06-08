"""Testing module hive table."""
import pytest
from getl.common.hive_table import HiveTable
from mock import Mock, call


@pytest.mark.parametrize(
    "params, num_calls, schema",
    [
        ({"location": "s3://bucket/prefix"}, 3, ""),
        (
            {"location": "s3://bucket/prefix", "db_schema": "col STRING"},
            3,
            " (col STRING)",
        ),
    ],
)
def test_create_hive_table(params, num_calls, schema):
    """While writing create a hive table with and without ZOPTIMIZE."""
    # Arrange
    spark_session = Mock()
    hive_table = HiveTable(
        spark=spark_session, database_name="db", table_name="tableking"
    )

    # Act
    hive_table.create(**params)

    # Assert
    assert spark_session.sql.call_count == num_calls
    calls = [
        call("CREATE DATABASE IF NOT EXISTS db"),
        call("USE db"),
        call(
            f"""
            CREATE TABLE IF NOT EXISTS tableking{schema}
            USING DELTA LOCATION "s3://bucket/prefix"
        """
        ),
    ]
    spark_session.sql.assert_has_calls(calls)
