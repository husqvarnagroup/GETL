"""Testing the module batch delta."""
import pytest
from getl.blocks.write.batch_delta import BatchDelta
from mock import Mock, call
from pyspark.sql import types as T


@pytest.mark.parametrize(
    "params, calls",
    [
        ({}, [call('OPTIMIZE "path/to/delta/files"')]),
        (
            {"zorder_by": "columnking"},
            [call('OPTIMIZE "path/to/delta/files" ZORDER BY (columnking)')],
        ),
    ],
)
def test_optimize_hive_table(params, calls):
    """While writing create a hive table with and without ZOPTIMIZE."""
    # Arrange
    spark_session = Mock()

    # Act
    BatchDelta.optimize(spark_session, "path/to/delta/files", **params)

    # Assert
    assert spark_session.sql.call_count == 1
    spark_session.sql.assert_has_calls(calls)


@pytest.mark.parametrize(
    "params, calls",
    [
        ({}, [call('VACUUM "path/to/delta/files" RETAIN 168 HOURS')]),
        (
            {"retain_hours": 1000},
            [call('VACUUM "path/to/delta/files" RETAIN 1000 HOURS')],
        ),
    ],
)
def test_vacuum_hive_table(params, calls):
    """While writing create a hive table with and without ZOPTIMIZE."""
    # Arrange
    spark_session = Mock()

    # Act
    BatchDelta.vacuum(spark_session, "path/to/delta/files", **params)

    # Assert
    assert spark_session.sql.call_count == 1
    spark_session.sql.assert_has_calls(calls)
