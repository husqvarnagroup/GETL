"""Testing the module batch delta."""

from unittest.mock import Mock, call

import pytest

from getl.blocks.write.batch_delta import BatchDelta


@pytest.mark.parametrize(
    "params, calls",
    [
        ({}, [call("OPTIMIZE default.table")]),
        (
            {"zorder_by": "columnking"},
            [call("OPTIMIZE default.table ZORDER BY (columnking)")],
        ),
    ],
)
def test_optimize_hive_table(params, calls):
    """While writing create a hive table with and without ZOPTIMIZE."""
    # Arrange
    spark_session = Mock()

    # Act
    BatchDelta.optimize(spark_session, "default", "table", **params)

    # Assert
    assert spark_session.sql.call_count == 1
    spark_session.sql.assert_has_calls(calls)


@pytest.mark.parametrize(
    "params, calls",
    [
        ({}, [call("VACUUM default.table RETAIN 168 HOURS")]),
        (
            {"retain_hours": 1000},
            [call("VACUUM default.table RETAIN 1000 HOURS")],
        ),
    ],
)
def test_vacuum_hive_table(params, calls):
    """While writing create a hive table with and without ZOPTIMIZE."""
    # Arrange
    spark_session = Mock()

    # Act
    BatchDelta.vacuum(spark_session, "default", "table", **params)

    # Assert
    assert spark_session.sql.call_count == 1
    spark_session.sql.assert_has_calls(calls)
