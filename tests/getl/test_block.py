"""Unit tests for the manager class that is the controller of the ETL process."""
import pytest

from getl.block import BlockConfig, BlockLog


@pytest.mark.parametrize(
    "prop, search_prop, result",
    [
        ({"Key": "Value"}, ("Key", "Value"), "it-works"),
        ({"Key": "Value"}, ("wrong", "Value"), None),
        ({"Key": "Value"}, ("Keys", "wrong"), None),
        ({"Key": {"KeyNested": "Value"}}, ("Key.KeyNested", "Value"), "it-works"),
    ],
)
def test_block_log_find(prop, search_prop, result):
    """Search  for props in block log."""
    # Arrange
    conf = BlockConfig("Section", None, None, prop)
    history = BlockLog()
    history.add(conf, "it-works")

    # Act
    res = history.find(prop=search_prop)

    # Assert
    assert res == result


@pytest.mark.parametrize(
    "prop, search_prop, result",
    [
        ({"Key": "Value"}, {"search": "Key"}, "Value"),
        ({"Key": {"NestedKey": "Value"}}, {"search": "Key.NestedKey"}, "Value"),
        (
            {"Key": {"NestedKey": "Value"}},
            {"search": "DoNotExist", "default_value": None},
            None,
        ),
        (
            {"Key": {"NestedKey": "Value"}},
            {"search": "Key.DoNotExist", "default_value": None},
            None,
        ),
    ],
)
def test_block_config_get(prop, search_prop, result):
    """Get nested properties with a dot notation."""
    # Arrange
    conf = BlockConfig("Section", None, None, prop)

    # Act
    res = conf.get(**search_prop)

    # Assert
    assert res == result


@pytest.mark.parametrize(
    "prop, search_prop, result",
    [
        ({"Key": "Value"}, {"search": "Key"}, True),
        ({"Key": {"NestedKey": "Value"}}, {"search": "NotExists"}, False),
    ],
)
def test_block_config_exists(prop, search_prop, result):
    """Get nested properties with a dot notation."""
    # Arrange
    conf = BlockConfig("Section", None, None, prop)

    # Act
    res = conf.exists(**search_prop)

    # Assert
    assert res == result


def test_block_config_get_exeception():
    """Throw exception when no values is found."""
    # Arrange
    conf = BlockConfig("Section", None, None, {})

    # Act
    with pytest.raises(KeyError) as excinfo:
        conf.get("Key")

    # Assert
    assert 'Could not find "Key" in properties' in str(excinfo.value)
