"""Module containing classes that are used by the blocks."""
from collections import OrderedDict
from typing import Any, List, Union

from pyspark.sql import SparkSession


class BlockLog:
    """Keeping the result and props used for each block."""

    def __init__(self):
        self.log = OrderedDict()

    def add(self, bconf: "BlockConfig", result: Any) -> None:
        """Add the result from the block."""
        self.log[bconf.section_name] = {"result": result, "bconf": bconf}

    def get(self, section_name: str) -> Any:
        """Get the result from the block."""
        return self.log[section_name]["result"]

    def find(self, prop: tuple = None) -> Any:
        """Find the result from a section."""
        if prop:
            return self._find_with_prop(prop)

        return None

    def _find_with_prop(self, find_prop) -> Any:
        key, value = find_prop

        for name, entry in self.log.items():
            if entry["bconf"].get(key, None) == value:
                return entry["result"]

        return None


class BlockConfig:
    """Config containing all that a block needs to execute."""

    def __init__(
        self,
        section_name: str,
        spark: SparkSession,
        block_input: Union[None, str, List[str]],
        props: dict,
        history: "BlockLog" = BlockLog(),
        file_registry: "BlockLog" = BlockLog(),
    ) -> None:
        self.section_name = section_name
        self.spark = spark
        self.input = block_input
        self.props = props
        self.history = history
        self.file_registry = file_registry

    def exists(self, search: str) -> bool:
        try:
            self._search(search)
            return True

        except KeyError:
            return False

    def get(self, search: str, default_value: str = "default_value") -> any:
        """Get properties from the block config."""
        try:
            return self._search(search)

        except KeyError:
            if default_value != "default_value":
                return default_value

            raise KeyError(f'Could not find "{search}" in properties.')

    def _search(self, search: str) -> any:
        tmp_props = self.props
        search_arr = search.split(".")

        for prop in search_arr:
            tmp_props = tmp_props[prop]

        return tmp_props
