"""Manager class that executes lift definitions."""
from collections import OrderedDict
from importlib import import_module
from types import FunctionType, ModuleType
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from getl.block import BlockConfig, BlockLog
from getl.common.errors import NoDataToProcess
from getl.logging import get_logger

LOGGER = get_logger(__name__)


class Manager:
    """Manager class for processing lift jobs."""

    def __init__(self, spark_session: SparkSession) -> None:
        """Initialize the spark session and type mapper."""
        self.history = BlockLog()
        self.file_registry = BlockLog()
        self.spark_session = spark_session

    def init_file_registry(self, file_registry: OrderedDict) -> None:
        """Initiate the file registrys defined in the lift definition."""
        for bconf, res in self._process_blocks(file_registry, "getl"):
            self.file_registry.add(bconf, res)

    def execute_lift_job(self, lift_job: OrderedDict) -> None:
        """Execute the lift definition."""
        try:
            for bconf, dataframe in self._process_blocks(lift_job, "getl.blocks"):
                # Check if output from block is a dataframe
                if not isinstance(dataframe, DataFrame):
                    raise TypeError(
                        f"""Block "{bconf.section_name}" needs to output a DataFrame.
                            But instead it outputed a {type(dataframe)} type."""
                    )

                # Add dataframe to history
                self.history.add(bconf, dataframe)
                LOGGER.info("Output %s", dataframe)

                # If a file registry exists then update it
                file_registry = self.file_registry.find(
                    prop=("UpdateAfter", bconf.section_name)
                )
                if file_registry:
                    file_registry.update()

            return self.history

        except NoDataToProcess:
            LOGGER.info("No new data to process now exiting lift job.")

    def _process_blocks(self, blocks: OrderedDict, base_import_path: str) -> Any:
        """Process each block and yield its result"""
        for section_name, params in blocks.items():
            LOGGER.info("Process block %s with params %s", section_name, params)

            # Fetch functions
            module, function_name = params["Type"].split("::")
            import_path = f"{base_import_path}.{module}"
            wrapper_function = self._get_function(import_path, "resolve")
            function = self._get_function(import_path, function_name)

            # Setup the block config
            bconf = BlockConfig(
                section_name,
                self.spark_session,
                params["Input"] if "Input" in params else None,
                params["Properties"],
                self.history,
                self.file_registry,
            )

            yield bconf, wrapper_function(function, bconf)

    def _get_function(self, module_dir: str, function_name: str) -> FunctionType:
        """Get function from a module."""
        module = self._load_module(module_dir)

        return getattr(module, function_name)

    @staticmethod
    def _load_module(module_dir: str) -> ModuleType:
        """Load the module responsible for resolving the block."""
        return import_module("{module_dir}.entrypoint".format(module_dir=module_dir))
