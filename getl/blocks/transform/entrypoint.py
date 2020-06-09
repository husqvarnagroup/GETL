"""
Entrypoint for the generic transform block.
"""
from importlib import import_module
from typing import Callable, Dict, List, Optional, Union

from pyspark.sql import DataFrame

from getl.block import BlockConfig


def resolve(func, bconf: BlockConfig) -> DataFrame:
    """Resolve the incoming request for the load type."""
    return func(bconf)


def generic(conf: BlockConfig) -> DataFrame:
    """Resolve the transform block.

    :param list Functions: a list of transform operators defined in [transform definitions](../transform-functions/)

    Example:

    ```
    SectionName:
        Type: transform::generic
        Input: OtherSectionName
        Properties:
            Functions:
                - add_column.date.unixtime_to_utcz:
                    from_column: timestamp
                    to_column: utcTimestamp
                - add_column.date.year:
                    from_column: utcTimestamp
                    to_column: year
    ```

    """
    dataframe = conf.history.get(conf.input)

    for func_meta in conf.props["Functions"]:
        module_path, module_function, params = _get_function_meta(func_meta)
        func = _get_function(module_path, module_function)
        dataframe = func(dataframe, **params)

    return dataframe


def _get_function_meta(function: Dict) -> List[Union[None, str]]:
    """Get the metadata for the module, function_name and parameters."""
    module_function = next(iter(function))
    module_path = None
    function_parameters = next(iter(function.values()))

    if "." in module_function:
        module_path, module_function = module_function.rsplit(".", 1)

    return module_path, module_function, function_parameters


def _get_function(module_path: Optional[str], module_function: str) -> Callable:
    """Import default transform module if moule_path is none."""
    module = import_module("getl.blocks.transform.transform")

    if module_path:
        module = import_module("getl.blocks.transform.{}".format(module_path))

    return getattr(module, module_function)
