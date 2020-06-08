"""Entrypoint for the custom code block."""
import importlib
import random
import string  # pylint: disable=deprecated-module
import subprocess
import sys
import tempfile
from types import FunctionType, ModuleType

from getl.block import BlockConfig
from getl.common.utils import fetch_s3_file
from pyspark.sql import DataFrame


def resolve(func: FunctionType, bconf: BlockConfig) -> DataFrame:
    """Resolve the incoming request for the custom type."""
    return func(bconf)


def python_codeblock(conf: BlockConfig) -> DataFrame:
    '''Execute external python function.

    :param str CustomFunction: this will always be a parameter string
    :param str CustomCodePath: path to the python file with a `resolve` function
    :param dict CustomProps: optional dictionary that will be send to the function
    :param list Packages: optional list of extra packages that will be installed


    The function requires 1 argument that is of type `dict`.
    The argument will contain a key `dataframes` that is of type `dict`,
    where the key is the Input name and value is the dataframe.
    The argument will also contain all `CustomProps` key and values.

    **Examples**:

    There are 2 ways for specifying the python code:

    1. specify a python function

    ```python
    yml_string = """
    Parameters:
        PyFunction:
            Description: custom python function

    LiftJob:
        LoadInput:
            Type: load::batch_json
            Path: s3://bucket/folder/with/data

        CustomPythonFunction:
            Type: custom::python_codeblock
            Input:
                - LoadInput
            Properties:
                CustomFunction: ${PyFunction}
                CustomProps:
                    date: '2020-01-01'
    """


    def my_python_function(params:dict) -> DataFrame:
        dataframe = params["dataframes"]["LoadInput"]
        return dataframe.where(F.col("date") == params["date"])


    lift(
        spark,
        lift_def=yml_string,
        parameters={
            "PyFunction": my_python_function,
        },
    )
    ```

    2\\. specify a python file

    ```python
    yml_string = """
    LiftJob:
        LoadInput:
            Type: load::batch_json
            Path: s3://bucket/folder/with/data

        CustomPythonFunction:
            Type: custom::python_codeblock
            Input:
                - LoadInput
            Properties:
                CustomCodePath: s3://bucket/path/to/python/module.py
                CustomProps:
                    date: '2020-01-01'
                Packages:
                    - pytz==2020.1
    """


    lift(
        spark,
        lift_def=yml_string,
        parameters={},
    )
    ```
    '''
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Add tmp path with libs
        sys.path.insert(0, tmpdirname)

        _install_packages(conf, tmpdirname)

        dataframes = {dataset: conf.history.get(dataset) for dataset in conf.input}
        custom_params = conf.props["CustomProps"] if "CustomProps" in conf.props else {}

        # Resolve the custom code
        custom_function = _get_custom_function(conf, tmpdirname)
        dataframe = custom_function({"dataframes": dataframes, **custom_params})

        # Remove tmp path from the PATH variable
        sys.path.remove(tmpdirname)

    return dataframe


def _get_custom_function(conf: BlockConfig, tmpdirname):
    if conf.exists("CustomFunction"):
        return conf.get("CustomFunction")

    file_content = fetch_s3_file(conf.get("CustomCodePath"))
    custom_module = _import_custom_code(file_content, tmpdirname)

    return custom_module.resolve


def _install_packages(bconf: BlockConfig, tmpdirname: str) -> None:
    if "Packages" not in bconf.props:
        return

    for package in bconf.props["Packages"]:
        _install(package, tmpdirname)


def _install(package: str, tmpdirname: str) -> None:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-t", tmpdirname, package]
    )


def _import_custom_code(file_content: str, tmpdir: str) -> ModuleType:
    """Write then import custom code and finally remove file."""
    module_name = "custom-code-{}".format(_random_string(20))
    custom_code_path = "{}/{}.py".format(tmpdir, module_name)

    with open(custom_code_path, "w") as file:
        file.write(file_content)

    spec = importlib.util.spec_from_file_location(module_name, custom_code_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def _random_string(string_length) -> str:
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(string_length))
