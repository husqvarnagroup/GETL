"""Validating and preprocessing lift definitions."""
import re
import string
from typing import Union

import oyaml as yaml

from getl.common.s3path import S3Path


def resolve_lift_definition(lift_def: str, parameters: dict):
    # Retrive and process lift definition
    lift_definition = fetch_lift_definition(lift_def)

    # Parse the lift definition
    return _replace_variables(lift_definition, parameters)


def fetch_lift_definition(lift_def: str) -> dict:
    """Load yaml string or fetch lift definition from s3."""
    if lift_def.startswith("s3://") or lift_def.startswith("s3a://"):
        lift_def = S3Path(lift_def).read_text()

    return yaml.safe_load(lift_def)


def _replace_variables(var: Union[dict, list, str, int, bool], params: dict):
    """Replace all the variables with the given parameters."""
    if isinstance(var, dict):
        return {key: _replace_variables(value, params) for key, value in var.items()}
    if isinstance(var, list):
        return [_replace_variables(value, params) for value in var]
    if isinstance(var, str):
        # Check if the format is "${myVar}" or "${myVar} + extra string"
        if re.match(r"^\$\{\w+\}$", var):
            # Return the raw param
            return params[var[2:-1]]
        # Use string replacement
        return string.Template(var).substitute(params)
    # Do nothing for int and bool
    return var
