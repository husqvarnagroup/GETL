"""Validating and preprocessing lift definitions."""
from typing import Any

import oyaml as yaml

from getl.common.s3path import S3Path


def resolve_lift_definition(lift_def: str, parameters: str):
    # Retrive and process lift definition
    lift_definition = fetch_lift_definition(lift_def)

    # Parse the lift definition
    return _replace_variables(lift_definition, parameters)


def fetch_lift_definition(lift_def: str) -> dict:
    """Load yaml string or fetch lift definition from s3."""
    if "LiftJob" in lift_def:
        return yaml.safe_load(lift_def)

    return yaml.safe_load(S3Path(lift_def).read_text())


def _replace_variables(lift_def: Any, params: dict) -> dict:
    """Replace all the variables with the given parameters."""

    def modify_dict(lift_def, key, value, params):
        if isinstance(value, str) and value in params:
            lift_def[key] = params[value]

    def search_and_modify(lift_def, params):
        if isinstance(lift_def, dict):
            for key, value in lift_def.items():
                modify_dict(lift_def, key, value, params)
                search_and_modify(value, params)

        return lift_def

    mod_params = {"${{{}}}".format(key): val for key, val in params.items()}
    return search_and_modify(lift_def, mod_params)
