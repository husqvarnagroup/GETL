"""A Helper that can supply schema of various types to test Utils GETL."""

import json


def create_json_schema(schema_type: str = "valid") -> dict:
    """Return a json schema."""
    mapper = {
        "missing_name": missing_name_field,
        "missing_type": missing_type_field,
        "missing_nullable": missing_nullable_field,
        "missing_type_and_name": missing_type_and_name_field,
        "missing_metadata": missing_metadata_field,
        "valid": valid_schema,
    }

    return json.loads(mapper[schema_type]())


def valid_schema() -> str:
    """Return a valid schema."""
    return """{
        "type": "struct",
        "fields": [
            { "type": "string", "nullable": true,
                "name": "name", "metadata": {} },
            { "type": "integer", "nullable": true,
                "name": "empid", "metadata": {} },
            { "type": "boolean", "nullable": true,
                "name": "happy", "metadata": {} }
        ]
        }
"""


def missing_name_field() -> str:
    """Return a schema with missing name key."""
    return """{
        "type": "struct",
        "fields": [
            { "type": "string", "nullable": true,
                "name": "name", "metadata": {} },
            { "type": "integer", "nullable": true,
                "name": "empid", "metadata": {} },
            { "type": "boolean", "nullable": true,
                "metadata": {} }
        ]
        }
"""


def missing_nullable_field() -> str:
    """Return a schema with missing nullable key."""
    return """{
        "type": "struct",
        "fields": [
            { "type": "string", "nullable": true,
                "name": "name", "metadata": {} },
            { "type": "integer", "nullable": true,
                "name": "empid", "metadata": {} },
            { "type": "boolean",
                "name": "happy", "metadata": {} }
        ]
        }
"""


def missing_type_field() -> str:
    """Return a schema with missing type key."""
    return """{
        "type": "struct",
        "fields": [
            { "type": "string", "nullable": true,
                "name": "name", "metadata": {} },
            { "type": "integer", "nullable": true,
                "name": "empid", "metadata": {} },
            { "nullable": true,
                "name": "happy", "metadata": {} }
        ]
        }
"""


def missing_type_and_name_field() -> str:
    """Return a schema with missing type and name key."""
    return """{
        "type": "struct",
        "fields": [
            { "type": "string", "nullable": true,
                "name": "name", "metadata": {} },
            { "type": "integer", "nullable": true,
                "name": "empid", "metadata": {} },
            { "nullable": true,
                "metadata": {} }
        ]
        }
"""


def missing_metadata_field() -> str:
    """Return a schema with missing metadata key."""
    return """{
        "type": "struct",
        "fields": [
            { "type": "string", "nullable": true,
                "name": "name", "metadata": {} },
            { "type": "integer", "nullable": true,
                "name": "empid", "metadata": {} },
            { "type": "boolean", "nullable": true,
                "name": "happy"}
        ]
        }
"""
