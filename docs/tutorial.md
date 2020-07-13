# Tutorial

## Intro

The goal of the tutorial is to import JSON data, do a couple of transformations on the data and save the data to a location.

The tutorial uses the dataset downloaded from
[https://apps.who.int/gho/data/node.main.A995?lang=en](https://apps.who.int/gho/data/node.main.A995?lang=en) (JSON simple)

The JSON schema used is:

```json
{
  "type": "struct",
  "fields": [
    {
      "name": "Value",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "dims",
      "type": {
        "type": "struct",
        "fields": [
          {
            "name": "COUNTRY",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "YEAR",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "DATASOURCE",
            "type": "string",
            "nullable": true,
            "metadata": {}
          },
          {
            "name": "GHO",
            "type": "string",
            "nullable": true,
            "metadata": {}
          }
        ]
      },
      "nullable": true,
      "metadata": {}
    }
  ]
}
```

## Reading data

We define an InputData block to load the json data.
We define 2 parameters, `Path` and `Schema`, to send to the lift function as well.

```python
yaml_str = """
Parameters:
  Path:
    Description: Path to import data from
  Schema:
    Description: The schema describing the json

LiftJob:
  InputData:
    Type: load::batch_json
    Properties:
      Path: ${Path}
      JsonSchemaPath: ${Schema}
"""

history = lift(
    spark=spark,
    lift_def=yaml_str,
    parameters={
        "Path": "/path/data.json",
        "Schema": "/path/schema.json",
    }
)

```

Displaying the `InputData` can be done as follows:

```python
display(history.get("InputData"))
```

## Cleaning up the `Value`

The `Value` column is of type string and contains spaces, to remove them we write a custom python codeblock.

```python
yaml_str = """
Parameters:
  Path:
    Description: Path to import data from
  Schema:
    Description: The schema describing the json
  CleanValuePython:
    Description: Clean value python function

LiftJob:
  InputData:
    Type: load::batch_json
    Properties:
      Path: ${Path}
      JsonSchemaPath: ${Schema}

  CleanValue:
    Type: custom::python_codeblock
    Input:
      - InputData
    Properties:
      CustomFunction: ${CleanValuePython}
"""

def clean_value_python(params):
    df = params["dataframes"]["InputData"]
    return df.withColumn("Value", F.regexp_replace(F.col("Value"), "\s+", ""))

history = lift(
    spark=spark,
    lift_def=yaml_str,
    parameters={
        "Path": "/path/data.json",
        "Schema": "/path/schema.json",
        "CleanValuePython": clean_value_python,
    }
)
```

## Transforming and casting


Now the `Value` column can be cast to an integer.
Only `Value`, `dims.YEAR` and `dims.COUNTRY` are relevant, so select the columns.
Rows where `Year` is null can be filtered out as well.

```python
yaml_str = """
Parameters:
  Path:
    Description: Path to import data from
  Schema:
    Description: The schema describing the json
  CleanValuePython:
    Description: Clean value python function

LiftJob:
  InputData:
    Type: load::batch_json
    Properties:
      Path: ${Path}
      JsonSchemaPath: ${Schema}

  CleanValue:
    Type: custom::python_codeblock
    Input:
      - InputData
    Properties:
      CustomFunction: ${CleanValuePython}

  Trans:
    Type: transform::generic
    Input: CleanValue
    Properties:
      Functions:
        - cast_column:
            col: Value
            new_type: integer
        - select:
            cols:
              - col: Value
              - { col: dims.YEAR, alias: Year, cast: integer }
              - { col: dims.COUNTRY, alias: Country }
        - where:
            predicate: ["Year", "!=", "null"]
"""

def clean_value_python(params):
    df = params["dataframes"]["InputData"]
    return df.withColumn("Value", F.regexp_replace(F.col("Value"), "\s+", ""))

history = lift(
    spark=spark,
    lift_def=yaml_str,
    parameters={
        "Path": "/path/data.json",
        "Schema": "/path/schema.json",
        "CleanValuePython": clean_value_python,
    }
)
```


## Save to delta files

Lastly, we save down the `Trans` block to delta files.

```python
yaml_str = """
Parameters:
  Path:
    Description: Path to import data from
  Schema:
    Description: The schema describing the json
  CleanValuePython:
    Description: Clean value python function
  WritePath:
    Description: Destination path

LiftJob:
  InputData:
    Type: load::batch_json
    Properties:
      Path: ${Path}
      JsonSchemaPath: ${Schema}

  CleanValue:
    Type: custom::python_codeblock
    Input:
      - InputData
    Properties:
      CustomFunction: ${CleanValuePython}

  Trans:
    Type: transform::generic
    Input: CleanValue
    Properties:
      Functions:
        - cast_column:
            col: Value
            new_type: integer
        - select:
            cols:
              - col: Value
              - { col: dims.YEAR, alias: Year, cast: integer }
              - { col: dims.COUNTRY, alias: Country }
        - where:
            predicate: ["Year", "!=", "null"]

  Save:
    Type: write::batch_delta
    Input: Trans
    Properties:
      Path: ${WritePath}
      Mode: overwrite
"""

def clean_value_python(params):
    df = params["dataframes"]["InputData"]
    return df.withColumn("Value", F.regexp_replace(F.col("Value"), "\s+", ""))

history = lift(
    spark=spark,
    lift_def=yaml_str,
    parameters={
        "Path": "/path/data.json",
        "Schema": "/path/schema.json",
        "CleanValuePython": clean_value_python,
        "WritePath": "/path/vehicles/",
    }
)
```
