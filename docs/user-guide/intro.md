# Intro

```python
from getl.lift import lift


lift(
    spark: SparkSession,
    lift_def: str,
    parameters: dict,
)
```

- `spark`: `SparkSession`, usually the global variable `spark` in databricks
- `lift_def`: `str`, can be a yaml formatted string as lift definition or an s3 path with the yaml file
- `parameters`: `dict`, all `${string}` formatted strings will be replaced with the parameters provided here


## Examples

Example: Yaml formatted string

```python
yaml_str = """
Parameters:
  Path:
    Description: Path to import data from

LiftJob:
  TrustedFiles:
    Type: load::batch_parquet
    Properties:
      Path: ${Path}
"""

lift(
    spark=spark,
    lift_def=yaml_str,
    parameters={
        "Path": "s3://bucket/with/data"
    }
)
```

Example: s3 path


```python
lift(
    spark=spark,
    lift_def="s3://bucket-name/path/to/file.yml",
    parameters={
        "Path": "s3://bucket/with/data"
    }
)
```
