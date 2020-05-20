# Lift definitions - base


## Parameters

- field: `Parameters`  
  type: dict  
  properties:
    - field: `{string}`  
      type: dict  
      properties:
        - field: `Description`  
          type: string

Example:

```yml
Parameters:
  Path:
    Description: Path to import data from

LiftJob:
  TrustedFiles:
    Type: load::batch_parquet
    Properties:
      Path: ${Path}
```


## LiftJob

List of jobs to do.

- field: `LiftJob`  
  type: dict
  properties:
    - field: `{string}`
      type: dict  
      properties:
        - field: `Type`  
          type: string, see [lift job types](lift-job-types.md)
        - field: `Properties`  
          type: dict, see [lift job types](lift-job-types.md)


## FileRegistry

Too complicated, just don't use it...
  
