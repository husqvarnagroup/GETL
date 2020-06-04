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

# Then the Path variable is accessible by using ${Path}

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

Example:

```yml
LiftJob:

  TrustedFiles:
    Type: load::batch_parquet
    Properties:
      Path: s3://bucket/path/to/data

  PerformOperation:
    Type: transform::generic
    Input: TrustedFiles
    Properties:
      Functions:
        - add_column.date.unixtime_to_utc:
            from_column: timestamp
            to_column: date

  FilterOperation:
    Type: transform::generic
    Input: PerformOperation
    Properties:
      Functions:
        - where:
            predicate: [date, '>=', '2020-01-01']
        - where:
            predicate: [company, '==', 'Husqvarna']
```


## FileRegistry

TODO
  
