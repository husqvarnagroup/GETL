# Lift definition
A lift definition is a yaml file that describes how the lift job should execute.

As of now it contains the following main blocks:

* [Parameters](#parameters): The params that can be passed in the definition.
* [FileRegistry](#fileregistry): Define a file registry to keep track of what files have been processed.
* [LiftJob](#liftjob): The lift job itself that contains blocks that are executed in a sequential order.



## Parameters
All parameters that are going to be used in the lift definition needs to be defined in the `Parameters` section.

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

## FileRegistry

A file registry keeps track of what files have been processed, returning files that have not been processed yet.

See [file registry](file-registry.md) for more details.


## LiftJob
The lift job section contains multiple blocks that are executed sequentialy.

See [lift job blocks](lift-job-blocks.md) for more details.


