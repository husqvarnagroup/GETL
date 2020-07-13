# File registry 

A file registry keeps track of what files have been processed, producing a delta of files that have not been processed yet.

This is true for each file registry:

* has a sub block that is annotated as `block::sub-block`
* outputs a list of unprocessed files 
* only some [load blocks](lift-job-blocks.md#load) can be configured with a file registry


```yaml
FileRegistry:
  {BlockName}
    Type: {block::sub-block}
    Input: {BlockInput}
    Properties:
      {Prop}: {Prop}

```


Example:

```yml
FileRegistry:
  S3FullScan:
    Type: fileregistry::s3_full_scan
    Properties:
      BasePath: s3://datalake/file-registry/dateset-a
      UpdateAfter: WriteToService
      HiveDatabaseName: file_registry
      HiveTableName: dataset-a

LiftJob:
  TrustedFiles:
    Type: load::batch_parquet
    Properties:
      Path: ${Path}
      FileRegistry: S3Fullscan

  WriteToService:
    Type: write::batch_delta
    Input: TrustedFiles
    Properties:
      Path: s3://path/to/prefix
      Mode: overwrite 

```

## AWS S3 

The following file registrys can only be used with AWS S3 service.

<file-registry>
