# Migrate from prefix_based_date to s3_prefix_scan

Before, `prefix_based_date` uses a `BasePrefix`, now `s3_prefix_scan` uses the attribute `BasePath`.

Before lift.yml:

```yml
FileRegistry:
  DateFileReg:
    Type: fileregistry::prefix_based_date
    Properties:
      BasePrefix: ${FileRegistryBasePrefix}
      UpdateAfter: WriteToS3
      HiveDatabaseName: file_registry_dev
      HiveTableName: example
      DefaultStartDate: "2020-01-01"
      PartitionFormat: year=%Y/month=%m/day=%d
```


Now lift.yaml:

```yml
FileRegistry:
  DateFileReg:
    Type: fileregistry::s3_prefix_scan
    Properties:
      BasePath: ${FileRegistryBasePrefix}
      UpdateAfter: WriteToS3
      HiveDatabaseName: file_registry_dev
      HiveTableName: example
      DefaultStartDate: "2020-01-01"
      PartitionFormat: year=%Y/month=%m/day=%d
```

## Keep backwards compatibility

The file registry path was calculated from the prefix, and appending the input (excluding s3 bucket).

Example with following attributes:

- Fileregistry BasePrefix: `s3://example/prefix/`
- Load Input: `s3://other-example/path/to/files/`

Would become:

- Fileregistry BasePath: `s3://example/prefix/path/to/files/`


**It is highly advised to double check your s3 bucket what the path is, just to be extra careful!**
