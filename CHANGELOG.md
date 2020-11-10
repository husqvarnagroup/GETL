# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
### Added
- Add drop_duplicates transform function

## [1.7.1] - 2020-11-10
 Allow a retention interval shorter than 7 days for delta tables

## [1.7.0] - 2020-10-30
### Added
- Write json files through write::batch_json block

### Changed
- Update dependency versions

## [1.6.3] - 2020-10-29

### Fixed
- Bugfix: When creating empty arrays they looked like array<null>. That is not supported by spark 3 so instead we create empty array<string>

## [1.6.2] - 2020-10-29
### Fixed
- Bugfix for loading empty directories with batch_delta using spark 3.0

## [1.6.1] - 2020-10-27
### Fixed
- Bugfix the Databricks optimize of file-registry after updating

## [1.6.0] - 2020-10-27
### Changed
- Changed python version requirements to include python 3.9
- Add Databricks optimize and vacuum of file-registry after updating

## [1.5.0] - 2020-10-23
### Added
- Options parameter in load::batch_json to be able to submit more settings when loading json files (like multiLine: true)

## [1.4.3] - 2020-10-21
### Changed
- Use of psycopg2.extras.execute_values to remove and simplify code

### Removed
- Utils functions chunked and flatten_rows_dict in getl/common/upsert.py

### Fixed
- When checking if a file registry exists in an empty directory or in a S3 prefix that doesn't exist, a different exception is raised

## [1.4.2] - 2020-09-30
- Critical bugfix for Hive table creation.

## [1.4.1] - 2020-09-30
- Support for PartitionBy columns for HiveTable

## [1.4.0] - 2020-09-29
### Added
- Support for PartitionBy columns in write::batch_delta

## [1.3.0] - 2020-09-28
### Added
- Support for loading csv files with batch_csv

## [1.2.0] - 2020-09-09

### Added
- Explode functionality is added to the generic transform block


## [1.1.0] - 2020-09-03
### Added
- Postgres upsert support

### Changed
- Schema for batch_json and batch_xml is now optional

## [1.0.1] - 2020-08-24
### Added
- Support for s3a:// paths
- python -m bin bumpversion changes the CHANGELOG.md for a release changelog
- Documentation on how to release a new version

### Fixed
- Links to versions in CHANGELOG.md
- Fix the fileregistry type in docs/migrations/s3_date_prefix_scan.md to fileregistry::s3_date_prefix_scan

## [1.0.0] - 2020-08-19
### Added
- s3_date_prefix_scan fileregistry, based upon prefix_based_date, see [migration](migrations/s3_date_prefix_scan.md).
- pyspark 3.0 support including backwards compatibility support for pyspark 2.4
- Python 3.8 support for pyspark 3.0

### Removed
- prefix_based_date fileregistry.


[Unreleased]: https://github.com/husqvarnagroup/GETL/compare/v1.7.1...HEAD
[1.7.1]: https://github.com/husqvarnagroup/GETL/compare/v1.7.0...v1.7.1
[1.7.0]: https://github.com/husqvarnagroup/GETL/compare/v1.6.3...v1.7.0
[1.6.3]: https://github.com/husqvarnagroup/GETL/compare/v1.6.2...v1.6.3
[1.6.2]: https://github.com/husqvarnagroup/GETL/compare/v1.6.1...v1.6.2
[1.6.1]: https://github.com/husqvarnagroup/GETL/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/husqvarnagroup/GETL/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/husqvarnagroup/GETL/compare/v1.4.3...v1.5.0
[1.4.3]: https://github.com/husqvarnagroup/GETL/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/husqvarnagroup/GETL/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/husqvarnagroup/GETL/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/husqvarnagroup/GETL/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/husqvarnagroup/GETL/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/husqvarnagroup/GETL/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/husqvarnagroup/GETL/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/husqvarnagroup/GETL/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/husqvarnagroup/GETL/compare/v0.2.0...v1.0.0
