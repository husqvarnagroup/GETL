# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
### Added
- Postgres upsert support

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


[Unreleased]: https://github.com/husqvarnagroup/GETL/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/husqvarnagroup/GETL/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/husqvarnagroup/GETL/compare/v0.2.0...v1.0.0
