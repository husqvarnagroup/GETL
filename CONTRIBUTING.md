# Contributing

## Prerequisites

- [Python 3.7+](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/)

## Install for development

Install GETL by running:

```sh
# Install dependencies and dev dependencies
poetry install

# Install the git pre-commit hooks
poetry run pre-commit install
```

## Running tests

```sh
poetry run pytest
```

## Running docs

[mkdocs](https://www.mkdocs.org/) is being used to build the documentation,
serving the docs can be done by running:

```sh
poetry run mkdocs serve
```

The documentation will then be available on [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Submitting pull requests

Once you are happy with your changes, create a [Pull Request](https://github.com/husqvarnagroup/GETL/pull/new/master).


## Releasing a new version

Releasing a new version is done through the command:

```bash
poetry run python -m bin bumpversion {patch,minor,major}
```

The command will bump the version, fix the headers in CHANGELOG.md for release and create a tag.

Pushing the code to github and it's tags will create a release on PyPI automatically.

```bash
git push
git push --tags
```

If the release needs to be undone, reset the last commit and delete the tag before pushing to github.

```bash
git reset --hard HEAD^ # Will remove the last commit
git tag -d v1.0.1 # Will remove the tag
```


### Example release

The current version is `1.0.0` and we want patch the version to version `1.0.1`.

```bash
poetry run python -m bin bumpversion patch
```

Once the files are changed, it will prompt to add the changes to the git commit

```diff
diff --git a/CHANGELOG.md b/CHANGELOG.md
index c36cae2..e17dd2a 100644
--- a/CHANGELOG.md
+++ b/CHANGELOG.md
@@ -6,6 +6,8 @@ and this project adheres to [Semantic Versioning](https://semver.or


 ## [Unreleased]
+
+## [1.0.1] - 2020-08-24
 ### Added
 - python -m bin bumpversion changes the CHANGELOG.md for a release changelog
 - documentation on how to release a new version
Stage this hunk [y,n,q,a,d,j,J,g,/,e,?]? y

@@ -23,5 +25,6 @@ and this project adheres to [Semantic Versioning](https://semver.or
 - prefix_based_date fileregistry.


-[Unreleased]: https://github.com/husqvarnagroup/GETL/compare/v1.0.0...HEAD
+[Unreleased]: https://github.com/husqvarnagroup/GETL/compare/v1.0.1...HEAD
+[1.0.1]: https://github.com/husqvarnagroup/GETL/compare/v1.0.0...v1.0.1
 [1.0.0]: https://github.com/husqvarnagroup/GETL/compare/v0.2.0...v1.0.0
Stage this hunk [y,n,q,a,d,j,J,g,/,e,?]? y

diff --git a/pyproject.toml b/pyproject.toml
index 6e6dea2..5d739da 100644
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -1,6 +1,6 @@
 [tool.poetry]
 name = "husqvarna-getl"
-version = "1.0.0"
+version = "1.0.1"
 description = "An elegant way to ETL'ing"
 documentation = "https://getl.readthedocs.io/"
 repository = "https://github.com/husqvarnagroup/GETL/"
Stage this hunk [y,n,q,a,d,e,?]? y
```

You can verify the commit by running:

```bash
git show
```

Output:

```diff
commit b013666137167969daa9e3c78bd490d7b382b1e3 (HEAD -> master, tag: v1.0.1)
Author: Niels Lemmens <draso.odin@gmail.com>
Date:   Mon Aug 24 10:08:26 2020 +0200

    Bump version from v1.0.0 to v1.0.1

diff --git a/CHANGELOG.md b/CHANGELOG.md
index c36cae2..e17dd2a 100644
--- a/CHANGELOG.md
+++ b/CHANGELOG.md
@@ -6,6 +6,8 @@ and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0


 ## [Unreleased]
+
+## [1.0.1] - 2020-08-24
 ### Added
 - python -m bin bumpversion changes the CHANGELOG.md for a release changelog
 - documentation on how to release a new version
@@ -23,5 +25,6 @@ and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0
 - prefix_based_date fileregistry.


-[Unreleased]: https://github.com/husqvarnagroup/GETL/compare/v1.0.0...HEAD
+[Unreleased]: https://github.com/husqvarnagroup/GETL/compare/v1.0.1...HEAD
+[1.0.1]: https://github.com/husqvarnagroup/GETL/compare/v1.0.0...v1.0.1
 [1.0.0]: https://github.com/husqvarnagroup/GETL/compare/v0.2.0...v1.0.0
diff --git a/pyproject.toml b/pyproject.toml
index 6e6dea2..5d739da 100644
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -1,6 +1,6 @@
 [tool.poetry]
 name = "husqvarna-getl"
-version = "1.0.0"
+version = "1.0.1"
 description = "An elegant way to ETL'ing"
 documentation = "https://getl.readthedocs.io/"
 repository = "https://github.com/husqvarnagroup/GETL/"
```
