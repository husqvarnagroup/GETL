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
