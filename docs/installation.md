# Installation

## Prerequisites

- [Python](https://www.python.org/downloads/) versions 3.7, 3.8 and 3.9 are supported

## Install the GETL

Install GETL by running:

```sh
# This will be the preferred way but until the code is deployed on PyPI, this is not possible
pip install husqvarna-getl
```

### On a databricks cluster

The GETL is only tested on Databricks Runtime Version 6.5 (Apache Spark 2.4.5)

To install the GETL in databricks, the GETL will need to be added to the cluster Libraries.

- Library Source: `PyPI`
- Package: husqvarna-getl
