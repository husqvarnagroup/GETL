# Installation

## Prerequisites

- [Python 3.7](https://www.python.org/downloads/) (Python 3.8 support comming soon with spark 3.0)
- [Apache Spark 2.4](https://spark.apache.org/downloads.html)

## Install the GETL

Install GETL by running:

```sh
# This will be the preferred way but until the code is deployed on PyPI, this is not possible
pip install {TODO}
```

Until the GETL is published on PyPI, installation can be done through the git url.

```sh
pip install git+https://github.com/husqvarnagroup/GETL.git
```

### On a databricks cluster

The GETL is only tested on Databricks Runtime Version 6.5 (Apache Spark 2.4.5)

To install the GETL in databricks, the GETL will need to be added to the cluster Libraries.

- Library Source: `PyPI`
- Package: `git+https://github.com/husqvarnagroup/GETL.git`
    - This will be changed once GETL is published on PyPI
