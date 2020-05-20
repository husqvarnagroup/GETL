# Installation

## Through pip

Install GETL by running:

```sh
# This will be the preferred way but until the code is deployed on PyPI, this is not possible
pip install {TODO}
```

Or by cloning the GETL repository.

```sh
git clone git@github.com:husqvarnagroup/GETL.git
pip install GETL
```

Or installing through git+https immediately:

```sh
pip install git+https://github.com/husqvarnagroup/GETL.git
```

## On a databricks cluster

To install the GETL in databricks, the GETL will need to be added to the cluster Libraries.

- Library Source: `PyPI`
- Package: `git+https://github.com/husqvarnagroup/GETL.git`
    - This will be changed once GETL is published on PyPI
