---
type: howto
category: how_to
tags: [installation, platforms, support]
---

# Installation

## Supported Platforms

MONETIO officially supports Linux and macOS. Windows is not an officially supported or CI-tested platform.

## Required Dependencies

- Python 3.8+
- [numpy](https://numpy.org/)
- [pandas](https://pandas.pydata.org/)
- [xarray](https://xarray.pydata.org/)
- [dask](https://dask.org/)
- [netcdf4](https://unidata.github.io/netcdf4-python/)
- [s3fs](https://github.com/fsspec/s3fs)
- [scipy](https://scipy.org/)

## Optional Dependencies

Some features require additional packages:

- **VirtualiZarr**: `virtualizarr`, `obstore`, `obspec_utils`, `ujson`, and `zarr`
- **GRIB2 support**: `grib2io`
- **HDF4 support**: `pyhdf`

## Instructions

MONETIO is a pure Python package. The easiest way to install it is using `pip` or `conda`.

### Using pip

You can install MONETIO directly from PyPI:

```bash
pip install monetio
```

Or from the GitHub repository:

```bash
pip install git+https://github.com/noaa-oar-arl/monetio.git
```

### Using conda

MONETIO is available on the `conda-forge` channel:

```bash
conda install -c conda-forge monetio
```

### Development Installation

If you want to contribute to MONETIO, you can install it in editable mode:

```bash
git clone https://github.com/noaa-oar-arl/monetio.git
cd monetio
pip install -e .
```
