"""Readers for NOAA ObsPack netCDF data."""

import xarray as xr


_COORDINATES = ("time", "latitude", "longitude", "altitude", "obspack_id")


def _set_coordinates(ds):
    coordinates = [name for name in _COORDINATES if name in ds.variables]
    return ds.set_coords(coordinates) if coordinates else ds


def open_dataset(filename, **kwargs):
    """Open an ObsPack netCDF file as an :class:`xarray.Dataset`.

    ObsPack's ``time``, ``latitude``, ``longitude``, ``altitude``, and
    ``obspack_id`` variables are exposed as coordinates when present. Time
    decoding and other options are passed through to
    :func:`xarray.open_dataset`.

    Parameters
    ----------
    filename : path-like
        Path to an ObsPack netCDF file.
    **kwargs
        Additional keyword arguments passed to :func:`xarray.open_dataset`.

    Returns
    -------
    xarray.Dataset
    """
    return _set_coordinates(xr.open_dataset(filename, **kwargs))


def open_mfdataset(paths, *, concat_dim="obs", **kwargs):
    """Open and concatenate multiple ObsPack netCDF files.

    Files are concatenated along ``concat_dim`` in the order provided by
    ``paths``. Coordinate variables are set on each file before concatenation.

    Parameters
    ----------
    paths : path-like, sequence of path-like, or str
        File path(s) or a glob pattern understood by
        :func:`xarray.open_mfdataset`.
    concat_dim : str, default: ``"obs"``
        Dimension along which to concatenate the files.
    **kwargs
        Additional keyword arguments passed to :func:`xarray.open_mfdataset`.

    Returns
    -------
    xarray.Dataset
    """
    return xr.open_mfdataset(
        paths,
        combine="nested",
        concat_dim=concat_dim,
        preprocess=_set_coordinates,
        **kwargs,
    )
