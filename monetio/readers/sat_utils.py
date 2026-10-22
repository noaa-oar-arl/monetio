"""Satellite Reader Utilities"""

import datetime
from collections.abc import Callable
from typing import TYPE_CHECKING, Union

import numpy as np

if TYPE_CHECKING:
    import dask.dataframe as dd
import pandas as pd
import xarray as xr


def apply_qa_mask(ds: xr.Dataset, qa_var: str = "qa_value", threshold: float = 0.5) -> xr.Dataset:
    """Apply quality flag masking to a dataset.

    Masks all data variables where the QA variable is below the threshold,
    while preserving the QA variable itself and all coordinates.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset containing a QA variable.
    qa_var : str
        Name of the quality flag variable.
    threshold : float
        Minimum acceptable quality value.

    Returns
    -------
    xr.Dataset
        Dataset with low-quality values masked as NaN.
    """
    if qa_var not in ds.data_vars:
        return ds
    qa = ds[qa_var]
    mask = qa >= threshold
    ds = ds.where(mask)
    ds[qa_var] = qa  # Restore unmasked QA values
    return ds


def convert_ppmv_to_ppbv(ds: xr.Dataset, variables: list[str] | None = None) -> xr.Dataset:
    """Convert gas-phase variables from ppmV to ppbV (multiply by 1000).

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    variables : list of str, optional
        Variables to convert. If None, converts all variables with
        units containing 'ppm'.

    Returns
    -------
    xr.Dataset
        Dataset with converted units.
    """
    if variables is None:
        variables = [v for v in ds.data_vars if "ppm" in ds[v].attrs.get("units", "").lower()]
    for var in variables:
        if var in ds.data_vars:
            ds[var] = ds[var] * 1000.0
            if "units" in ds[var].attrs:
                ds[var].attrs["units"] = ds[var].attrs["units"].replace("ppm", "ppb")
    return ds


def lazy_index_along_axis(data: xr.DataArray, index: xr.DataArray, dim: str) -> xr.DataArray:
    """
    Index a dimension using another DataArray lazily, handling both Eager and Dask.
    Fixes the 'vindex does not support indexing with dask objects' limitation.

    Parameters
    ----------
    data : xr.DataArray
        DataArray to index. Must have dimension `dim`.
    index : xr.DataArray
        DataArray of indices.
    dim : str
        Dimension name to index along.

    Returns
    -------
    xr.DataArray
        The indexed DataArray.
    """

    def _index_func(arr, idx):
        # In apply_ufunc with input_core_dims=[[dim], []], the core dimension
        # is moved to the last axis of arr.
        # arr shape: (..., dim_size)
        # idx shape: (...)
        idx_expanded = idx[..., np.newaxis]
        return np.take_along_axis(arr, idx_expanded, axis=-1).squeeze(axis=-1)

    return xr.apply_ufunc(
        _index_func,
        data,
        index,
        input_core_dims=[[dim], []],
        output_core_dims=[[]],
        dask="parallelized",
        output_dtypes=[data.dtype],
        dask_gufunc_kwargs={"allow_rechunk": True},
    )


def apply_lazy_conversion(
    data: xr.DataArray, func: Callable, output_dtype: str | np.dtype | type
) -> xr.DataArray:
    """
    Apply a conversion function lazily to a DataArray backend-agnostic.

    Parameters
    ----------
    data : xr.DataArray
        Input DataArray.
    func : Callable
        Function to apply. Should work on NumPy arrays.
    output_dtype : Union[str, np.dtype, type]
        Expected output dtype.

    Returns
    -------
    xr.DataArray
        Converted DataArray.
    """

    def _wrapped_func(x):
        res = func(x)
        # Ensure result is a NumPy array to avoid issues with Dask/Xarray
        # (e.g. DatetimeIndex causing transpose errors)
        if hasattr(res, "to_numpy"):
            return res.to_numpy()
        return np.asarray(res)

    return xr.apply_ufunc(
        _wrapped_func,
        data,
        dask="parallelized",
        output_dtypes=[output_dtype],
    )


def standardize_satellite_coords(
    ds: xr.Dataset,
    lat_name: str = "Latitude",
    lon_name: str = "Longitude",
    y_dim: str | list[str] = ["Rows", "scanline", "nlat", "lat", "nscan", "nTimes"],
    x_dim: str | list[str] = ["Columns", "ground_pixel", "nlon", "lon", "nstep", "nIFOV"],
    z_dim: str | list[str] = ["Levels", "layer", "level", "nLayer"],
    time_name: str = "Time",
) -> xr.Dataset:
    """
    Standardize satellite swath/gridded coordinates and dimensions.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    lat_name : str, optional
        Name of the latitude coordinate in the file, by default "Latitude".
    lon_name : str, optional
        Name of the longitude coordinate in the file, by default "Longitude".
    y_dim : str or list of str, optional
        Name(s) of the y/row dimension in the file, by default ["Rows", "scanline"].
    x_dim : str or list of str, optional
        Name(s) of the x/column dimension in the file, by default ["Columns", "ground_pixel"].

    Returns
    -------
    xr.Dataset
        Dataset with standardized dimensions (y, x) and coordinates (latitude, longitude).
    """
    if isinstance(y_dim, str):
        y_dim = [y_dim]
    if isinstance(x_dim, str):
        x_dim = [x_dim]

    rename_dict = {}
    for y in y_dim:
        if y in ds.dims:
            rename_dict[y] = "y"
            break
    for x in x_dim:
        if x in ds.dims:
            rename_dict[x] = "x"
            break
    for z in z_dim:
        if z in ds.dims:
            rename_dict[z] = "z"
            break

    if rename_dict:
        ds = ds.rename(rename_dict)

    coord_rename = {}
    # Case insensitive search for lat/lon if not found exactly
    # Try to find latitude/longitude
    actual_lat = None
    lat_names = [lat_name, "latitude", "lat", "LAT", "Latitude"]
    for ln in lat_names:
        if ln in ds.variables:
            actual_lat = ln
            break
    if actual_lat is None:
        for v in ds.variables:
            if v.lower() in ["latitude", "lat"]:
                actual_lat = v
                break

    actual_lon = None
    lon_names = [lon_name, "longitude", "lon", "LON", "Longitude"]
    for ln in lon_names:
        if ln in ds.variables:
            actual_lon = ln
            break
    if actual_lon is None:
        for v in ds.variables:
            if v.lower() in ["longitude", "lon"]:
                actual_lon = v
                break

    if actual_lat and actual_lat != "latitude":
        coord_rename[actual_lat] = "latitude"
    if actual_lon and actual_lon != "longitude":
        coord_rename[actual_lon] = "longitude"

    if coord_rename:
        ds = ds.rename(coord_rename)

    # Ensure they are coordinates
    to_set = []
    if "latitude" in ds.variables and "latitude" not in ds.coords:
        to_set.append("latitude")
    if "longitude" in ds.variables and "longitude" not in ds.coords:
        to_set.append("longitude")
    if to_set:
        ds = ds.set_coords(to_set)

    if "latitude" in ds.coords:
        ds["latitude"].attrs.update({"units": "degrees_north", "standard_name": "latitude"})
    if "longitude" in ds.coords:
        ds["longitude"].attrs.update({"units": "degrees_east", "standard_name": "longitude"})

    # Handle Time
    if time_name in ds.variables and "time" not in ds.variables:
        ds = ds.rename({time_name: "time"})
    elif "time" not in ds.variables:
        for v in ds.variables:
            if v.lower() == "time":
                ds = ds.rename({v: "time"})
                break

    if "time" in ds.variables and "time" not in ds.coords:
        # If it's a coordinate-like variable, set it
        if "time" in ds.dims or ds["time"].ndim == 1:
            ds = ds.set_coords("time")

    return ds


def update_history(ds: Union[xr.Dataset, xr.DataArray, pd.DataFrame, "dd.DataFrame"], message: str):
    """
    Update the 'history' attribute of a dataset or dataframe backend-agnostic.

    Parameters
    ----------
    ds : xarray.Dataset, xarray.DataArray, pandas.DataFrame, or dask.DataFrame
        Input object.
    message : str
        Message to add to history.

    Returns
    -------
    object
        The input object with updated history.
    """
    if not hasattr(ds, "attrs"):
        # For dask objects that don't have attrs yet or other edge cases
        return ds

    history = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {message}"
    try:
        if "history" in ds.attrs:
            ds.attrs["history"] = f"{ds.attrs['history']}\n{history}"
        else:
            ds.attrs["history"] = history
    except (AttributeError, TypeError):
        # Some dask-backed objects might have 'attrs' but it might be read-only or similar
        pass
    return ds


def jpss_time_to_datetime(
    time_array: xr.DataArray, origin: str = "1958-01-01", unit: str = "us"
) -> xr.DataArray:
    """
    Convert JPSS time (usually microseconds since 1958) to datetime64[ns].

    Parameters
    ----------
    time_array : xr.DataArray
        Input time array.
    origin : str, optional
        Origin date, by default "1958-01-01".
    unit : str, optional
        Time unit, by default "us" (microseconds).

    Returns
    -------
    xr.DataArray
        Time array in datetime64[ns].
    """

    def _convert(t):
        return pd.to_datetime(t, unit=unit, origin=origin)

    return apply_lazy_conversion(time_array, _convert, "datetime64[ns]")


def tai93_to_datetime(time_array: xr.DataArray) -> xr.DataArray:
    """
    Convert TAI93 time (seconds since 1993-01-01) to datetime64[ns].

    Parameters
    ----------
    time_array : xr.DataArray
        Input time array in seconds since 1993-01-01 00:00:00 UTC.

    Returns
    -------
    xr.DataArray
        Time array in datetime64[ns].

    Examples
    --------
    >>> ds["time"] = tai93_to_datetime(ds["Scan_Start_Time"])
    """

    def _convert(t):
        # pd.to_datetime expects 1D input
        # We use .values to return a numpy array from the pandas series
        return pd.to_datetime(t.ravel(), unit="s", origin="1993-01-01").values.reshape(t.shape)

    return apply_lazy_conversion(time_array, _convert, "datetime64[ns]")


def add_time_coord(
    ds: xr.Dataset,
    time_val: datetime.datetime | None = None,
    time_attr: str | None = None,
) -> xr.Dataset:
    """
    Add a time dimension and coordinate to the dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    time_val : datetime.datetime, optional
        Explicit time value to use.
    time_attr : str, optional
        Attribute name to extract time from if time_val is None.

    Returns
    -------
    xr.Dataset
        Dataset with 'time' dimension and coordinate.
    """
    if time_val is None and time_attr is not None:
        if time_attr in ds.attrs:
            try:
                time_val = pd.to_datetime(ds.attrs[time_attr])
            except (ValueError, TypeError):
                pass

    if time_val is not None:
        if isinstance(time_val, str):
            time_val = pd.to_datetime(time_val)
        ds = ds.expand_dims("time")
        ds["time"] = [time_val]

    return ds
