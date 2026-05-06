"""
Generic model reader.

You can use this for gridded data that is not officially supported
but can be coaxed into the MONET format by renaming and coord conversions.
"""

import warnings

import pandas as pd
import xarray as xr


def _maybe_rename_dim(ds, old, new):
    if old is None:
        return ds
    else:
        if old in ds.dims:
            if new != old:  # otherwise ValueError: dim already exists
                return ds.rename_dims({old: new})
        else:
            s_dims = ", ".join(f"{dim} ({size})" for dim, size in ds.sizes.items())
            warnings.warn(
                f"Dimension {old} not found in Dataset, cannot rename to {new}. "
                f"Available dimensions: {s_dims}",
                stacklevel=3,
            )
            return ds


def _maybe_rename_var(ds, old, new):
    if old is None:
        return ds
    else:
        if old in ds.variables:
            # Note: no error if new == old
            return ds.rename_vars({old: new})
        else:
            s_vars = ", ".join(sorted(ds.variables))
            warnings.warn(
                f"Variable {old} not found in Dataset, cannot rename to {new}. "
                f"Available variables: {s_vars}",
                stacklevel=3,
            )
            return ds


def _monetify(
    ds,
    *,
    x_dim="lon",
    y_dim="lat",
    z_dim=None,
    time_dim="time",
    lon_var="lon",
    lat_var="lat",
    pres_var=None,
    hgt_var=None,
    time_var="time",
    attrs=None,
):
    """Convert a dataset to MONET format by renaming dimensions and variables."""

    # Rename dims
    ds = _maybe_rename_dim(ds, x_dim, "x")
    ds = _maybe_rename_dim(ds, y_dim, "y")
    ds = _maybe_rename_dim(ds, z_dim, "z")
    ds = _maybe_rename_dim(ds, time_dim, "time")

    # Rename spatial coord vars
    ds = _maybe_rename_var(ds, lon_var, "longitude")
    ds = _maybe_rename_var(ds, lat_var, "latitude")
    ds = _maybe_rename_var(ds, pres_var, "pres_pa_mid")
    ds = _maybe_rename_var(ds, hgt_var, "alt_agl_m_mid")

    # If time is not in pandas format, change it to pandas format
    if not isinstance(ds.indexes[time_var], pd.DatetimeIndex):
        ds = ds.assign({time_var: ds.indexes[time_var].to_datetimeindex(unsafe=True)})
    ds = _maybe_rename_var(ds, time_var, "time")

    # Ensure coords are set
    ds = ds.set_coords(["time", "latitude", "longitude"])  # required coords
    if "pres_pa_mid" in ds.variables:
        ds = ds.set_coords("pres_pa_mid")
    if "alt_agl_m_mid" in ds.variables:
        ds = ds.set_coords("alt_agl_m_mid")

    # Convert longitude from assumed [0, 360) to [-180, 180) if needed
    ds["longitude"] = xr.where(ds["longitude"] >= 180, ds["longitude"] - 360, ds["longitude"])

    # Ensure correct dim order
    ds = ds.transpose("time", "z", "y", "x")

    # Add dataset-level attributes if provided (e.g. cen_lon, cen_lat)
    if attrs is not None:
        ds.attrs.update(attrs)

    return ds
