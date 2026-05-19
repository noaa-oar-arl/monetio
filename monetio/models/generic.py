"""
Generic model reader.

You can use this for gridded data that is not officially supported
but can be coaxed into the MONET format by renaming and coord conversions.
"""

import warnings

import pandas as pd
import xarray as xr


def _maybe_rename_dim(ds, old, new, *, required=False):
    s_dims = ", ".join(f"{dim} ({size})" for dim, size in ds.sizes.items())
    if old is None:
        if new in ds.dims or not required:
            return ds
        else:
            raise ValueError(
                "Dimension to rename was not provided, "
                f"and {new} is not in the Dataset dimensions ({s_dims})."
            )
    else:
        if old in ds.dims:
            if new != old:  # otherwise ValueError: dim already exists
                return ds.rename_dims({old: new})
            else:
                return ds
        else:
            if new in ds.dims:
                warnings.warn(
                    f"Dimension {new} already exists in Dataset, cannot rename {old} to {new}.",
                    stacklevel=3,
                )
                return ds
            else:
                msg = (
                    f"Dimension {old} not found in Dataset, cannot rename to {new}. "
                    f"Available dimensions: {s_dims}"
                )
                if required:
                    raise ValueError(msg)
                else:
                    warnings.warn(msg, stacklevel=3)
                    return ds


def _maybe_rename_var(ds, old, new, *, required=False):
    s_vars = ", ".join(sorted(ds.variables))
    if old is None:
        if new in ds.variables or not required:
            return ds
        else:
            raise ValueError(
                "Variable to rename was not provided, "
                f"and {new} is not in the Dataset variables ({s_vars})."
            )
    else:
        if old in ds.variables:
            return ds.rename_vars({old: new})  # Note: no error if new == old
        else:
            if new in ds.variables:
                warnings.warn(
                    f"Variable {new} already exists in Dataset, cannot rename {old} to {new}.",
                    stacklevel=3,
                )
                return ds
            else:
                msg = (
                    f"Variable {old} not found in Dataset, cannot rename to {new}. "
                    f"Available variables: {s_vars}"
                )
                if required:
                    raise ValueError(msg)
                else:
                    warnings.warn(msg, stacklevel=3)
                    return ds


def _monetify(
    ds,
    *,
    x_dim=None,  # e.g. 'lon'
    y_dim=None,  # e.g. 'lat'
    z_dim=None,  # e.g. 'lev'
    time_dim=None,  # e.g. 'time'
    lon_var=None,  # e.g. 'lon'
    lat_var=None,  # e.g. 'lat'
    pres_var=None,  # e.g. 'pfull'
    hgt_var=None,
    time_var=None,  # e.g. 'time'
    attrs=None,
):
    """Convert a dataset to MONET format by renaming dimensions and variables."""

    # If time is a dim coord, take name if not passed
    if time_dim is not None and time_var is None and time_dim in ds.coords:
        time_var = time_dim

    # Rename dims
    ds = _maybe_rename_dim(ds, x_dim, "x", required=True)
    ds = _maybe_rename_dim(ds, y_dim, "y", required=True)
    ds = _maybe_rename_dim(ds, z_dim, "z")
    ds = _maybe_rename_dim(ds, time_dim, "time", required=True)

    # Rename spatial coord vars
    ds = _maybe_rename_var(ds, lon_var, "longitude", required=True)
    ds = _maybe_rename_var(ds, lat_var, "latitude", required=True)
    ds = _maybe_rename_var(ds, pres_var, "pres_pa_mid")
    ds = _maybe_rename_var(ds, hgt_var, "alt_agl_m_mid")
    ds = _maybe_rename_var(ds, time_var, "time", required=True)

    # If time is not an index, make it so
    if "time" not in ds.coords:
        ds = ds.set_coords("time")  # must be coord to be index
    if "time" not in ds.indexes:
        ds = ds.set_xindex("time")

    # If time is not in pandas format, change it to pandas format
    if not isinstance(ds.indexes["time"], pd.DatetimeIndex):
        ds = ds.assign({"time": ds.indexes["time"].to_datetimeindex(unsafe=True)})

    # Convert longitude from assumed [0, 360) to [-180, 180) if needed
    ds["longitude"] = xr.where(ds["longitude"] >= 180, ds["longitude"] - 360, ds["longitude"])

    # If lat and lon are 1-D convert to 2-D
    lat, lon = ds["latitude"], ds["longitude"]
    if lat.ndim == 1 and lon.ndim == 1:
        lon_2d, lat_2d = xr.broadcast(lon, lat)
        ds = ds.assign(latitude=lat_2d, longitude=lon_2d)
    elif lat.ndim == 2 and lon.ndim == 2:
        pass
    else:
        raise ValueError(
            f"Latitude and longitude must be either 1-D or 2-D. "
            f"Got latitude with shape {lat.shape} and longitude with shape {lon.shape}."
        )

    # Ensure coords are set
    ds = ds.set_coords(["time", "latitude", "longitude"])  # required coords
    if "pres_pa_mid" in ds.variables:
        ds = ds.set_coords("pres_pa_mid")
    if "alt_agl_m_mid" in ds.variables:
        ds = ds.set_coords("alt_agl_m_mid")

    # Put z dim in if not there
    if "z" not in ds.dims:
        ds = ds.expand_dims("z")

    # Ensure correct dim order
    ds = ds.transpose("time", "z", "y", "x", missing_dims="ignore")

    # Add dataset-level attributes if provided (e.g. cen_lon, cen_lat)
    if attrs is not None:
        ds.attrs.update(attrs)

    return ds


def _maybe_select_surface(ds, surf_only, surf_lev=0):
    if not surf_only or "z" not in ds.dims or ds.sizes["z"] == 1:
        return ds

    iz = int(surf_lev)

    return (
        ds.isel({"z": iz}).expand_dims("z").transpose("time", "z", "y", "x", missing_dims="ignore")
    )


def _maybe_select_vars(ds, var_list):
    if var_list is None:
        return ds

    s_vars = ", ".join(sorted(ds.variables))
    filtered_var_list = []
    for vn in var_list:
        if vn in ds.coords:
            continue
        elif vn in ds.data_vars:
            filtered_var_list.append(vn)
        else:
            warnings.warn(
                f"Variable {vn} not found in Dataset, cannot select it. "
                f"Available variables: {s_vars}",
                stacklevel=3,
            )

    return ds[filtered_var_list]


def open_dataset(
    path,
    *,
    x_dim=None,
    y_dim=None,
    z_dim=None,
    time_dim=None,
    lon_var=None,
    lat_var=None,
    pres_var=None,
    hgt_var=None,
    time_var=None,
    attrs=None,
    #
    surf_only=False,
    surf_lev=0,
    var_list=None,
    #
    **kwargs,
):
    """Open a dataset."""
    raw = xr.open_dataset(path, **kwargs)
    ds = _monetify(
        raw,
        x_dim=x_dim,
        y_dim=y_dim,
        z_dim=z_dim,
        time_dim=time_dim,
        lon_var=lon_var,
        lat_var=lat_var,
        pres_var=pres_var,
        hgt_var=hgt_var,
        time_var=time_var,
        attrs=attrs,
    )
    ds = _maybe_select_surface(ds, surf_only, surf_lev=surf_lev)
    ds = _maybe_select_vars(ds, var_list)
    return ds


def open_mfdataset(
    paths,
    *,
    x_dim=None,
    y_dim=None,
    z_dim=None,
    time_dim=None,
    lon_var=None,
    lat_var=None,
    pres_var=None,
    hgt_var=None,
    time_var=None,
    attrs=None,
    #
    surf_only=False,
    surf_lev=0,
    var_list=None,
    #
    **kwargs,  # e.g. combine='nested', concat_dim=<time_dim>
):
    """Open multiple datasets."""
    raw = xr.open_mfdataset(paths, **kwargs)
    ds = _monetify(
        raw,
        x_dim=x_dim,
        y_dim=y_dim,
        z_dim=z_dim,
        time_dim=time_dim,
        lon_var=lon_var,
        lat_var=lat_var,
        pres_var=pres_var,
        hgt_var=hgt_var,
        time_var=time_var,
        attrs=attrs,
    )
    ds = _maybe_select_surface(ds, surf_only, surf_lev=surf_lev)
    ds = _maybe_select_vars(ds, var_list)
    return ds
