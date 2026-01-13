"""
Reader for RAQMS real-time files.

RAQMS: Realtime Air Quality Monitoring System

More information: http://raqms-ops.ssec.wisc.edu/
"""

import xarray as xr


def open_dataset(fname, *, convert_to_ppb=True, surf_only=False):
    """Open a single dataset from RAQMS output. Currently expects netCDF file format.

    Parameters
    ----------
    fname : str
        File to be opened.
    convert_to_ppb : bool
        If true the units of the gas species will be converted to ppbv

    Returns
    -------
    xarray.Dataset
    """
    names, netcdf = _ensure_mfdataset_filenames(fname)
    if not netcdf:
        raise ValueError(
            "File format not supported. Note that files should be preprocessed to netCDF."
        )

    ds = xr.open_dataset(names[0], drop_variables=["theta"])
    ds = _fix(ds, surf_only=surf_only, convert_to_ppb=convert_to_ppb)

    return ds


def open_mfdataset(fname, *, convert_to_ppb=True, var_list=None, surf_only=False):
    """Open a multiple file dataset from RAQMS output.

    Parameters
    ----------
    fname : str or list of str
        Files to be opened, expressed as a glob string or list of string paths.
    convert_to_ppb : bool
        If true the units of the gas species will be converted to ppbv
    var_list : list of str, optional
        List of variables to include in output. MELODIES MONET should only read in
        variables needed to plot in order to save on memory and simulation cost
        especially for vertical data. If ``None`` (default), will read in all model data.

    Returns
    -------
    xarray.Dataset
    """
    names, netcdf = _ensure_mfdataset_filenames(fname)
    if not netcdf:
        raise ValueError(
            "File format not supported. Note that files should be "
            "in netCDF format."
            "Do not mix and match file types."
        )
    ds = xr.open_mfdataset(names, concat_dim="time", drop_variables=["theta"], combine="nested")
    if var_list is not None:
        var_list.extend(["lat", "lon", "IDATE", "Times", "psfc", "delp", "pdash", "ttheta"])
        ds = ds[var_list]
    ds = _fix(ds, surf_only=surf_only, convert_to_ppb=convert_to_ppb)

    return ds


def _fix(ds, *, surf_only, convert_to_ppb):
    ds = _fix_grid(ds)
    ds = _fix_time(ds)
    ds = _fix_pres(ds)

    if surf_only:
        # Handle surf_only by selecting the first level and expanding dimensions
        # Make sure all variables that depend on 'z' are handled consistently
        ds = ds.isel(z=0).expand_dims("z")
        # Also handle any coordinate variables that depend on z
        for coord_name in list(ds.coords):
            if "z" in ds[coord_name].dims:
                ds = ds.assign_coords({coord_name: ds[coord_name].isel(z=0).expand_dims("z")})

    if convert_to_ppb:
        for i in ds.variables:
            if "units" in ds[i].attrs:
                if ds[i].attrs["units"] == "ppv":
                    with xr.set_options(keep_attrs=True):
                        ds[i] = ds[i] * 1e9
                    ds[i].attrs["units"] = "ppbv"

    if "ttheta" in ds.keys():
        # Calculate temperature from potential temperature
        k = 0.28571428571428564  # R/cp = kappa (unitless; value for dry air from metpy.constants)
        ds["temperature_k"] = ds["ttheta"] * (ds["pres_pa_mid"] / 100000) ** k
        ds["temperature_k"].attrs["units"] = "K"

    ds = ds.transpose("time", "z", "y", "x")

    return ds


def _fix_grid(ds):
    import xarray as xr
    from numpy import meshgrid

    # Store coordinate values before making changes
    lat_vals = ds.lat.values
    lon_vals = ds.lon.values
    lev_vals = ds.lev.values if "lev" in ds.coords else None

    # Create 2-D lat/lon grid with dims ('y', 'x') and lon in [-180, 180)
    lon_vals_adj = lon_vals.copy()
    lon_vals_adj[(lon_vals_adj >= 180)] -= 360
    lon_2d, lat_2d = meshgrid(lon_vals_adj, lat_vals)

    # Build new coordinates dict
    new_coords = {}

    # Create time coordinate (unchanged)
    new_coords["time"] = ds.coords["time"]

    # Create z coordinate if lev exists
    if lev_vals is not None:
        new_coords["z"] = ("z", lev_vals[::-1])  # Invert here to put surface first

    # Create y and x coordinates
    new_coords["y"] = ("y", lat_vals)
    new_coords["x"] = ("x", lon_vals_adj)

    # Create latitude and longitude 2D coordinates
    new_coords["latitude"] = (
        ("y", "x"),
        lat_2d,
        {
            "long_name": "Latitude",
            "units": "degree_north",
            "standard_name": "latitude",
        },
    )
    new_coords["longitude"] = (
        ("y", "x"),
        lon_2d,
        {
            "long_name": "Longitude",
            "units": "degree_east",
            "standard_name": "longitude",
        },
    )

    # Build new data variables dict
    new_data_vars = {}

    for var_name in ds.data_vars:
        var = ds[var_name]

        # Map old dimension names to new ones
        new_dims = []
        for dim in var.dims:
            if dim == "lev":
                new_dims.append("z")
            elif dim == "lat":
                new_dims.append("y")
            elif dim == "lon":
                new_dims.append("x")
            else:
                new_dims.append(dim)

        # Get the data and reverse z dimension if it exists
        data = var.values
        if "lev" in var.dims:
            z_axis = var.dims.index("lev")
            data = data[
                tuple(
                    slice(None, None, -1) if i == z_axis else slice(None) for i in range(data.ndim)
                )
            ]

        new_data_vars[var_name] = (new_dims, data, var.attrs)

    # Create new dataset
    ds_new = xr.Dataset(new_data_vars, coords=new_coords, attrs=ds.attrs)

    # Add attrs for 'z' if z coordinate exists
    if "z" in ds_new.coords:
        ds_new["z"].attrs.update(
            long_name="Nominal potential temperature of model level",
            units="K",
            description=(
                "In the stratosphere (beginning at lev=492), the model levels are on potential temperature surfaces. "
                "Below lev=492, the model levels are a blend of potential temperature and sigma (terrain-following) coordinates."
            ),
        )

    # Clean up
    del lon_2d, lat_2d, lon_vals_adj

    return ds_new


def _fix_time(ds):
    """Set 'time' coordinate variable based on the date/time strings."""
    import pandas as pd

    dtstr = ds.Times.values.astype(str)

    time = pd.to_datetime(dtstr, format=r"%Y_%m_%d_%H:%M:%S")
    ds["time"] = (("time",), time)

    # These time variables are no longer needed
    ds = ds.drop_vars(["IDATE", "Times"], errors="ignore")

    return ds


def _fix_pres(ds):
    """Rename pressure variables and convert from mb to Pa."""
    rename0 = {
        "psfc": "surfpres_pa",
        "delp": "dp_pa",
        "pdash": "pres_pa_mid",
    }
    rename = {k: v for k, v in rename0.items() if k in ds.variables}

    ds = ds.rename_vars(rename)
    for vn in rename.values():
        assert ds[vn].attrs.get("units", "mb") in {"mb", "hPa"}
        with xr.set_options(keep_attrs=True):
            ds[vn] *= 100
        ds[vn].attrs.update(units="Pa")

    return ds


def _ensure_mfdataset_filenames(fname):
    """Checks if RAQMS netcdf dataset

    Parameters
    ----------
    fname : str or list of str

    Returns
    -------
    list of str
        The file paths.
    bool
        Whether all of files are the expected uwhyb netCDF format.
    """
    from glob import glob
    from os.path import basename

    if isinstance(fname, str):
        fpaths = sorted(glob(fname))
    else:
        fpaths = sorted(fname)

    # Check file name is of the expected format
    good = len(fpaths) > 0 and all(fp.endswith(".nc") and "uwhyb" in basename(fp) for fp in fpaths)

    return fpaths, good
