"""
Reader for RAQMS real-time files.

RAQMS: Realtime Air Quality Monitoring System

More information: http://raqms-ops.ssec.wisc.edu/
"""
import xarray as xr


def open_dataset(fname):
    """Open a single dataset from RAQMS output. Currently expects netCDF file format.

    Parameters
    ----------
    fname : str
        File to be opened.

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
    ds = _fix_grid(ds)
    ds = _fix_time(ds)
    ds = _fix_pres(ds)

    return ds


def open_mfdataset(fname):
    """Open a multiple file dataset from RAQMS output.

    Parameters
    ----------
    fname : str or list of str
        Files to be opened, expressed as a glob string or list of string paths.

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
    ds = _fix_grid(ds)
    ds = _fix_time(ds)
    ds = _fix_pres(ds)

    return ds


def _fix_grid(ds):
    from numpy import meshgrid

    # Create 2-D lat/lon grid with dims ('y', 'x') and lon in [-180, 180)
    lat = ds.lat.values
    lon = ds.lon.values
    lon[(lon >= 180)] -= 360
    lon, lat = meshgrid(lon, lat)
    ds = ds.rename_dims({"lat": "y", "lon": "x", "lev": "z"}).drop_vars(["lat", "lon"])
    ds["longitude"] = (
        ("y", "x"),
        lon,
        {
            "long_name": "Longitude",
            "units": "degree_east",
            "standard_name": "longitude",
        },
    )
    ds["latitude"] = (
        ("y", "x"),
        lat,
        {
            "long_name": "Latitude",
            "units": "degree_north",
            "standard_name": "latitude",
        },
    )
    ds = ds.reset_coords().set_coords(["latitude", "longitude"])
    del lon, lat

    # Add attrs for 'lev'
    # The 'lev' values are nominal and should be the same among files
    ds["lev"].attrs.update(
        long_name="Nominal potential temperature of model level",
        units="K",
        description=(
            "In the stratosphere (beginning at lev=492), the model levels are on potential temperature surfaces. "
            "Below lev=492, the model levels are a blend of potential temperature and sigma (terrain-following) coordinates."
        ),
    )

    # Invert in z so that index 0 is closest to surface
    # https://github.com/pydata/xarray/discussions/6695
    ds = ds.isel(z=slice(None, None, -1))

    return ds


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
