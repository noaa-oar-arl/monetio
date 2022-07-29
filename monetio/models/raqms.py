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

    return ds


def _fix_grid(ds):
    from numpy import meshgrid

    lat = ds.lat.values
    lon = ds.lon.values
    lon[(lon > 180)] -= 360
    lon, lat = meshgrid(lon, lat)
    ds = ds.rename({"lat": "y", "lon": "x", "lev": "z"})
    ds["longitude"] = (("y", "x"), lon)
    ds["latitude"] = (("y", "x"), lat)
    ds = ds.set_coords(["latitude", "longitude"])
    del lon, lat

    return ds


def _fix_time(ds):
    """Set 'time' coordinate variable based on IDATE."""
    import pandas as pd

    dtstr = ds.IDATE.values

    date = pd.to_datetime(dtstr, format=r"%Y%m%d%H")
    ds["time"] = (("time",), date)

    # These time variables are no longer needed
    ds = ds.drop_vars(["IDATE", "Times"], errors="ignore")

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
