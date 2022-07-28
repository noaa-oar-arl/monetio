# Reader for RAQMS real-time files.

import xarray as xr


def open_dataset(fname):
    """Open a single dataset from RAQMS output. Currently set for netcdf file format.
    Parameters
    ----------
    fname : string
        Filename to be opened.
    Returns
    -------
    xarray.Dataset
    """
    names, netcdf = _ensure_mfdataset_filenames(fname)
    try:
        if netcdf:
            f = xr.open_dataset(names[0], drop_variables=["theta"])
            f = _fix_grid(f)
            f = _fix_time(f)
        else:
            raise ValueError
    except ValueError:
        print("File format not recognized. Note that files should be preprocessed to netcdf.")
    # if 'latitude' not in f.coords: print('coordinate issue')
    return f


def open_mfdataset(fname):
    """Open a multiple file dataset from RAQMS output.
    Parameters
    ----------
    fname : string
        Filenames to be opened
    Returns
    -------
    xarray.Dataset
    """
    names, netcdf = _ensure_mfdataset_filenames(fname)
    try:
        if netcdf:
            f = xr.open_mfdataset(
                names, concat_dim="time", drop_variables=["theta"], combine="nested"
            )
            f = _fix_grid(f)
            f = _fix_time(f)
        else:
            raise ValueError
    except ValueError:
        print(
            "File format not recognized. Note that files should be "
            "in netcdf format."
            "Do not mix and match file types."
        )

    return f


def _fix_grid(f):
    from numpy import meshgrid

    lat = f.lat.values
    lon = f.lon.values
    lon[(lon > 180)] -= 360
    lon, lat = meshgrid(lon, lat)
    f = f.rename({"lat": "y", "lon": "x", "lev": "z"})
    f["longitude"] = (("y", "x"), lon)
    f["latitude"] = (("y", "x"), lat)
    f = f.set_coords(["latitude", "longitude"])
    del lon, lat
    return f


def _fix_time(f):
    import pandas as pd

    dtstr = f.IDATE.values

    date = pd.to_datetime(dtstr, format="%Y%m%d%H")
    f["Time"] = (("time",), date)

    f = f.set_index({"time": "Time"})
    del date, dtstr
    return f


def _ensure_mfdataset_filenames(fname):
    """Checks if RAQMS netcdf dataset
    Parameters
    ----------
    fname : string or list of strings
    Returns
    -------
    type
    """
    from glob import glob

    from numpy import sort

    if isinstance(fname, str):
        names = sort(glob(fname))
    else:
        names = sort(fname)
    netcdfs = [True for i in names if "nc" in i if "uwhyb" in i]
    netcdf = False
    if len(netcdfs) >= 1:
        netcdf = True
    return names, netcdf
