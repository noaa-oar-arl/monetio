"""Read NASA Suomi NPP OMPS Level 3 Nadir Mapper TO3 file."""
from pathlib import Path


def open_dataset(files):
    """Open OMPS nadir mapper Total Column Ozone L3 files.

    Parameters
    ----------
    files: str or Path or list
        Input file path(s).
        If :class:`str`, shell-style wildcards (e.g. ``*``) will be expanded.

    Returns
    -------
    xarray.Dataset
    """
    from glob import glob

    import xarray as xr

    if isinstance(files, str):
        filelist = sorted(glob(files, recursive=False))
    elif isinstance(files, Path):
        filelist = [files]
    else:
        filelist = files  # assume list

    times = []
    datasets = []
    for filename in filelist:
        data = _open_one_dataset(filename)
        times.append(data.attrs.pop("time"))
        datasets.append(data)

    ds = xr.concat(datasets, dim="time")
    ds["time"] = (("time"), times)
    ds = ds.reset_coords().set_coords(["latitude", "longitude", "time"])

    return ds


def _open_one_dataset(fname):
    """Read locally stored NASA Suomi NPP OMPS Level 3 Nadir Mapper TO3 file.

    Parameters
    ----------
    fname: str
        Local path to h5 file.

    Returns
    -------
    xarray.Dataset
    """
    import h5py
    import numpy as np
    import pandas as pd
    import xarray as xr

    with h5py.File(fname, "r") as f:
        lat = f["Latitude"][:]
        lon = f["Longitude"][:]
        column = f["ColumnAmountO3"][:]
        cloud_fraction = f["RadiativeCloudFraction"][:]
        time = pd.to_datetime(f.attrs.get("Date").decode("UTF-8"), format=r"%Y-%m-%d")

    # Remove cloudy scenes and points with no data (eg. polar dark zone)
    column[(column < 0)] = np.nan
    column[(cloud_fraction > 0.3)] = np.nan
    lon_2d, lat_2d = np.meshgrid(lon, lat)

    ds = xr.Dataset(
        {
            "ozone_column": (["time", "x", "y"], column[None, :, :]),
        },
        coords={
            "longitude": (["x", "y"], lon_2d),
            "latitude": (["x", "y"], lat_2d),
        },
        attrs={"time": time},
    )

    return ds
