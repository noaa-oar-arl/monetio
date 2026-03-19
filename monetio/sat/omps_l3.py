"""Read NASA Suomi NPP OMPS Level 3 Nadir Mapper TO3 file."""


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
    from pathlib import Path

    import xarray as xr

    if isinstance(files, str):
        filelist = sorted(glob(files, recursive=False))
    elif isinstance(files, Path):
        filelist = [files]
    else:
        filelist = files  # assume list

    datasets = []
    for filename in filelist:
        ds = _open_one_dataset(filename)
        datasets.append(ds)

    return xr.concat(datasets, dim="time")


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
        # Info: https://snpp-omps.gesdisc.eosdis.nasa.gov/data/SNPP_OMPS_Level3/OMPS_NPP_NMTO3_L3_DAILY.2/doc/README.OMPS_NPP_NMTO3_L3_DAILY.2.pdf
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
            "ozone_column": (
                ("y", "x"),
                column,
                {"long_name": "total column ozone amount", "units": "DU"},
            ),
        },
        coords={
            "longitude": (("y", "x"), lon_2d, {"long_name": "longitude", "units": "degree_east"}),
            "latitude": (("y", "x"), lat_2d, {"long_name": "latitude", "units": "degree_north"}),
            "time": ((), time),
        },
    )

    return ds
