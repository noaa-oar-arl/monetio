"""GOCART File Reader."""

import sys

import xarray as xr


def open_mfdataset(fnames, wavelengths=["470", "550", "670", "870"], **kwargs):
    """Method to open GOCART netcdf files.

    Parameters
    ----------
    fnames
        String glob expression or a list of files to open.
    wavelengths
        List of strings to describe wavelengths of AOD output.
        Must have the same length as lev dim size.

    Returns
    -------
    xarray.Dataset
        GOCART model dataset in standard format for use in MELODIES MONET

    """

    ds = xr.open_mfdataset(
        fnames,
        concat_dim="time",
        combine="nested",
    )

    if len(wavelengths) != ds.lev.size:
        print("ERROR: wavelengths list must have the same length as lev dim size")
        sys.exit()

    wv0 = wavelengths[0]
    wv1 = wavelengths[1]
    wv2 = wavelengths[2]
    wv3 = wavelengths[3]

    ds["AOD" + wv0] = ds["AOD"].isel(lev=0)
    ds["AOD" + wv1] = ds["AOD"].isel(lev=1)
    ds["AOD" + wv2] = ds["AOD"].isel(lev=2)
    ds["AOD" + wv3] = ds["AOD"].isel(lev=3)

    ds["AOD_BC" + wv0] = ds["AOD_BC"].isel(lev=0)
    ds["AOD_BC" + wv1] = ds["AOD_BC"].isel(lev=1)
    ds["AOD_BC" + wv2] = ds["AOD_BC"].isel(lev=2)
    ds["AOD_BC" + wv3] = ds["AOD_BC"].isel(lev=3)

    ds["AOD_DU" + wv0] = ds["AOD_DU"].isel(lev=0)
    ds["AOD_DU" + wv1] = ds["AOD_DU"].isel(lev=1)
    ds["AOD_DU" + wv2] = ds["AOD_DU"].isel(lev=2)
    ds["AOD_DU" + wv3] = ds["AOD_DU"].isel(lev=3)

    ds["AOD_OC" + wv0] = ds["AOD_OC"].isel(lev=0)
    ds["AOD_OC" + wv1] = ds["AOD_OC"].isel(lev=1)
    ds["AOD_OC" + wv2] = ds["AOD_OC"].isel(lev=2)
    ds["AOD_OC" + wv3] = ds["AOD_OC"].isel(lev=3)

    ds["AOD_SS" + wv0] = ds["AOD_SS"].isel(lev=0)
    ds["AOD_SS" + wv1] = ds["AOD_SS"].isel(lev=1)
    ds["AOD_SS" + wv2] = ds["AOD_SS"].isel(lev=2)
    ds["AOD_SS" + wv3] = ds["AOD_SS"].isel(lev=3)

    ds["AOD_SU" + wv0] = ds["AOD_SU"].isel(lev=0)
    ds["AOD_SU" + wv1] = ds["AOD_SU"].isel(lev=1)
    ds["AOD_SU" + wv2] = ds["AOD_SU"].isel(lev=2)
    ds["AOD_SU" + wv3] = ds["AOD_SU"].isel(lev=3)

    ds = ds.drop_vars(["AOD", "AOD_BC", "AOD_DU", "AOD_OC", "AOD_SS", "AOD_SU"])
    ds = ds.drop_dims("lev")

    ds = _fix(ds)

    return ds


def _fix(ds):
    ds = _fix_grid(ds)

    ds = ds.expand_dims("z")
    ds = ds.transpose("time", "z", "y", "x")

    return ds


def _fix_grid(ds):
    from numpy import meshgrid

    # Create 2-D lat/lon grid with dims ('y', 'x') and lon in [-180, 180)
    lat = ds.lat.values
    lon = ds.lon.values
    lon, lat = meshgrid(lon, lat)
    ds = ds.rename_dims({"lat": "y", "lon": "x"}).drop_vars(["lat", "lon"])
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
    ds = ds.reset_coords().set_coords(["time", "latitude", "longitude"])
    return ds
