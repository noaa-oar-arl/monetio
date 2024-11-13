""" GOCART File Reader """
import xarray as xr

def open_mfdataset(fnames):
    """Method to open GOCART netcdf files.

    Parameters
    ----------
    fname : string or list
        fname is the path to the file or files.  It will accept hot keys in
        strings as well.

    Returns
    -------
    xarray.DataSet
        GOCART model dataset in standard format for use in MELODIES-MONET

    """

    # open the dataset using xarray
    ds = xr.open_mfdataset(fnames, concat_dim="time", combine="nested", drop_variables=["AOD_BC","AOD_DU","AOD_OC","AOD_SS","AOD_SU"])

    ds["AOD470"] = ds.AOD.isel(lev=0)
    ds["AOD550"] = ds.AOD.isel(lev=1)
    ds["AOD670"] = ds.AOD.isel(lev=2)
    ds["AOD870"] = ds.AOD.isel(lev=3)
    ds = ds.drop_vars("AOD").drop_dims("lev")

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
    ds = ds.reset_coords().set_coords(["time","latitude", "longitude"])
    del lon, lat
    return ds
