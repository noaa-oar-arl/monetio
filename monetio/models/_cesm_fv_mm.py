""" CESM File Reader """
import xarray as xr
from numpy import meshgrid


def open_mfdataset(
    fname, earth_radius=6370000, convert_to_ppb=True, var_list=["O3", "PM25"], **kwargs
):
    """Method to open multiple (or single) CESM netcdf files.
       This method extends the xarray.open_mfdataset functionality
       It is the main method called by the driver. Other functions defined
       in this file are internally called by open_mfdataset and are proceeded
       by an underscore (e.g. _get_latlon).

    Parameters
    ----------
    fname : string or list
        fname is the path to the file or files.  It will accept wildcards in
        strings as well.
    earth_radius : float
        The earth radius used for map projections
    convert_to_ppb : boolean
        If true the units of the gas species will be converted to ppbv
        and units of aerosols to ug m^-3
    var_list : string or list
        List of variables to load from the CESM file. Default is to load ozone (O3) and PM2.5 (PM25).


    Returns
    -------
    xarray.DataSet


    """
    from pyresample.utils import wrap_longitudes

    # check that the files are netcdf format
    names, netcdf = _ensure_mfdataset_filenames(fname)

    # open the dataset using xarray
    try:
        if netcdf:
            dset_load = xr.open_mfdataset(fname, combine="nested", concat_dim="time", **kwargs)
        else:
            raise ValueError
    except ValueError:
        print(
            """File format not recognized. Note that files should be in netcdf
                format. Do not mix and match file types."""
        )

    #############################
    # Process the loaded data
    # extract variables of choice
    dset = dset_load.get(var_list)
    # rename altitude dimension to z for monet use
    # also rename lon to x and lat to y
    dset = dset.rename_dims({"lon": "x", "lat": "y", "lev": "z"})

    # convert to -180 to 180 longitude
    lon = wrap_longitudes(dset["lon"])
    lat = dset["lat"]
    lons, lats = meshgrid(lon, lat)
    dset.coords["longitude"] = (("y", "x"), lons)
    dset.coords["latitude"] = (("y", "x"), lats)

    # Set longitude and latitude to be only coordinates
    dset = dset.reset_coords()
    dset = dset.set_coords(["latitude", "longitude"])

    # re-order so surface is associated with the first vertical index
    dset = dset.sortby("z", ascending=False)

    #############################

    # convert units
    if convert_to_ppb:
        for i in dset.variables:
            if "units" in dset[i].attrs:
                # convert all gas species from mol/mol to ppbv
                if "mol/mol" in dset[i].attrs["units"]:
                    dset[i] *= 1e09
                    dset[i].attrs["units"] = "ppbv"
                # convert "kg/m3 to \mu g/m3 "
                elif "kg/m3" in dset[i].attrs["units"]:
                    dset[i] *= 1e09
                    dset[i].attrs["units"] = r"$\mu g m^{-3}$"

    return dset


# -----------------------------------------
# Below are internal functions to this file
# -----------------------------------------


def _ensure_mfdataset_filenames(fname):
    """Checks if dataset in netcdf format
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
    netcdfs = [True for i in names if "nc" in i]
    netcdf = False
    if len(netcdfs) >= 1:
        netcdf = True
    return names, netcdf
