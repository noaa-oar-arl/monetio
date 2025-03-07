"""CESM File Reader"""

import warnings

import numpy as np
import xarray as xr
from numpy import meshgrid


def open_mfdataset(
    fname,
    earth_radius=6370000,
    convert_to_ppb=True,
    var_list=["O3", "PM25"],
    surf_only=True,
    **kwargs,
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
    xarray.Dataset
    """

    if not surf_only:
        warnings.warn(
            "3D data processing is still experimental in CESM-FV (CAM-Chem), "
            + "and has not been properly tested. Use at own risk."
        )

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
    # If vertical information is required, add it.
    if not surf_only:
        if "PMID" not in dset_load.keys():
            dset_load["PMID"] = _calc_pressure(dset_load)
        var_list = var_list + ["PMID"]
        if "Z3" not in dset_load.keys():
            warnings.warn("Geopotential height Z3 is not in model keys. Assuming hydrostatic runs")
            dset_load["Z3"] = _calc_hydrostatic_height(dset_load)

        dset_load.rename(
            {
                "T": "temperature_k",
                "Z3": "alt_msl_m_mid",
                "PS": "surfpres_pa",
                "PMID": "pres_pa_mid",
            }
        )
        # Calc height agl. PHIS is in m2/s2, whereas Z3 is in already in m
        dset_load["alt_agl_m_mid"] = dset_load["alt_msl_m_mid"] - dset_load["PHIS"] / 9.80665
        dset_load["alt_agl_m_mid"].attrs = {
            "description": "geopotential height above ground level",
            "units": "m",
        }
        var_list = var_list + [
            "temperature_k",
            "alt_msl_m_mid",
            "alt_agl_m_mid",
            "surfpres_pa",
            "pres_pa_mid",
        ]

    dset = dset_load.get(var_list)
    # rename altitude dimension to z for monet use
    # also rename lon to x and lat to y
    dset = dset.rename_dims({"lon": "x", "lat": "y", "lev": "z"})

    # convert to -180 to 180 longitude
    lon = wrap_longitudes(dset["lon"])
    lat = dset["lat"]
    lons, lats = meshgrid(lon, lat)
    dset["longitude"] = (("y", "x"), lons)
    dset["latitude"] = (("y", "x"), lats)

    # Set longitude and latitude to be only coordinates
    dset = dset.reset_coords()
    dset = dset.set_coords(["latitude", "longitude"])

    # re-order so surface is associated with the first vertical index
    dset = dset.sortby("z", ascending=False)

    # Get rid of original 1-D lat and lon to avoid future conflicts
    dset = dset.drop_vars(["lat", "lon"])

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


def _calc_pressure(dset):
    """Calculates midlayer pressure using P0, PS, hyam, hybm

    Parameters
    ----------
    dset: xr.Dataset

    Returns
    -------
    xr.DataArray
    """
    presvars = ["PS", "hyam", "hybm"]
    if not all(pvar in list(dset.keys()) for pvar in presvars):
        raise KeyError(
            "The model does not have the variables to calculate"
            + "the pressure. This can be done either with PMID or with"
            + "P0, PS, hyam and hybm.\n"
            + "If the vertical coordinate is not needed, set surface_only=True"
        )
    time = dset["PS"].time.values
    vert = dset["hyam"].lev.values
    lat = dset["PS"].lat.values
    lon = dset["PS"].lon.values
    n_vert = len(vert)
    n_time = len(time)
    n_lat = len(lat)
    n_lon = len(lon)

    pressure = np.zeros((n_time, n_vert, n_lat, n_lon))

    if "P0" not in dset.keys():
        warnings.warn("P0 not in netcdf keys, assuming 100_000 Pa")
        p0 = 100_000
    else:
        p0 = dset["P0"].values

    for nlev in range(n_vert):
        pressure[:, nlev, :, :] = (
            dset["hyam"][nlev].values * p0 + dset["hybm"][nlev].values * dset["PS"].values
        )
    P = xr.DataArray(
        data=pressure,
        dims=["time", "lev", "lat", "lon"],
        coords={"time": time, "lev": vert, "lat": lat, "lon": lon},
        attrs={"description": "Mid layer pressure", "units": "Pa"},
    )
    return P


def _calc_hydrostatic_height(dset):
    """Calculates midlayer height using PMID, P, PS and PHIS, T,

    Parameters
    ----------
    dset: xr.Dataset

    Returns
    -------
    xr.DataArray
    """
    R = 8.314  # Pa * m3 / mol K
    M_AIR = 0.028  # kg / mol
    GRAVITY = 9.80665  # m / s2
    time = dset["PMID"].time.values
    vert = dset["PMID"].lev.values
    lat = dset["PMID"].lat.values
    lon = dset["PMID"].lon.values
    n_vert = len(vert)
    n_time = len(time)
    n_lat = len(lat)
    n_lon = len(lon)

    # Check if the vertical levels go from highest to lowest altitude,
    # which is the default in CESM. That means, that the hybrid
    # pressure levels should be increasing.
    _height_decreasing = np.all(vert[:-1] < vert[1:])
    if not _height_decreasing:
        raise Exception(
            "Expected default CESM behaviour:" + "pressure levels should be in decreasing order"
        )

    height = np.zeros((n_time, n_vert, n_lat, n_lon))
    height[:, n_vert, :, :] = dset["PHIS"].values / GRAVITY
    for nlev in range(n_vert - 1, -1, -1):
        height_b = height[:, nlev + 1, :, :]
        temp_b = dset["T"].isel(lev=nlev + 1).values
        press_b = dset["PMID"].isel(lev=nlev + 1)
        press = dset["PMID"].isel(lev=nlev)
        height[:, nlev, :, :] = height_b - R * temp_b * np.ln(press / press_b) / (GRAVITY * M_AIR)

    z = xr.DataArray(
        data=height,
        dims=["time", "lev", "lat", "lon"],
        coords={"time": time, "lev": vert, "lat": lat, "lon": lon},
        attrs={"description": "Mid layer (hydrostatic) height", "units": "m"},
    )
    return z
