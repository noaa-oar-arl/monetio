""" 
UFS-Chem v1 File Reader. 
Modified from RRFS-CMAQ File Reader 

6/2025: Additional modifications to optionally read sfc files
"""

import numpy as np
import xarray as xr
from numpy import concatenate
from pandas import Series


def can_do(index):
    if index.max():
        return True
    else:
        return False


def open_mfdataset(
    fname,
    convert_to_ppb=True,
    var_list=None,
    surf_only=False,
    fname_sfc=None,
    sfc_varlist=['aod550'],
    **kwargs,
):
    """Method to open UFS-Chem v1 netcdf files.

    Parameters
    ----------
    fname : string or list
        fname is the path to the file or files.  It will accept hot keys in
        strings as well.
    convert_to_ppb : boolean
        If true the units of the gas species will be converted to ppbv
    var_list: list
        List of variables to include in output. MELODIES-MONET only reads in
        variables need to plot in order to save on memory and simulation cost
        especially for vertical data. If None, will read in all model data and
        calculate all sums.
    surf_only: boolean
        Whether to save only surface data to save on memory and computational
        cost (True) or not (False).
    fname_sfc : string or list
        Path to the sfc file in UFS-Chem. This file contains additonal variables 
        and diagnostics not included in the standard atm files.
    sfc_varlist : list
        List of variables from the sfc file to include in output.

    Returns
    -------
    xarray.DataSet
        UFS-Chem v1 model dataset in standard format for use in MELODIES-MONET

    """

    if var_list is not None:
        # Read in only a subset of variables and only do calculations if needed.
        var_list_orig = var_list.copy()  # Keep track of the original list before changes.
        list_remove_extra = []  # list of variables to remove after the sum to save in memory.
        # append the other needed species.
        var_list.append("lat")
        var_list.append("lon")
        var_list.append("phalf")
        var_list.append("tmp")
        var_list.append("pressfc")
        var_list.append("dpres")
        var_list.append("hgtsfc")
        var_list.append("delz")

        # Remove duplicates just in case:
        var_list = list(dict.fromkeys(var_list))
        list_remove_extra = list(dict.fromkeys(list_remove_extra))
        # Select only those elements in list_remove_extra that are not in var_list_orig
        list_remove_extra_only = list(set(list_remove_extra) - set(var_list_orig))

        # open the dataset using xarray
        dset = xr.open_mfdataset(fname, concat_dim="time", combine="nested", **kwargs)[var_list]
    else:
        # Read in all variables and do all calculations.
        dset = xr.open_mfdataset(fname, concat_dim="time", combine="nested", **kwargs)

    # Standardize some variable names
    dset = dset.rename(
        {
            "grid_yt": "y",
            "grid_xt": "x",
            "pfull": "z",
            "phalf": "z_i",  # Interface pressure levels
            "lon": "longitude",
            "lat": "latitude",
            "tmp": "temperature_k",  # standard temperature (kelvin)
            "pressfc": "surfpres_pa",
            "dpres": "dp_pa",  # Change names so standard surfpres_pa and dp_pa
            "hgtsfc": "surfalt_m",
            "delz": "dz_m",
        }
    )  # Optional, but when available include altitude info

    # Calculate pressure. This has to go before sorting because ak and bk
    # are not sorted as they are in attributes
    dset["pres_pa_mid"] = _calc_pressure(dset)

    # Adjust pressure levels for all models such that the surface is first.
    if np.all(np.diff(dset.z.values) > 0):  # increasing pressure
        dset = dset.isel(z=slice(None, None, -1))  # -> decreasing
    if np.all(np.diff(dset.z_i.values) > 0):  # increasing pressure
        dset = dset.isel(z_i=slice(None, None, -1))  # -> decreasing
    dset["dz_m"] = dset["dz_m"] * -1.0  # Change to positive values.

    # Note this altitude calcs needs to always go after resorting.
    # Altitude calculations are all optional, but for each model add values that are easy to calculate.
    if not surf_only:
        dset["alt_msl_m_full"] = _calc_hgt(dset)

    # Set coordinates
    dset = dset.reset_index(
        ["x", "y", "z", "z_i"], drop=True
    )  # For now drop z_i no variables use it.
    dset["latitude"] = dset["latitude"].isel(time=0)
    dset["longitude"] = dset["longitude"].isel(time=0)
    
    # modify longitude from 0-360 to -180-180
    dset['longitude'] = xr.where(dset['longitude'] >= 180, dset['longitude'] - 360, dset['longitude'])

    dset = dset.reset_coords()
    dset = dset.set_coords(["latitude", "longitude"])

    # These sums and units are quite expensive and memory intensive,
    # so add option to shrink dataset to just surface when needed
    if surf_only:
        dset = dset.isel(z=0).expand_dims("z", axis=1)

    # Need to adjust units before summing for aerosols
    # convert all gas species to ppbv
    if convert_to_ppb:
        for i in dset.variables:
            if "units" in dset[i].attrs:
                if "ppm" in dset[i].attrs["units"]:
                    dset[i] *= 1000.0
                    dset[i].attrs["units"] = "ppbv"

    # convert "ug/kg to ug/m3"
    for i in dset.variables:
        if "units" in dset[i].attrs:
            if "ug/kg" in dset[i].attrs["units"]:
                # ug/kg -> ug/m3 using dry air density
                dset[i] = dset[i] * dset["pres_pa_mid"] / dset["temperature_k"] / 287.05535
                dset[i].attrs["units"] = r"$\mu g m^{-3}$"


    # Drop extra variables that were part of sum, but are not in original var_list
    # to save memory and computational time.
    # This is only revevant if var_list is provided
    if var_list is not None:
        if bool(list_remove_extra_only):  # confirm list not empty
            dset = dset.drop_vars(list_remove_extra_only)
    
    # Read in additional variables from the sfc file
    if fname_sfc is not None:
        ds_sfc = xr.open_mfdataset(fname_sfc, **kwargs)[sfc_varlist]
        ds_sfc = ds_sfc.rename({"grid_yt": "y",
            "grid_xt": "x"})
        dset = dset.merge(ds_sfc)
    return dset

def _calc_hgt(f):
    """Calculates the geopotential height in m from the variables hgtsfc and
    delz. Note: To use this function the delz value needs to go from surface
    to top of atmosphere in vertical. Because we are adding the height of
    each grid box these are really grid top values

    Parameters
    ----------
    f : xarray.Dataset
        UFS-Chem v1 model data

    Returns
    -------
    xr.DataArray
        Geoptential height with attributes.
    """
    sfc = f.surfalt_m.load()
    dz = f.dz_m.load() * -1.0
    # These are negative in UFS-Chem v1, but you resorted and are adding from the surface,
    # so make them positive.
    dz[:, 0, :, :] = dz[:, 0, :, :] + sfc  # Add the surface altitude to the first model level only
    z = dz.rolling(z=len(f.z), min_periods=1).sum()
    z.name = "alt_msl_m_full"
    z.attrs["long_name"] = "Altitude MSL Full Layer in Meters"
    z.attrs["units"] = "m"
    return z


def _calc_pressure(dset):
    """Calculate the mid-layer pressure in Pa from surface pressure
    and ak and bk constants.

    Interface pressures are calculated by:
    phalf(k) = a(k) + surfpres * b(k)

    Mid layer pressures are calculated by:
    pfull(k) = (phalf(k+1)-phalf(k))/log(phalf(k+1)/phalf(k))

    Parameters
    ----------
    dset : xarray.Dataset
        UFS-Chem v1 model data

    Returns
    -------
    xarray.DataArray
        Mid-layer pressure with attributes.
    """
    p = dset.dp_pa.copy().load()  # Have to load into memory here so can assign levels.
    psfc = dset.surfpres_pa.copy().load()
    for k in range(len(dset.z)):
        pres_2 = dset.ak[k + 1] + psfc * dset.bk[k + 1]
        pres_1 = dset.ak[k] + psfc * dset.bk[k]
        p[:, k, :, :] = (pres_2 - pres_1) / np.log(pres_2 / pres_1)

    p.name = "pres_pa_mid"
    p.attrs["units"] = "pa"
    p.attrs["long_name"] = "Pressure Mid Layer in Pa"
    return p
