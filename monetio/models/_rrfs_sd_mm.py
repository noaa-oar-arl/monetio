""" RRFS-CMAQ File Reader """
import numpy as np
import xarray as xr
from numpy import concatenate
from pandas import Series


def open_mfdataset(
    fname,
    var_list=None,
    surf_only=False,
    **kwargs,
):
    # Like WRF-chem add var list that just determines whether to calculate sums or not to speed this up.
    """Method to open RFFS-SD dyn* netcdf files.

    Parameters
    ----------
    fname : string or list
        fname is the path to the file or files.  It will accept hot keys in
        strings as well.
    surf_only: boolean
        Whether to save only surface data to save on memory and computational
        cost (True) or not (False).

    Returns
    -------
    xarray.DataSet
        RRFS-SD model dataset in standard format for use in MELODIES-MONET

    """

    # Read in all variables and do all calculations.
    dset = xr.open_mfdataset(fname, concat_dim="time", combine="nested", **kwargs)
    
    if 'smoke' in dset.data_vars and 'dust' in dset.data_vars:
        dset['PM25'] = dset.smoke + dset.dust
        dset['PM25'].attrs['long_name'] = 'Particulate Matter < 2.5 microns'
        dset['PM25'].attrs['units'] = "ug/kg"
    if 'smoke' in dset.data_vars and 'dust' in dset.data_vars and 'coarsepm' in dset.data_vars: 
        dset['PM10'] = dset.smoke + dset.dust + dset.coarsepm
        dset['PM10'].attrs['long_name'] = 'Particulate Matter < 10 microns'
        dset['PM10'].attrs['units'] = "ug/kg"

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
    )

    # Calculate pressure. This has to go before sorting because ak and bk
    # are not sorted as they are in attributes
    dset["pres_pa_mid"] = _calc_pressure(dset,surf_only)

    # Adjust pressure levels for all models such that the surface is first.
    dset = dset.sortby("z", ascending=False)
    dset = dset.sortby("z_i", ascending=False)

    # Note this altitude calcs needs to always go after resorting.
    # Altitude calculations are all optional, but for each model add values that are easy to calculate.
    dset["alt_msl_m_full"] = _calc_hgt(dset)
    dset["dz_m"] = dset["dz_m"] * -1.0  # Change to positive values.

    # Set coordinates
    dset = dset.reset_index(
        ["x", "y", "z", "z_i"], drop=True
    )  # For now drop z_i no variables use it.
    dset["latitude"] = dset["latitude"].isel(time=0)
    dset["longitude"] = dset["longitude"].isel(time=0)
    dset = dset.reset_coords()
    dset = dset.set_coords(["latitude", "longitude"])

    # These sums and units are quite expensive and memory intensive,
    # so add option to shrink dataset to just surface when needed
    if surf_only:
        dset = dset.isel(z=0).expand_dims("z", axis=1)

    

    # convert "ug/kg to ug/m3"
    for i in dset.variables:
        if "units" in dset[i].attrs:
            if "ug/kg" in dset[i].attrs["units"]:
                # ug/kg -> ug/m3 using dry air density
                dset[i] = dset[i] * dset["pres_pa_mid"] / dset["temperature_k"] / 287.05535
                dset[i].attrs["units"] = r"$\mu g m^{-3}$"

    
    # Change the times to pandas format
    dset["time"] = dset.indexes["time"].to_datetimeindex(unsafe=True)
    # Turn off warning for now. This is just because the model is in julian time

    return dset

def _calc_hgt(f):
    """Calculates the geopotential height in m from the variables hgtsfc and
    delz. Note: To use this function the delz value needs to go from surface
    to top of atmosphere in vertical. Because we are adding the height of
    each grid box these are really grid top values

    Parameters
    ----------
    f : xarray.Dataset
        RRFS-CMAQ model data

    Returns
    -------
    xr.DataArray
        Geoptential height with attributes.
    """
    sfc = f.surfalt_m.load()
    dz = f.dz_m.load() * -1.0
    # These are negative in RRFS-CMAQ, but you resorted and are adding from the surface,
    # so make them positive.
    dz[:, 0, :, :] = dz[:, 0, :, :] + sfc  # Add the surface altitude to the first model level only
    z = dz.rolling(z=len(f.z), min_periods=1).sum()
    z.name = "alt_msl_m_full"
    z.attrs["long_name"] = "Altitude MSL Full Layer in Meters"
    z.attrs["units"] = "m"
    return z


def _calc_pressure(dset, surf_only=False):
    """Calculate the mid-layer pressure in Pa from surface pressure
    and ak and bk constants.

    Interface pressures are calculated by:
    phalf(k) = a(k) + surfpres * b(k)

    Mid layer pressures are calculated by:
    pfull(k) = (phalf(k+1)-phalf(k))/log(phalf(k+1)/phalf(k))

    Parameters
    ----------
    dset : xarray.Dataset
        RRFS-CMAQ model data

    Returns
    -------
    xarray.DataArray
        Mid-layer pressure with attributes.
    """
    p = dset.dp_pa.copy().load()  # Have to load into memory here so can assign levels.
    srfpres = dset.surfpres_pa.copy().load()
    for k in range(len(dset.z)):
        if surf_only:
           pres_2 = 0.0 + srfpres * 0.9978736
           pres_1 = 0.0 + srfpres * 1.0
        else:
           pres_2 = dset.ak[k + 1] + srfpres * dset.bk[k + 1]
           pres_1 = dset.ak[k] + srfpres * dset.bk[k]
        p[:, k, :, :] = (pres_2 - pres_1) / np.log(pres_2 / pres_1)

    p.name = "pres_pa_mid"
    p.attrs["units"] = "pa"
    p.attrs["long_name"] = "Pressure Mid Layer in Pa"
    return p
