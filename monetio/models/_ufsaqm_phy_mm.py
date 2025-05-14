""" UFS-AQM phy File Reader. Intended as temporary to facilitate AOD eval"""

import xarray as xr
import numpy as np

def open_mfdataset(fname, var_list=['aod']):
    """ Method to open UFS-AQM phy* netcdf files.

    Parameters
    ----------
    fname : string or list
        fname is the path to the file or files. It will accept hot keys in
        strings as well.
    var_list : list
        List of variables to include in output. 

    Returns
    -------
    xarray.Dataset
        UFS-AQM dataset in standard format for use in MELODIES MONET
    """
    # Add latitutude and longitude to the needed species list
    var_list.extend(["lat",'lon'])

    # open dataset using xarray
    dset = xr.open_mfdataset(fname, concat_dim='time', combine='nested')[var_list]

    # standardize dimension names
    dset = dset.rename(
            {
                "grid_yt": "y",
                "grid_xt": "x",
                "lon": "longitude",
                "lat": "latitude",            
            }
    )
    
    # set coordinates
    dset["latitude"] = dset["latitude"].isel(time=0)
    dset["longitude"] = dset["longitude"].isel(time=0)
    dset = dset.reset_coords()
    dset = dset.set_coords(["latitude", "longitude"])

    # Change the times to pandas format
    dset["time"] = dset.indexes["time"].to_datetimeindex(unsafe=True)

    return dset
