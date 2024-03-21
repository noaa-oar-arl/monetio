"""MOPITT gridded data file reader.

History:

- updated 2024-02 meb
  * read multiple variables into a DataSet instead of individual variables
  * add functions to combine profiles and surface as in rrb's code
- updated 2023-08 rrb
  * Added units
- updated 2022-10 rrb
  * Dataset instead of DataArray
- created 2021-12 rrb
"""
import glob
from pathlib import Path

import pandas as pd
import xarray as xr


def get_start_time(filename):
    """Method to read the time in MOPITT level 3 HDF files.

    Parameters
    ----------
    filename : str
        Path to the file.

    Returns
    -------
    pandas.Timestamp or pandas.NaT
    """
    import h5py

    structure = "/HDFEOS/ADDITIONAL/FILE_ATTRIBUTES"

    inFile = h5py.File(filename, "r")

    grp = inFile[structure]
    k = grp.attrs
    startTimeBytes = k.get("StartTime", default=None)  # one-element float array
    if startTimeBytes is None:
        startTime = pd.NaT
    else:
        startTime = pd.to_datetime(
            startTimeBytes[0],
            unit="s",
            origin="1993-01-01 00:00:00",
        )

    inFile.close()

    return startTime


def load_variable(filename, varname):
    """Method to open MOPITT gridded HDF files.
    Masks data that is missing (turns into ``np.nan``).

    Parameters
    ----------
    filename
        Path to the file.
    varname : str
        The variable to load from the MOPITT file.

    Returns
    -------
    xarray.Dataset
    """
    import h5py

    ds = xr.Dataset()

    # Load the dimensions
    he5_load = h5py.File(filename, mode="r")
    lat = he5_load["/HDFEOS/GRIDS/MOP03/Data Fields/Latitude"][:]
    lon = he5_load["/HDFEOS/GRIDS/MOP03/Data Fields/Longitude"][:]
    alt = he5_load["/HDFEOS/GRIDS/MOP03/Data Fields/Pressure2"][:]
    alt_short = he5_load["/HDFEOS/GRIDS/MOP03/Data Fields/Pressure"][:]

    # 2D or 3D variables to choose from
    variable_dict = {
        "column": "/HDFEOS/GRIDS/MOP03/Data Fields/RetrievedCOTotalColumnDay",
        "apriori_col": "/HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOTotalColumnDay",
        "apriori_surf": "/HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOSurfaceMixingRatioDay",
        "pressure_surf": "/HDFEOS/GRIDS/MOP03/Data Fields/SurfacePressureDay",
        "ak_col": "/HDFEOS/GRIDS/MOP03/Data Fields/TotalColumnAveragingKernelDay",
        "apriori_prof": "/HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOMixingRatioProfileDay",
    }
    if varname not in variable_dict:
        raise ValueError(f"Variable {varname!r} not in {sorted(variable_dict)}.")
    data_loaded = he5_load[variable_dict[varname]][:]

    he5_load.close()

    # Create xarray DataArray
    if varname == "column":
        ds[varname] = xr.DataArray(
            data_loaded,
            dims=["lon", "lat"],
            coords=[lon, lat],
            attrs={
                "long_name": "Retrieved CO Total Column",
                "units": "molec/cm^2",
            },
        )
    elif (varname=='apriori_col'
        or varname=='apriori_surf'or varname=='pressure_surf'):
        ds[varname] = xr.DataArray(data_loaded, dims=["lon","lat"], coords=[lon,lat])    
    elif varname == "ak_col":
        ds[varname] = xr.DataArray(
            data_loaded,
            dims=["lon", "lat", "alt"],
            coords=[lon, lat, alt],
            attrs={
                "long_name": "Total Column Averaging Kernel",
                "units": "mol/(cm^2 log(VMR))",
            },
        )
    elif varname == "apriori_prof":
        ds[varname] = xr.DataArray(
            data_loaded,
            dims=["lon", "lat", "alt"],
            coords=[lon, lat, alt_short],
            attrs={
                "long_name": "A Priori CO Mixing Ratio Profile",
                "units": "ppbv",
            },
        )
    # missing value -> nan
    ds[varname] = ds[varname].where(ds[varname] != -9999.0)
    return ds

def _add_pressure_variabiles(dataset):
    '''Setup 3-D pressure array. 
    
    Parameters
    ----------
    dataset : xarray.Dataset
        Should have the 3D averaging kernel field and surface pressure field
    Returns
    -------
    xarray.DataSet
    '''
    import numpy as np
    
    # broadcast 10 levels 1000 to 100 hPa repeated everywhere
    dummy, press_dummy_arr = xr.broadcast(dataset['ak_col'],dataset['ak_col'].alt)
    # Replace level with 1000 hPa with the actual surface pressure
    dataset['pressure'] = press_dummy_arr.copy()
    dataset['pressure'][:,:,:,9] = dataset['pressure_surf'].values
    
    #Correct for where MOPITT surface pressure <900 hPa
    ## difference between layer pressure and surface pressure
    diff = xr.full_like(dataset['pressure'],np.nan)
    diff[:,:,:,0] = 1000
    diff[:,:,:,1:] = dataset['pressure_surf'].values[:,:,:,None]-dataset['pressure'][:,:,:,:9].values
    ## add fill values below true surface
    dataset['pressure'] = dataset['pressure'].where(diff > 0)
    ## replace lowest pressure with surface pressure; broadcast happens in background
    dataset['pressure'].values = dataset['pressure_surf'].where((diff > 0) & (diff < 100), dataset['pressure']).values 
    
    # Center Pressure
    dummy = dataset['pressure'].copy()
    dummy[:,:,:,0] = 87.
    for z in range(1,10):
        dummy[:,:,:,z] = dataset['pressure'][:,:,:,z] - (dataset['pressure'][:,:,:,z]-dataset['pressure'][:,:,:,z-1])/2
    dataset['pressure'] = dummy
    
    return dataset

def _combine_apriori(dataset):
    '''MOPITT surface values are stored separately to profile values because
        of the floating surface pressure. So, for the smoothing calculations, 
        need to combine surface and profile

        Parameters
        ----------
        xarray.Dataset
        Returns
        -------
        xarray.Dataset
    '''
    import numpy as np
    
    dataset['apriori_prof'][:,:,:,-1] = dataset['apriori_surf'].values
    
    #As with pressure, correct for where MOPITT surface pressure <900 hPa
    ## difference between layer pressure and surface pressure
    diff = xr.full_like(dataset['pressure'],np.nan)
    diff[:,:,:,0] = 1000
    diff[:,:,:,1:] = dataset['pressure_surf'].values[:,:,:,None]-dataset['pressure'][:,:,:,:9].values
    ## add fill values below true surface
    dataset['apriori_prof'] = dataset['apriori_prof'].where(diff > 0)
    ## replace lowest pressure with surface pressure; broadcast happens in background
    dataset['apriori_prof'].values = dataset['apriori_surf'].where((diff > 0) & (diff < 100), dataset['apriori_prof']).values     
    
    return dataset


def open_dataset(files, varnames):
    """Loop through files to open the MOPITT level 3 data for variable `varname`.

    Parameters
    ----------
    files : str or Path or list
        Input file path(s).
        If :class:`str`, shell-style wildcards (e.g. ``*``) will be expanded.
    varnames : list
        The variables to load from the MOPITT file.

    Returns
    -------
    xarray.Dataset
    """
    if isinstance(files, str):
        filelist = sorted(glob.glob(files, recursive=False))
    elif isinstance(files, Path):
        filelist = [files]
    else:
        filelist = files  # assume list

    datasets = []
    for filename in filelist:
        print(filename)
        file_varset = []
        for varname in varnames:
            data = load_variable(filename, varname)
            time = get_start_time(filename)
            data = data.expand_dims(axis=0, time=[time])
            file_varset.append(data)
            
        data = xr.merge(file_varset) # merge variables for file into single dataset
        if "apriori_prof" in varnames and "pressure_surf" in varnames:
            data = _add_pressure_variabiles(data) # add 3-d pressure field
            data = _combine_apriori(data) # combine suface and rest of profile into single variable
        
        datasets.append(data)
        
    return xr.concat(datasets, dim="time")
