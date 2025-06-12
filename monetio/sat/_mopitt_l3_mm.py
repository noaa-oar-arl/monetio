"""MOPITT gridded data file reader.

History:
- updated 2025-06-12
  * Altered to allow for directly accessing from OPeNDAP server (https://opendap.larc.nasa.gov/opendap/MOPITT/MOP03J.009/contents.html)
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
import numpy as np
import h5py
import warnings


def get_start_time(filename,from_opendap=False):
    """Method to read the time in MOPITT level 3 HDF files.

    Parameters
    ----------
    filename : str
        Path to the file.
    from_opendap : bool
        Flag specifying if data is local or being read from the OPeNDAP

    Returns
    -------
    pandas.Timestamp or pandas.NaT
    """
    
    if from_opendap:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inFile = xr.open_dataset(filename)
        structure =  'HDFEOS_ADDITIONAL_FILE_ATTRIBUTES'
        startTime_varname = f'{structure}.StartTime'
        k = inFile.attrs
        startTimeBytes = k.get(startTime_varname,None)
    else:
        inFile = h5py.File(filename, "r")
        structure = "/HDFEOS/ADDITIONAL/FILE_ATTRIBUTES"    
        startTime_varname = 'StartTime'
        k = inFile[structure].attrs
        startTimeBytes = k.get(startTime_varname, default=None)[0]# one-element float array
    
    if startTimeBytes is None:
        startTime = pd.NaT
    else:
        startTime = pd.to_datetime(
            startTimeBytes,
            unit="s",
            origin="1993-01-01 00:00:00",
        )

    inFile.close()

    return startTime


def load_variable_opendap(filename, varname):
    """Method to open MOPITT gridded files from OPeNDAP. Reading either directly from the OPeNDAP database 
    or using netCDF files that originated from reading directly from OPeNDAP drops the grouped structure.
    Masks data that is missing (turns into ``np.nan``).

    Parameters
    ----------
    filename
        Path to the file. May be a url
    varname : str
        The variable to load from the MOPITT file.

    Returns
    -------
    xarray.Dataset
    """
    # Load the dimensions
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ds = xr.open_dataset(filename)
    lat = ds["Latitude"].values
    lon = ds["Longitude"].values
    alt = ds["Pressure2"].values
    alt_short = ds["Pressure"].values

    # 2D or 3D variables to choose from
    variable_dict = {
        "column": "RetrievedCOTotalColumnDay",
        "apriori_col": "APrioriCOTotalColumnDay",
        "apriori_surf": "APrioriCOSurfaceMixingRatioDay",
        "pressure_surf": "SurfacePressureDay",
        "ak_col": "TotalColumnAveragingKernelDay",
        "apriori_prof": "APrioriCOMixingRatioProfileDay",
    }
    if varname not in variable_dict:
        raise ValueError(f"Variable {varname!r} not in {sorted(variable_dict)}.")
    data_loaded = ds[[variable_dict[varname]]]

    ds.close()

    # Create xarray DataArray
    if varname in {"column", "apriori_col", "apriori_surf", "pressure_surf"}:
        data_loaded = data_loaded.rename({'XDim': 'lon','YDim': 'lat',
                                          variable_dict[varname]: varname})
    elif varname == "ak_col":
        data_loaded = data_loaded.rename({'XDim': 'lon','YDim': 'lat', 'Prs2': 'alt',
                                          variable_dict[varname]: varname})
    elif varname == "apriori_prof":
        data_loaded = data_loaded.rename({'XDim': 'lon','YDim': 'lat', 'Prs': 'alt',
                                          variable_dict[varname]: varname})
    else:
        raise AssertionError(f"Variable {varname!r} in variable dict but not accounted for.")

    # missing value -> nan
    data_loaded[varname] = data_loaded[varname].where(data_loaded[varname] != -9999.0)

    return data_loaded
    
def load_variable_local_h5_files(filename, varname):
    """Method to open MOPITT gridded HDF files saved locally after being downloaded from a database using wget.
    These files retain the h5 group structure. 
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
    elif varname in {"apriori_col", "apriori_surf", "pressure_surf"}:
        ds[varname] = xr.DataArray(data_loaded, dims=["lon", "lat"], coords=[lon, lat])
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
    else:
        raise AssertionError(f"Variable {varname!r} in variable dict but not accounted for.")

    # missing value -> nan
    ds[varname] = ds[varname].where(ds[varname] != -9999.0)

    return ds


def _add_pressure_variables(dataset):
    """Setup 3-D pressure array.

    Parameters
    ----------
    dataset : xarray.Dataset
        Should have the 3D averaging kernel field and surface pressure field
    Returns
    -------
    xarray.DataSet
    """

    # broadcast 10 levels 1000 to 100 hPa repeated everywhere
    dummy, press_dummy_arr = xr.broadcast(dataset["ak_col"], dataset["ak_col"].alt)
    # Replace level with 1000 hPa with the actual surface pressure
    dataset["pressure"] = press_dummy_arr.copy()
    dataset["pressure"][:, :, :, 9] = dataset["pressure_surf"].values

    # Correct for where MOPITT surface pressure <900 hPa
    # difference between layer pressure and surface pressure
    diff = xr.full_like(dataset["pressure"], np.nan)
    diff[:, :, :, 0] = 1000
    diff[:, :, :, 1:] = (
        dataset["pressure_surf"].values[:, :, :, None] - dataset["pressure"][:, :, :, :9].values
    )
    # add fill values below true surface
    dataset["pressure"] = dataset["pressure"].where(diff > 0)
    # replace lowest pressure with surface pressure; broadcast happens in background
    dataset["pressure"].values = (
        dataset["pressure_surf"].where((diff > 0) & (diff < 100), dataset["pressure"]).values
    )

    # Center Pressure
    dummy = dataset["pressure"].copy()
    dummy[:, :, :, 0] = 87.0
    for z in range(1, 10):
        dummy[:, :, :, z] = (
            dataset["pressure"][:, :, :, z]
            - (dataset["pressure"][:, :, :, z] - dataset["pressure"][:, :, :, z - 1]) / 2
        )
    dataset["pressure"] = dummy

    return dataset


def _combine_apriori(dataset):
    """MOPITT surface values are stored separately to profile values because
    of the floating surface pressure. So, for the smoothing calculations,
    need to combine surface and profile

    Parameters
    ----------
    xarray.Dataset
    Returns
    -------
    xarray.Dataset
    """

    dataset["apriori_prof"][:, :, :, -1] = dataset["apriori_surf"].values

    # As with pressure, correct for where MOPITT surface pressure <900 hPa
    # difference between layer pressure and surface pressure
    diff = xr.full_like(dataset["pressure"], np.nan)
    diff[:, :, :, 0] = 1000
    diff[:, :, :, 1:] = (
        dataset["pressure_surf"].values[:, :, :, None] - dataset["pressure"][:, :, :, :9].values
    )
    # add fill values below true surface
    dataset["apriori_prof"] = dataset["apriori_prof"].where(diff > 0)
    # replace lowest pressure with surface pressure; broadcast happens in background
    dataset["apriori_prof"].values = (
        dataset["apriori_surf"].where((diff > 0) & (diff < 100), dataset["apriori_prof"]).values
    )

    return dataset


def open_dataset(files, varnames,from_opendap=False):
    """Loop through files to open the MOPITT level 3 data for variable `varname`.

    Parameters
    ----------
    files : str or Path or list
        Input file path(s).
        If :class:`str`, shell-style wildcards (e.g. ``*``) will be expanded.
    varnames : str or list of str
        The variable(s) to load from the MOPITT file.
    from_opendap : bool
        Flag specifying if data is local or being read from the OPeNDAP

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

    if isinstance(varnames, str):
        varnames = [varnames]

    datasets = []
    for filename in filelist:
        print(filename)
        file_varset = []
        for varname in varnames:
            if from_opendap:
                data = load_variable_opendap(filename, varname)
            else:
                data = load_variable_local_h5_files(filename, varname)
            
            time = get_start_time(filename,from_opendap=from_opendap)
            data = data.expand_dims(axis=0, time=[time])
            file_varset.append(data)

        # merge variables for file into single dataset
        data = xr.merge(file_varset)
        if "apriori_prof" in varnames and "pressure_surf" in varnames:
            # add 3-d pressure field
            data = _add_pressure_variables(data)
            # combine surface and rest of profile into single variable
            data = _combine_apriori(data)

        datasets.append(data)

    return xr.concat(datasets, dim="time")
