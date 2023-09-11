"""MOPITT gridded data file reader.

History:

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
        "ak_prof": "/HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOMixingRatioProfileDay",
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
        # missing value -> nan
        ds[varname] = ds[varname].where(ds[varname] != -9999.0)
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

    return ds


def open_dataset(files, varname):
    """Loop through files to open the MOPITT level 3 data for variable `varname`.

    Parameters
    ----------
    files : str or Path or list
        Input file path(s).
        If :class:`str`, shell-style wildcards (e.g. ``*``) will be expanded.
    varname : str
        The variable to load from the MOPITT file.

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
        data = load_variable(filename, varname)
        time = get_start_time(filename)
        data = data.expand_dims(axis=0, time=[time])
        datasets.append(data)

    return xr.concat(datasets, dim="time")
