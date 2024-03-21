"""Read TROPOMI L2 NO2 data."""

import logging
import os
import sys
from collections import OrderedDict
from glob import glob

import numpy as np
import xarray as xr
from netCDF4 import Dataset


def _open_one_dataset(fname, variable_dict):
    """
    Parameters
    ----------
    fname : str
        Input file path.
    variable_dict : dict

    Returns
    -------
    xarray.Dataset
    """
    print("reading " + fname)

    ds = xr.Dataset()

    dso = Dataset(fname, "r")

    longitude = dso.groups["PRODUCT"]["longitude"]
    latitude = dso.groups["PRODUCT"]["latitude"]
    start_time = dso.groups["PRODUCT"]["time"]

    # squeeze 1-dimension
    longitude = np.squeeze(longitude)
    latitude = np.squeeze(latitude)
    start_time = np.squeeze(start_time)

    ds["lon"] = xr.DataArray(longitude)
    ds["lat"] = xr.DataArray(latitude)

    for varname in variable_dict:
        print(varname)
        values = dso.groups["PRODUCT"][varname]
        # squeeze out 1-dimension
        values = np.squeeze(values)

        if "fillvalue" in variable_dict[varname]:
            fillvalue = variable_dict[varname]["fillvalue"]
            values[:][values[:] == fillvalue] = np.nan

        if "scale" in variable_dict[varname]:
            values[:] = variable_dict[varname]["scale"] * values[:]

        if "minimum" in variable_dict[varname]:
            minimum = variable_dict[varname]["minimum"]
            values[:][values[:] < minimum] = np.nan

        if "maximum" in variable_dict[varname]:
            maximum = variable_dict[varname]["maximum"]
            values[:][values[:] > maximum] = np.nan

        ds[varname] = xr.DataArray(values)

        if "quality_flag_min" in variable_dict[varname]:
            ds.attrs["quality_flag"] = varname
            ds.attrs["quality_thresh_min"] = variable_dict[varname]["quality_flag_min"]

    dso.close()

    return ds


def apply_quality_flag(ds):
    """Mask variables in place based on quality flag.

    Parameters
    ----------
    ds : xarray.Dataset
    """
    if "quality_flag" in ds.attrs:
        quality_flag = ds[ds.attrs["quality_flag"]]
        quality_thresh_min = ds.attrs["quality_thresh_min"]

        # Apply the quality thresh minimum to all variables in ds
        for varname in ds:
            print(varname)
            if varname != ds.attrs["quality_flag"]:
                logging.debug(varname)
                values = ds[varname].values
                values[quality_flag <= quality_thresh_min] = np.nan


def open_dataset(fnames, variable_dict, debug=False):
    """
    Parameters
    ----------
    fnames : str
        Glob expression for input file paths.
    variable_dict : dict
    debug : bool
        Set logging level to debug.

    Returns
    -------
    xarray.Dataset
    """
    if debug:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO
    logging.basicConfig(stream=sys.stdout, level=logging_level)

    for subpath in fnames.split("/"):
        if "$" in subpath:
            envvar = subpath.replace("$", "")
            envval = os.getenv(envvar)
            if envval is None:
                print("environment variable not defined: " + subpath)
                exit(1)
            else:
                fnames = fnames.replace(subpath, envval)

    print(fnames)
    files = sorted(glob(fnames))
    granules = OrderedDict()
    for file in files:
        granule = _open_one_dataset(file, variable_dict)
        apply_quality_flag(granule)
        granule_str = file.split("/")[-1]
        granule_info = granule_str.split("____")

        datetime_str = granule_info[1][0:8]
        granules[datetime_str] = granule

    return granules
