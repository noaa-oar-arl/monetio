import logging
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from glob import glob

import numpy as np
import xarray as xr


def read_dataset(fname, variable_dict):
    """
    Parameters
    ----------
    fname : str
        Input file path.

    Returns
    -------
    xarray.Dataset
    """
    print("reading " + fname)

    ds_subset = xr.Dataset()

    ds = xr.open_dataset(fname)
    # print(ds)

    for varname in variable_dict:
        print(varname)
        values = ds[varname].values
        if "scale" in variable_dict[varname]:
            values = variable_dict[varname]["scale"] * values
        if "minimum" in variable_dict[varname]:
            minimum = variable_dict[varname]["minimum"]
            values[values < minimum] = np.nan
        if "maximum" in variable_dict[varname]:
            maximum = variable_dict[varname]["maximum"]
            values[values > maximum] = np.nan
        ds_subset[varname] = xr.DataArray(values)
        if "quality_flag" in variable_dict[varname]:
            ds_subset.attrs["quality_flag"] = varname
            ds_subset.attrs["quality_thresh"] = variable_dict[varname]["quality_flag"]

    return ds_subset


def apply_quality_flag(ds):
    """
    Parameters
    ----------
    ds : xarray.Dataset
    """
    if "quality_flag" in ds.attrs:
        quality_flag = ds[ds.attrs["quality_flag"]]
        quality_thresh = ds.attrs["quality_thresh"]
        for varname in ds:
            if varname != ds.attrs["quality_flag"]:
                logging.debug(varname)
                values = ds[varname].values
                values[quality_flag >= quality_thresh] = np.nan
                ds[varname].values = values


def read_mfdataset(fnames, variable_dict, debug=False):
    """
    Parameters
    ----------
    fnames : str
        Regular expression for input file paths.

    Returns
    -------
    xarray.Dataset
    """
    if debug:
        logging_level = logging.DEBUG
        logging.basicConfig(stream=sys.stdout, level=logging_level)

    if isinstance(fnames, str):
        files = sorted(glob(fnames))
    else:
        files = fnames

    granules = OrderedDict()

    for file in files:
        granule = read_dataset(file, variable_dict)
        apply_quality_flag(granule)
        granule_str = file.split("/")[-1]
        granule_info = granule_str.split(".")
        datetime_str = granule_info[1][1:] + granule_info[2]
        granules[datetime_str] = granule

    return granules
