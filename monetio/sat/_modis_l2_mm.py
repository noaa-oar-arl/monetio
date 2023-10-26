import logging
import sys
from collections import OrderedDict
from glob import glob

import numpy as np
import xarray as xr


def read_dataset(fname, variable_dict):
    """
    Parameters
    __________
    fname : str
        Input file path.

    Returns
    _______
    xarray.Dataset
    """
    from monetio.sat.hdfio import hdf_close, hdf_list, hdf_open, hdf_read

    print("reading " + fname)

    ds = xr.Dataset()

    f = hdf_open(fname)
    hdf_list(f)
    latitude = hdf_read(f, "Latitude")  # noqa: F841
    longitude = hdf_read(f, "Longitude")  # noqa: F841
    start_time = hdf_read(f, "Scan_Start_Time")  # noqa: F841
    for varname in variable_dict:
        print(varname)
        values = hdf_read(f, varname)
        if "scale" in variable_dict[varname]:
            values = variable_dict[varname]["scale"] * values
        if "minimum" in variable_dict[varname]:
            minimum = variable_dict[varname]["minimum"]
            values[values < minimum] = np.nan
        if "maximum" in variable_dict[varname]:
            maximum = variable_dict[varname]["maximum"]
            values[values > maximum] = np.nan
        ds[varname] = xr.DataArray(values)
        if "quality_flag" in variable_dict[varname]:
            ds.attrs["quality_flag"] = varname
            ds.attrs["quality_thresh"] = variable_dict[varname]["quality_flag"]
    hdf_close(f)

    return ds


def apply_quality_flag(ds):
    """
    Parameters
    __________
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
    __________
    fnames : str
        Regular expression for input file paths.

    Returns
    _______
    xarray.Dataset
    """
    if debug:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO
    logging.basicConfig(stream=sys.stdout, level=logging_level)

    files = sorted(glob(fnames))

    granules = OrderedDict()

    for file in files:
        granule = read_dataset(file, variable_dict)
        apply_quality_flag(granule)
        granule_str = file.split("/")[-1]
        granule_info = granule_str.split(".")
        datetime_str = granule_info[1][1:] + granule_info[2]
        granules[datetime_str] = granule

    return granules
