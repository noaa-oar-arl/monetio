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
    from monetio.sat.hdfio import hdf_close, hdf_list, hdf_open, hdf_read

    epoch_1993 = int(datetime(1993, 1, 1, tzinfo=timezone.utc).timestamp())

    print("reading " + fname)

    ds = xr.Dataset()

    f = hdf_open(fname)
    hdf_list(f)
    # Geolocation and Time Parameters
    latitude = hdf_read(f, "Latitude")
    longitude = hdf_read(f, "Longitude")
    # convert seconds since 1993 to since 1970
    start_time = hdf_read(f, "Scan_Start_Time") + epoch_1993
    for varname in variable_dict:
        print(varname)
        values = hdf_read(f, varname)
        print("min, max: ", values.min(), " ", values.max())
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

    ds = ds.assign_coords(
        {
            "lon": (["dim_0", "dim_1"], longitude),
            "lat": (["dim_0", "dim_1"], latitude),
            "time": (["dim_0", "dim_1"], start_time),
        }
    )
    ds = ds.rename_dims({"dim_0": "Cell_Along_Swath", "dim_1": "Cell_Across_Swath"})

    return ds


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
