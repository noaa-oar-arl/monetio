import logging

import numpy as np
import xarray as xr


def read_gridded_eos(fname, var_dict, *, save_as_netcdf=False):
    """
    Parameters
    __________
    fname : str
        Input file path.
    var_dict : dict
        Dictionary of variables to read, along with variable metadata.
        ``{varname: {"fillvalue": float, "scale": float, "units": str}, ...}``
    save_as_netcdf : bool, default=False
        Save variables in var_dict in netcdf format.


    Returns
    _______
    xarray.Dataset
    """

    from .hdfio import hdf_close, hdf_list, hdf_open, hdf_read

    logger = logging.getLogger(__name__)

    ds_dict = dict()

    logger.info("read_gridded_eos:" + fname)
    f = hdf_open(fname)
    datasets, indices = hdf_list(f)

    lon = hdf_read(f, "XDim")
    lat = np.flip(hdf_read(f, "YDim"))
    lon_da = xr.DataArray(lon, attrs={"long_name": "longitude", "units": "deg east"})
    lat_da = xr.DataArray(lat, attrs={"long_name": "latitude", "units": "deg north"})

    for var in var_dict:
        logger.info("read_gridded_eos:" + var)
        data = np.array(hdf_read(f, var), dtype=float)
        data = np.flip(data, axis=0)
        data[data == var_dict[var]["fillvalue"]] = np.nan
        data *= var_dict[var]["scale"]
        var_da = xr.DataArray(
            data,
            coords=[lat_da, lon_da],
            dims=["lat", "lon"],
            attrs={"units": var_dict[var]["units"]},
        )
        ds_dict[var] = var_da

    hdf_close(f)

    ds = xr.Dataset(ds_dict)

    if save_as_netcdf:
        fname_nc = fname.replace(".hdf", ".nc")
        logger.info("writing " + fname_nc)
        ds.to_netcdf(fname_nc)

    return ds
