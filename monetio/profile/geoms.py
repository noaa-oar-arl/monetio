"""
GEOMS -- The Generic Earth Observation Metadata Standard

This is a format for storing profile data,
used by several LiDAR networks.

It is currently `TOLNet <https://www-air.larc.nasa.gov/missions/TOLNet/>`__'s
format of choice.

For more info, see: https://evdc.esa.int/documentation/geoms/
"""
import warnings

import numpy as np
import pandas as pd
import xarray as xr

from ..util import _import_required


def open_dataset(fp, *, rename_all=True, squeeze=True):
    """Open a file in GEOMS format, e.g. modern TOLNet files.

    Parameters
    ----------
    fp
        File path.
    rename_all : bool, default: True
        Rename all non-coordinate variables:

        * lowercase
        * convert ``.`` to ``_``

        as done for the coordinate variables regardless of this setting.
        These conversions allow for easy access to the variables as attributes,
        e.g. ::

            ds.integration_time
    squeeze : bool, default: True
        Apply ``.squeeze()`` before returning the Dataset.
        This simplifies working with the Dataset for the case of one instrument/location.

    Returns
    -------
    xarray.Dataset
    """
    pyhdf_SD = _import_required("pyhdf.SD")

    sd = pyhdf_SD.SD(fp)

    data_vars = {}
    for name, _ in sd.datasets().items():
        sds = sd.select(name)

        data = sds.get()
        dims = tuple(sds.dimensions())
        attrs = sds.attributes()

        data_vars[name] = (dims, data, attrs)

        sds.endaccess()

    attrs = sd.attributes()

    sd.end()

    ds = xr.Dataset(
        data_vars=data_vars,
        attrs=attrs,
    )

    # Set instrument position as coords
    instru_coords = ["LATITUDE.INSTRUMENT", "LONGITUDE.INSTRUMENT", "ALTITUDE.INSTRUMENT"]
    for vn in instru_coords:
        da = ds[vn]
        (dim_name0,) = da.dims
        dim_name = _rename_var(vn)
        ds = ds.set_coords(vn).rename_dims({dim_name0: dim_name})

    # Rename time and scan dims
    rename_main_dims = {"DATETIME": "time", "ALTITUDE": "altitude"}
    for ref, new_dim in rename_main_dims.items():
        n = ds[ref].size
        time_dims = [
            dim_name
            for dim_name, dim_size in ds.dims.items()
            if dim_name.startswith("fakeDim") and dim_size == n
        ]
        ds = ds.rename_dims({dim_name: new_dim for dim_name in time_dims})

    # Deal with remaining fakeDims
    # 'PRESSURE_INDEPENDENT_SOURCE'
    # 'TEMPERATURE_INDEPENDENT_SOURCE'
    # These are '|S1' char arrays that need to be joined to make strings
    remaining_vns = [
        vn for vn, da in ds.variables.items() if any(dim.startswith("fakeDim") for dim in da.dims)
    ]
    for vn in remaining_vns:
        da = ds[vn]
        assert da.dtype.kind == "S"
        (dim0, dim1) = da.dims
        assert dim1.startswith("fakeDim") and not dim0.startswith("fakeDim")
        strings = [b"".join(row).decode() for row in da.values]
        ds[vn] = ((dim0,), strings, da.attrs)

    unique_dims = {dim for v in ds.variables.values() for dim in v.dims}
    if any(dim.startswith("fakeDim") for dim in unique_dims):
        warnings.warn(f"There are still some fakeDim's around in the set of dims: {unique_dims}")

    # Set time and altitude (dims of a LiDAR scan) as coords
    ds = ds.set_coords(["DATETIME", "ALTITUDE"])

    # Convert time arrays to datetime format
    tstart_from_attr = pd.Timestamp(attrs["DATA_START_DATE"])
    tstop_from_attr = pd.Timestamp(attrs["DATA_STOP_DATE"])
    t = _dti_from_mjd2000(ds.DATETIME)
    tlb = _dti_from_mjd2000(ds["DATETIME.START"])  # lower bounds
    tub = _dti_from_mjd2000(ds["DATETIME.STOP"])  # upper
    assert abs(tstart_from_attr.tz_localize(None) - tlb[0]) < pd.Timedelta(
        milliseconds=100
    ), "times should be consistent with DATA_START_DATE attr"
    assert abs(tstop_from_attr.tz_localize(None) - tub[-1]) < pd.Timedelta(
        milliseconds=100
    ), "times should be consistent with DATA_STOP_DATE attr"
    ds["DATETIME"].values = t
    ds["DATETIME.START"].values = tub
    ds["DATETIME.STOP"].values = tlb

    # Match coords to dim names (so can use sel and such)
    ds = ds.rename_vars(rename_main_dims)
    ds = ds.rename_vars({old: _rename_var(old) for old in instru_coords})

    # Rename other variables
    if rename_all:
        ds = ds.rename_vars({old: _rename_var(old) for old in ds.data_vars})

    # latitude_instrument -> latitude
    if "latitude_instrument" in ds.coords and "latitude" not in ds.coords:
        ds = ds.rename(
            {
                "latitude_instrument": "latitude",
                "longitude_instrument": "longitude",
            }
        )

    if squeeze:
        ds = ds.squeeze()

    return ds


def _rename_var(vn, *, under="_", dot="_"):
    return vn.lower().replace("_", under).replace(".", dot)


def _dti_from_mjd2000(x):
    """Convert xr.DataArray of GEOMS times to a pd.DatetimeIndex."""
    assert x.VAR_UNITS == "MJD2K" or x.VAR_UNITS == "MJD2000"
    # 2400000.5 -- offset for MJD
    # 51544 -- offset between MJD2000 and MJD
    return pd.to_datetime(np.asarray(x) + 2400000.5 + 51544, unit="D", origin="julian")
