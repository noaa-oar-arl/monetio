"""
GEOMS - The Generic Earth Observation Metadata Standard

This is a format for storing profile data,
used by several LiDAR networks.

It is currently TOLnet's format of choice.

More info: https://evdc.esa.int/documentation/geoms/
"""
import pandas as pd
import xarray as xr

from ..util import _import_required


def open_dataset(fp):
    """
    Parameters
    ----------
    fp
        File path.

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
    for vn in ["LATITUDE.INSTRUMENT", "LONGITUDE.INSTRUMENT", "ALTITUDE.INSTRUMENT"]:
        da = ds[vn]
        (dim_name0,) = da.dims
        dim_name = vn.lower().replace(".", "_")
        ds = ds.set_coords(vn).rename_dims({dim_name0: dim_name})

    # Rename time and scan dims
    for ref, new_dim in [("DATETIME", "time"), ("ALTITUDE", "altitude")]:
        n = ds[ref].size
        time_dims = [
            dim_name
            for dim_name, dim_size in ds.dims.items()
            if dim_name.startswith("fakeDim") and dim_size == n
        ]
        ds = ds.rename_dims({dim_name: new_dim for dim_name in time_dims})

    # TODO: the fakeDims with length 5
    # 'PRESSURE_INDEPENDENT_SOURCE'
    # 'TEMPERATURE_INDEPENDENT_SOURCE'
    # These are '|S1' char arrays that need to be joined to make strings

    # Set time and altitude (dims of a LiDAR scan) as coords
    ds = ds.set_coords(["DATETIME", "ALTITUDE"])

    # Convert time arrays to datetime format
    tstart_from_attr = pd.Timestamp(attrs["DATA_START_DATE"])
    tstop_from_attr = pd.Timestamp(attrs["DATA_STOP_DATE"])
    t = _dti_from_mjd2000(ds.DATETIME.values)
    tlb = _dti_from_mjd2000(ds["DATETIME.START"].values)  # lower bounds
    tub = _dti_from_mjd2000(ds["DATETIME.STOP"].values)  # upper
    assert abs(tstart_from_attr.tz_localize(None) - tlb[0]) < pd.Timedelta(
        milliseconds=100
    ), "times should be consistent with DATA_START_DATE attr"
    assert abs(tstop_from_attr.tz_localize(None) - tub[-1]) < pd.Timedelta(
        milliseconds=100
    ), "times should be consistent with DATA_STOP_DATE attr"
    ds["time"] = ("time", t)
    ds["DATETIME"].values = t
    ds["DATETIME.START"].values = tub
    ds["DATETIME.STOP"].values = tlb

    # TODO: coordinates match dim names (so can use sel)

    return ds


def _dti_from_mjd2000(x):
    # 2400000.5 -- offset for MJD
    # 51544 -- offset between MJD2000 and MJD
    return pd.to_datetime(x + 2400000.5 + 51544, unit="D", origin="julian")
