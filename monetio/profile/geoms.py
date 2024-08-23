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
    from pathlib import Path

    ext = Path(fp).suffix.lower()

    if ext in {".h4", ".hdf4", ".hdf"}:
        pyhdf_SD = _import_required("pyhdf.SD")

        sd = pyhdf_SD.SD(str(fp))

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
    elif ext in {".h5", ".he5", ".hdf5"}:
        import h5py

        f = h5py.File(fp, "r")
        data_vars = {
            k: (
                tuple(_rename_h5_dim(str(d)) for d in v.dims),
                v[...],
                dict(v.attrs),
            )
            for k, v in f.items()
        }
        attrs = dict(f.attrs)
        f.close()
    else:
        raise ValueError(f"unrecognized file extension: {ext!r}")

    ds = xr.Dataset(
        data_vars=data_vars,
        attrs=attrs,
    )

    # Set instrument position as coords
    instru_coords = ["LATITUDE.INSTRUMENT", "LONGITUDE.INSTRUMENT", "ALTITUDE.INSTRUMENT"]
    for vn in instru_coords:
        da = ds[vn]
        if da.ndim == 0:
            ds = ds.set_coords(vn)
            continue
        (dim_name0,) = da.dims
        dim_name = _rename_var(vn)
        ds = ds.set_coords(vn).rename_dims({dim_name0: dim_name})

    # Rename time and scan dims
    rename_main_dims = {"DATETIME": "time", "ALTITUDE": "altitude"}
    for ref, new_dim in list(rename_main_dims.items()):
        if ref not in ds:
            del rename_main_dims[ref]
            continue
        n = ds[ref].size
        time_dims = [
            dim_name
            for dim_name, dim_size in ds.sizes.items()
            if dim_name.startswith("fakeDim") and dim_size == n
        ]
        ds = ds.rename_dims({dim_name: new_dim for dim_name in time_dims})

    # Squeeze out some unnecessary fakeDims of float vars
    # Possible for 'PRESSURE_INDEPENDENT' and 'TEMPERATURE_INDEPENDENT'
    for vn, da in ds.variables.items():
        if da.ndim >= 1 and da.dims[-1].startswith("fakeDim") and da.dtype.kind == "f":
            n = da.sizes[da.dims[-1]]
            if n == 1:
                ds[vn] = da.squeeze(dim=da.dims[-1])

    # Deal with remaining fakeDims
    # 'PRESSURE_INDEPENDENT_SOURCE'
    # 'TEMPERATURE_INDEPENDENT_SOURCE'
    # These are '|S1' char arrays that need to be joined to make strings along the last dim
    remaining_vns = [
        vn for vn, da in ds.variables.items() if any(dim.startswith("fakeDim") for dim in da.dims)
    ]
    for vn in remaining_vns:
        da = ds[vn]
        if not da.dtype.kind == "S":
            continue
        *other_dims, fake_dim = da.dims
        other_dims = other_dims
        assert fake_dim.startswith("fakeDim")
        assert not any(d.startswith("fakeDim") for d in other_dims)
        if not other_dims:
            ds[vn] = ((), "".join(c.decode("utf-8") for c in da.values), da.attrs)
        else:
            # Note: This xarray method works but seems to have a 31-char limit?
            # with xr.set_options(keep_attrs=True):
            #     ds[vn] = da.str.join(dim=fake_dim).str.decode("utf-8")
            ds[vn] = (
                da.str.decode("utf-8")
                .to_series()
                .groupby(other_dims)
                .agg("".join)
                .to_xarray()
                .astype(str)
                .drop_vars(other_dims)  # fake coords at this point
                .assign_attrs(da.attrs)
            )

    unique_dims = set(ds.dims)
    fake_dims = {dim for dim in unique_dims if dim.startswith("fakeDim")}
    if fake_dims:
        from collections import defaultdict

        dim_to_vn = defaultdict(list)
        for vn in ds.variables:
            for dim in ds[vn].dims:
                dim_to_vn[dim].append(vn)
        fake_dim_info = ", ".join(
            f"{dim}({ds.sizes[dim]}) [{', '.join(dim_to_vn[dim])}]" for dim in sorted(fake_dims)
        )
        warnings.warn(f"There are still some fakeDim's around: {fake_dim_info}")

    # Normalize dtypes
    for vn, da in ds.variables.items():
        if da.dtype.kind == "S":
            ds[vn] = da.astype(str)
        elif da.dtype.kind == "O":
            try:
                x = da.values[0]
            except IndexError:
                x = da.item()
            if isinstance(x, bytes):
                ds[vn] = da.astype(str)
        elif da.dtype.kind == "f":
            if da.dtype.byteorder not in {"=", "|"}:
                ds[vn] = da.astype(da.dtype.newbyteorder("="))

    # Set time and altitude (dims of a LiDAR scan) as coords
    ds = ds.set_coords(list(rename_main_dims))

    # Convert time arrays to datetime format
    tstart_from_attr = pd.Timestamp(attrs["DATA_START_DATE"])
    tstop_from_attr = pd.Timestamp(attrs["DATA_STOP_DATE"])
    t = _dti_from_mjd2000(ds.DATETIME)
    tlb = _dti_from_mjd2000(ds["DATETIME.START"])  # lower bounds
    tub = _dti_from_mjd2000(ds["DATETIME.STOP"])  # upper
    dt = tstart_from_attr.tz_localize(None) - tlb[0]
    if not abs(dt) < pd.Timedelta(milliseconds=500):
        warnings.warn(
            f"first DATETIME.START ({tlb[0]}) "
            f"is more than 500 ms from the DATA_START_DATE ({tstart_from_attr})"
        )
    dt = tstop_from_attr.tz_localize(None) - tub[-1]
    if not abs(dt) < pd.Timedelta(milliseconds=500):
        warnings.warn(
            f"last DATETIME.STOP ({tub[-1]}) "
            f"is more than 500 ms from the DATA_STOP_DATE ({tstop_from_attr})"
        )
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


def _rename_h5_dim(s):
    # e.g. '<"" dimension 0 of HDF5 dataset at 2054726550768>'
    import re

    s_re = r'<"(.*)" dimension (\d+) of HDF5 dataset at (\d+)>'
    m = re.fullmatch(s_re, s)
    if m is None:
        raise ValueError(f"unexpected str of h5 dim: {s!r}. Expected to match {s_re!r}.")

    label, num, _ = m.groups()

    return f"fakeDim{num}{label}"


def _rename_var(vn, *, under="_", dot="_"):
    return vn.lower().replace("_", under).replace(".", dot)


def _dti_from_mjd2000(x):
    """Convert xr.DataArray of GEOMS times to a pd.DatetimeIndex."""
    assert x.VAR_UNITS == "MJD2K" or x.VAR_UNITS == "MJD2000"
    # 2400000.5 -- offset for MJD
    # 51544 -- offset between MJD2000 and MJD
    return pd.to_datetime(np.asarray(x) + 2400000.5 + 51544, unit="D", origin="julian")
