"""
GEOMS - The Generic Earth Observation Metadata Standard

This is a format for storing profile data,
used by several LiDAR networks.

It is currently TOLnet's format of choice.

More info: https://evdc.esa.int/documentation/geoms/
"""
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
    for ref, new_dim in [("DATETIME", "time"), ("ALTITUDE", "scan")]:
        n = ds[ref].size
        time_dims = [
            dim_name
            for dim_name, dim_size in ds.dims.items()
            if dim_name.startswith("fakeDim") and dim_size == n
        ]
        ds = ds.rename_dims({dim_name: new_dim for dim_name in time_dims})

    # Set time and altitude (dims of a LiDAR scan) as coords
    ds = ds.set_coords(["DATETIME", "ALTITUDE"])

    return ds
