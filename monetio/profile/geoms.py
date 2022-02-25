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

    return ds
