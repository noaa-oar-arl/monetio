"""
GEOMS - The Generic Earth Observation Metadata Standard

This is a format for storing profile data,
used by several LiDAR networks.

It is currently TOLnet's format of choice.

More info: https://evdc.esa.int/documentation/geoms/
"""
from ..util import _import_required


def open_dataset(fp):
    """
    Parameters
    ----------
    fp
        File path.

    """
    pyhdf = _import_required("pyhdf")
