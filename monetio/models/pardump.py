"""PARDUMP Reader. Deprecated wrapper — use monetio.load('pardump', ...) instead."""

from ..readers._deprecation import deprecated_wrapper
from ..readers.pardump import PardumpReader  # noqa: F401


@deprecated_wrapper(
    "monetio.models.pardump.open_dataset",
    'monetio.load("pardump", files=...)',
)
def open_dataset(fname, drange=None, century=2000, verbose=False):
    """Read a HYSPLIT PARDUMP binary file.

    Parameters
    ----------
    fname : str
        full path to pardump file
    drange : list of two datetime objects, optional
        read in only particle positions between these two dates.
    century : int, optional
        Only the last two digits of the year are stored in the pardump
        file. century must be specified (1900 or 2000) to read in the
        correct year.
    verbose : boolean, optional

    Returns
    -------
    pandas.DataFrame
    """
    return PardumpReader().open_dataset(
        files=fname,
        drange=drange,
        century=century,
        verbose=verbose,
        as_xarray=True,
    )
