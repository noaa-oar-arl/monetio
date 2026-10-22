"""HYTRAJ Reader. Deprecated wrapper — use monetio.load('hytraj', ...) instead."""

from ..readers._deprecation import deprecated_wrapper
from ..readers.hytraj import HYTRAJReader  # noqa: F401


@deprecated_wrapper(
    "monetio.models.hytraj.combine_dataset",
    'monetio.load("hytraj", files=...)',
)
def combine_dataset(flist, taglist=None, renumber=False, verbose=False):
    """Opens multiple tdump files. returns Pandas DataFrame

    Parameters
    ----------
    flist : list
        filenames
    taglist : list, optional
        differentiate trajectories by adding extra pid column with this value.
    renumber : bool, optional
        renumber the trajectories so all trajectories have unique number.
    verbose : bool, optional
        print verbose output.

    Returns
    -------
    pandas.DataFrame
    """
    return HYTRAJReader().open_dataset(
        files=flist,
        taglist=taglist,
        renumber=renumber,
        as_xarray=True,
    )


@deprecated_wrapper(
    "monetio.models.hytraj.open_dataset",
    'monetio.load("hytraj", files=...)',
)
def open_dataset(filename):
    """Opens a tdump file, returns trajectory array

    Parameters
    ----------
    filename : string
        Full file path for tdump file

    Returns
    -------
    pandas.DataFrame
        DataFrame with all trajectory information
    """
    return HYTRAJReader().open_dataset(files=filename, as_xarray=True)
