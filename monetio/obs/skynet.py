"""SKYNET Reader. Deprecated wrapper — use monetio.load('skynet', ...) instead."""

from ..readers._deprecation import deprecated_wrapper
from ..readers.skynet import SKYNETReader  # noqa: F401


@deprecated_wrapper(
    "monetio.obs.skynet.add_data",
    'monetio.load("skynet", dates=...)',
)
def add_data(
    dates=None,
    siteid=None,
    product="AOT",
    as_xarray=True,
    **kwargs,
):
    """Retrieve and load SKYNET data."""
    return SKYNETReader().open_dataset(
        dates=dates,
        siteid=siteid,
        product=product,
        as_xarray=as_xarray,
        **kwargs,
    )


@deprecated_wrapper(
    "monetio.obs.skynet.add_local",
    'monetio.load("skynet", files=...)',
)
def add_local(
    fname,
    as_xarray=True,
    **kwargs,
):
    """Read a local SKYNET file."""
    return SKYNETReader().open_dataset(
        files=fname,
        as_xarray=as_xarray,
        **kwargs,
    )
