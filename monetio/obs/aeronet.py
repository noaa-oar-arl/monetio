"""AERONET Reader. Deprecated wrapper — use monetio.load('aeronet', ...) instead."""

from ..readers._deprecation import deprecated_wrapper
from ..readers.aeronet import AERONET, AERONETReader, get_valid_sites  # noqa: F401


@deprecated_wrapper(
    "monetio.obs.aeronet.add_data",
    'monetio.load("aeronet", dates=...)',
)
def add_data(
    dates=None,
    product="AOD15",
    *,
    inv_type=None,
    latlonbox=None,
    siteid=None,
    daily=False,
    lunar=False,
    freq=None,
    detect_dust=False,
    interp_to_aod_values=None,
    n_procs=1,
    verbose=10,
    as_xarray=True,
    **kwargs,
):
    """Retrieve and load AERONET data."""
    return AERONETReader().open_dataset(
        dates=dates,
        product=product,
        inv_type=inv_type,
        latlonbox=latlonbox,
        siteid=siteid,
        daily=daily,
        lunar=lunar,
        freq=freq,
        detect_dust=detect_dust,
        interp_to_aod_values=interp_to_aod_values,
        n_procs=n_procs,
        as_xarray=as_xarray,
        **kwargs,
    )


@deprecated_wrapper(
    "monetio.obs.aeronet.add_local",
    'monetio.load("aeronet", files=...)',
)
def add_local(
    fname,
    *,
    freq=None,
    detect_dust=False,
    interp_to_aod_values=None,
    as_xarray=True,
    **kwargs,
):
    """Read a local AERONET file."""
    return AERONETReader().open_dataset(
        files=fname,
        freq=freq,
        detect_dust=detect_dust,
        interp_to_aod_values=interp_to_aod_values,
        as_xarray=as_xarray,
        **kwargs,
    )
