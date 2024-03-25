"""
ICAP-MME

"`ICAP <https://aero.und.edu/atmos/icap/index.html>`__
(International Cooperative for Aerosol Prediction)
is an international forum for aerosol forecast centres,
remote sensing data providers and lead system developers
to share best practices and discuss pressing issues
facing the operational aerosol community."

The ICAP multi-model ensemble (ICAP-MME) is constructed from
the following aerosol forecast systems:

- ECMWF CAMS
- JMA MASINGAR
- NASA GEOS
- NRL NAAPS
- NOAA GEFS-Aerosols (NGAC before 2022-09)
- Météo‐France MOCAGE
- FMI SILAM
- UKMO dust forecast (Unified Model)
- BSC MONARCH

The C4C product is a "core four multi-model consensus product",
including members that use MODIS and other observational total AOD in assimilation:

- ECMWF CAMS
- JMA MASINGAR
- NASA GEOS
- NRL NAAPS

The data is 6-hourly, each file including multiple time steps (e.g. 21 or 25).

Online visualizer: https://usgodae.org/metools/ensemble/

The files are loaded from: https://usgodae.org/ftp/outgoing/nrl/ICAP-MME/
"""

import pandas as pd

valid_filetypes = ("MMC", "C4", "MME")
valid_data_vars = (
    "modeaod550",
    "dustaod550",
    "pm",
    "seasaltaod550",
    "smokeaod550",
    "totaldustaod550",
)


def build_urls(dates, filetype="MMC", data_var="dustaod550", *, verbose=True):
    """Construct URLs for downloading NEPS data.

    Parameters
    ----------
    dates : datetime-like or iterable of datetime-like
        Dates to download data for.
    filetype : {'MMC', 'C4', 'MME'}, optional
        The first MME date is 2014-11-28, and the last 2022-09-11.
        The first non-MME (C4 and MMC) is 2022-09-01, and these continue to the present.
    data_var : {'modeaod550', 'dustaod550', 'pm', 'seasaltaod550', \
        'smokeaod550', 'totaldustaod550'}, optional

    Returns
    -------
    urls : pd.Series
    fnames : pd.Series
    """

    from collections.abc import Iterable

    if isinstance(dates, Iterable):
        dates = pd.DatetimeIndex(dates)
    else:
        dates = pd.DatetimeIndex([dates])

    urls = []
    fnames = []
    if verbose:
        print("Building ICAP-MME URLs...")
    base_url = "https://usgodae.org/ftp/outgoing/nrl/ICAP-MME/"

    for dt in dates:
        fname = "icap_{}_{}_{}.nc".format(
            dt.strftime(r"%Y%m%d%H"), filetype.upper(), data_var.lower()
        )
        url = base_url + dt.strftime(r"%Y/%Y%m/") + fname
        urls.append(url)
        fnames.append(fname)

    # Note: files needed for comparison
    urls = pd.Series(urls, index=None)
    fnames = pd.Series(fnames, index=None)
    return urls, fnames


def remote_file_exists(file_url, *, verbose=True):
    import requests

    r = requests.head(file_url)

    if r.status_code == 200:
        return True
    else:
        if verbose:
            print(f"HTTP Error {r.status_code} - {r.reason}")
        return False


def retrieve(url, fname, *, download=False, verbose=True):
    """Return BytesIO of the file at `url` or
    download it to path `fname` if it doesn't exist.

    Parameters
    ----------
    url : str
    fname : str or path-like
    download : bool, optional
        If True, download the file (if it doesn't already exist)
        and return ``pathlib.Path``.
        Otherwise, return ``io.BytesIO`` of the file in memory.

    Returns
    -------
    io.BytesIO or pathlib.Path
        The requested netCDF file.
    """
    from io import BytesIO
    from pathlib import Path

    import requests

    p = Path(fname).absolute()

    if not download:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        return BytesIO(r.content)
    else:
        if not p.is_file():
            if verbose:
                print(f"Downloading {url} to {p.as_posix()}")
            r = requests.get(url, stream=True)
            r.raise_for_status()
            with open(p, "wb") as f:
                f.write(r.content)
        else:
            if verbose:
                print(f"File Exists: {p.as_posix()}")
        return p


def _check_file_url(url, *, verbose=True):
    """
    Raises
    ------
    ValueError
        If the file URL HEAD request doesn't return 200,
        with info about how to check available products / data vars for the date.
    """
    if not remote_file_exists(url, verbose=verbose):
        raise ValueError(
            f"File does not exist on ICAP HTTPS server: {url}. "
            f"Check {url[:url.index('icap_')]} to see the available "
            "`product` and `data_var`s for this month."
        )


def open_dataset(date, product="MMC", data_var="dustaod550", *, download=False, verbose=True):
    """
    Parameters
    ----------
    date : str or datetime-like
        The date for which to open the dataset.
    product : {'MMC', 'C4', 'MME'}, optional
        The first MME date is 2014-11-28, and the last 2022-09-11.
        The first non-MME (C4 and MMC) is 2022-09-01, and these continue to the present.
    data_var : {'modeaod550', 'dustaod550', 'pm', 'seasaltaod550', \
        'smokeaod550', 'totaldustaod550'}, optional
        Note that which data variables are available
        depends on the `product` selection and the date.
    download : bool, optional
        If True, use files on disk, downloading if necessary.
        If False, download and load dataset in memory.

    Returns
    -------
    xarray.Dataset

    Raises
    ------
    ValueError
        If input parameters are invalid or a file does not exist on the server.
    """
    import pandas as pd
    import xarray as xr

    if not isinstance(date, pd.Timestamp):
        d = pd.to_datetime(date)
    else:
        d = date

    if product.upper() not in valid_filetypes:
        raise ValueError(f"Invalid input for 'product': Valid values are {valid_filetypes}.")

    if data_var.lower() not in valid_data_vars:
        raise ValueError(f"Invalid input for 'data_var': Valid values are {valid_data_vars}.")

    urls, fnames = build_urls(d, filetype=product, data_var=data_var, verbose=verbose)
    url = urls.values[0]
    fname = fnames.values[0]
    _check_file_url(url, verbose=verbose)
    dset = xr.open_dataset(retrieve(url, fname, download=download, verbose=verbose))

    return dset


def open_mfdataset(dates, product="MMC", data_var="dustaod550", *, download=False, verbose=True):
    """
    .. note::
       Depending on the selected product/variable and the provided dates,
       the result may have overlapping times.
       You may wish to apply ``.drop_duplicates("time", keep="last")``.

    Parameters
    ----------
    dates : iterable of datetime-like
        The dates for which to open the dataset.
    product : {'MMC', 'C4', 'MME'}, optional
        The first MME date is 2014-11-28, and the last 2022-09-11.
        The first non-MME (C4 and MMC) is 2022-09-01, and these continue to the present.
    data_var : {'modeaod550', 'dustaod550', 'pm', 'seasaltaod550', \
        'smokeaod550', 'totaldustaod550'}, optional
        Note that which data variables are available
        depends on the `product` selection and the date.
    download : bool, optional
        If True, use files on disk, downloading if necessary.
        If False, download and load dataset in memory.
        In this case, the files are fully loaded instead of being opened lazily.

    Returns
    -------
    xarray.Dataset

    Raises
    ------
    ValueError
        If input parameters are invalid or a file does not exist on the server.
    """
    import pandas as pd
    import xarray as xr

    d = pd.DatetimeIndex(dates)

    if product.upper() not in valid_filetypes:
        raise ValueError(f"Invalid input for 'product': Valid values are {valid_filetypes}.")

    if data_var.lower() not in valid_data_vars:
        raise ValueError(f"Invalid input for 'data_var': Valid values are {valid_data_vars}.")

    urls, fnames = build_urls(d, filetype=product, data_var=data_var, verbose=verbose)

    if download is True:
        paths = []
        for url, fname in zip(urls, fnames):
            _check_file_url(url, verbose=verbose)
            paths.append(retrieve(url, fname, download=True, verbose=verbose))
        dset = xr.open_mfdataset(paths, combine="nested", concat_dim="time")
    else:
        dsets = []
        for url, fname in zip(urls, fnames):
            _check_file_url(url, verbose=verbose)
            o = retrieve(url, fname, download=False, verbose=verbose)
            dsets.append(xr.open_dataset(o))
        dset = xr.concat(dsets, dim="time")

    return dset
