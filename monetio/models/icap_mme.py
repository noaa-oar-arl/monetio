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


def build_urls(dates, *, filetype="MMC", data_var="dustaod550"):
    """Construct URLs for downloading NEPS data.

    Parameters
    ----------
    dates : pd.DatetimeIndex or iterable of datetime
        Dates to download data for.
    filetype : {'MMC', 'C4', 'MME'}, optional
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


def check_remote_file_exists(file_url):
    import requests

    r = requests.head(file_url, verify=False)

    if r.status_code == 200:
        return True
    else:
        print(f"HTTP Error {r.status_code} - {r.reason}")
        return False


def retrieve(url, fname, *, download=False, stream=True, verbose=True):
    """Download the file at `url` to path `fname` if it doesn't exist.

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
    pathlib.Path or io.BytesIO
        The requested netCDF file.
    """
    from io import BytesIO
    from pathlib import Path

    import requests

    p = Path(fname).absolute()

    if not download:
        r = requests.get(url, stream=stream)
        r.raise_for_status()
        return BytesIO(r.content)
    else:
        if not p.is_file():
            if verbose:
                print(f"Downloading {url} to {p.as_posix()}")
            r = requests.get(url, stream=stream)
            r.raise_for_status()
            with open(p, "wb") as f:
                f.write(r.content)
        else:
            if verbose:
                print(f"File Exists: {p.as_posix()}")
        return p


def open_dataset(date, product="MMC", data_var="modeaod550", download=False):
    """
    Parameters
    ----------
    date : str or datetime-like
        The date for which to open the dataset.
        2022-10-29 to current is available.
    product : {'MMC', 'C4', 'MME'}, optional
    data_var : {'modeaod550', 'dustaod550', 'pm', 'seasaltaod550', \
        'smokeaod550', 'totaldustaod550'}, optional
    download : bool, optional
        If True, use files on disk, downloading if necessary.
        If False, load from memory.

    Returns
    -------
    xarray.Dataset
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

    urls, fnames = build_urls(d, filetype=product, data_var=data_var)
    url = urls.values[0]
    fname = fnames.values[0]
    if check_remote_file_exists(url) is False:
        raise ValueError(f"File does not exist on ICAP HTTPS server: {url}")
    if download is True:
        p = retrieve(url, fname, download=True)
        dset = xr.open_dataset(p)
    else:
        o = retrieve(url, fname, download=False)
        dset = xr.open_dataset(o)

    return dset


def open_mfdataset(dates, product="MMC", data_var="modeaod550", download=False):
    """
    Parameters
    ----------
    dates : str or datetime-like
        The dates for which to open the dataset.
        2022-10-29 to current is available.
    product : {'MMC', 'C4', 'MME'}, optional
    data_var : {'modeaod550', 'dustaod550', 'pm', 'seasaltaod550', \
        'smokeaod550', 'totaldustaod550'}, optional
    download : bool, optional
        If True, use files on disk, downloading if necessary.
        If False, load from memory.
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

    if isinstance(dates, pd.DatetimeIndex):
        d = dates
    else:
        raise TypeError(f"Please provide a pandas.DatetimeIndex. Got {type(dates)}.")

    if product.upper() not in valid_filetypes:
        raise ValueError(f"Invalid input for 'product': Valid values are {valid_filetypes}.")

    if data_var.lower() not in valid_data_vars:
        raise ValueError(f"Invalid input for 'data_var': Valid values are {valid_data_vars}.")

    urls, fnames = build_urls(d, filetype=product, data_var=data_var)

    if download is True:
        for url, fname in zip(urls, fnames):
            paths = []
            if check_remote_file_exists(url) is False:
                raise ValueError(f"File does not exist on ICAP HTTPS server: {url}")
            paths.append(retrieve(url, fname, download=True))
        dset = xr.open_mfdataset(paths, combine="nested", concat_dim="time")
    else:
        dsets = []
        for url, fname in zip(urls, fnames):
            if check_remote_file_exists(url) is False:
                raise ValueError(f"File does not exist on ICAP HTTPS server: {url}")
            o = retrieve(url, fname, download=False)
            dsets.append(xr.open_dataset(o))
        dset = xr.concat(dsets, dim="time")

    return dset
