import pandas as pd

server = "ftp.star.nesdis.noaa.gov"
base_dir = "/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550/"

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

    r = requests.head(file_url, stream=True, verify=False)

    if r.status_code == 200:
        return True
    else:
        print(f"HTTP Error {r.status_code} - {r.reason}")
        return False


def retrieve(url, fname):
    """Download the file at `url` to path `fname` if it doesn't exist.

    Parameters
    ----------
    url : str
    fname : str or path-like

    Returns
    -------
    None
    """
    import os

    import requests

    if not os.path.isfile(fname):
        print("\n Retrieving: " + fname)
        print(url)
        print("\n")
        r = requests.get(url)
        r.raise_for_status()
        with open(fname, "wb") as f:
            f.write(r.content)
    else:
        print("\n File Exists: " + fname)


def open_dataset(date, product="MMC", data_var="modeaod550"):
    """
    Parameters
    ----------
    date : str or datetime-like
        The date for which to open the dataset.
        2022-10-29 to current is available.
    product : {'MMC', 'C4', 'MME'}, optional
    data_var : {'modeaod550', 'dustaod550', 'pm', 'seasaltaod550', \
        'smokeaod550', 'totaldustaod550'}, optional

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
    print(url)
    print(fname)
    if check_remote_file_exists(url) is False:
        raise ValueError(f"File does not exist on ICAP HTTPS server: {url}")
    retrieve(url, fname)

    dset = xr.open_dataset(fname)

    return dset


def open_mfdataset(dates, product="MMC", data_var="modeaod550"):
    """
    Parameters
    ----------
    dates : str or datetime-like
        The dates for which to open the dataset.
        2022-10-29 to current is available.
    product : {'MMC', 'C4', 'MME'}, optional
    data_var : {'modeaod550', 'dustaod550', 'pm', 'seasaltaod550', \
        'smokeaod550', 'totaldustaod550'}, optional

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
    url = urls.values[0]
    fname = fnames.values[0]

    for url, fname in zip(urls, fnames):
        if check_remote_file_exists(url) is False:
            raise ValueError(f"File does not exist on ICAP HTTPS server: {url}")
        retrieve(url, fname)

    dset = xr.open_mfdataset(fnames, combine="nested", concat_dim="time")
    # dset["time"] = d

    return dset
