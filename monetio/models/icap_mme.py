import pandas as pd

server = "ftp.star.nesdis.noaa.gov"
base_dir = "/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550/"


def build_urls(dates, *, filetype="mmc", data_var="dustaod550"):
    """Construct URLs for downloading NEPS data.

    Parameters
    ----------
    dates : pd.DatetimeIndex or iterable of datetime
        Dates to download data for.
    filetype : str, optional
        mmc or c4
    res : float, optional
        Resolution of data in km, only used for sub-daily data.
    sat : str, optional
        variable

    Returns
    -------
    pd.Series
        Series with URLs and corresponding file names.

    Notes
    -----
    The `res` and `sat` parameters are only used for sub-daily data.
    """

    from collections.abc import Iterable

    if isinstance(dates, Iterable):
        dates = pd.DatetimeIndex(dates)
    else:
        dates = pd.DatetimeIndex([dates])
    if filetype == "mmc":
        ft = "MMC"
    elif filetype == "c4":
        ft = "C4"
    else:
        ft = "MME"

    urls = []
    fnames = []
    print("Building VIIRS URLs...")  #
    base_url = "https://usgodae.org/ftp/outgoing/nrl/ICAP-MME/"

    for dt in dates:
        fname = "icap_{}_{}_{}.nc".format(dt.strftime("%Y%m%d%H"), ft, data_var.lower())
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
    """Download files from the airnowtech S3 server.

    Parameters
    ----------
    url : string
        Description of parameter `url`.
    fname : string
        Description of parameter `fname`.

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


def open_dataset(date, product="mmc", data_var="modeaod550"):
    """
    Parameters
    ----------
    datestr : str or datetime-like
        The date for which to open the dataset.
        2022-10-29 to current is available.
    """
    import pandas as pd
    import xarray as xr

    if not isinstance(date, pd.Timestamp):
        d = pd.to_datetime(date)
    else:
        d = date

    try:
        if product.lower() not in ("mmc", "c4", "mme"):
            raise ValueError
    except ValueError:
        print("Invalid input for 'product': Valid values are 'MMC' 'C4' 'MME'")

    try:
        if data_var.lower() not in (
            "modeaod550",
            "dustaod550",
            "pm",
            "seasaltaod550",
            "smokeaod550",
            "totaldustaod550",
        ):
            raise ValueError
    except ValueError:
        print(
            "Invalid input for 'data_var': Valid values are 'modeaod550' 'dustaod550' 'pm' 'seasaltaod550' 'smokeaod550' 'totaldustaod550'"
        )

    urls, fnames = build_urls(d, filetype=product, data_var=data_var)
    url = urls.values[0]
    fname = fnames.values[0]
    print(url)
    print(fname)
    try:
        if check_remote_file_exists(url) is False:
            raise ValueError
    except ValueError:
        print("File does not exist on ICAP HTTPS server.", url)
        return ValueError
    retrieve(url, fname)

    dset = xr.open_dataset(fname)

    return dset


def open_mfdataset(dates, product="mmc", data_var="modeaod550"):
    """
    Parameters
    ----------
    datestr : str or datetime-like
        The date for which to open the dataset.
        2022-10-29 to current is available.
    """
    import pandas as pd
    import xarray as xr

    try:
        if isinstance(dates, pd.DatetimeIndex):
            d = dates
        else:
            raise TypeError
    except TypeError:
        print("Please provide a pandas.DatetimeIndex")
        return

    try:
        if product.lower() not in ("mmc", "c4", "mme"):
            raise ValueError
    except ValueError:
        print("Invalid input for 'product': Valid values are 'MMC' 'C4' 'MME'")

    try:
        if data_var.lower() not in (
            "modeaod550",
            "dustaod550",
            "pm",
            "seasaltaod550",
            "smokeaod550",
            "totaldustaod550",
        ):
            raise ValueError
    except ValueError:
        print(
            "Invalid input for 'data_var': Valid values are 'modeaod550' 'dustaod550' 'pm' 'seasaltaod550' 'smokeaod550' 'totaldustaod550'"
        )

    urls, fnames = build_urls(d, filetype=product, data_var=data_var)
    url = urls.values[0]
    fname = fnames.values[0]

    for url, fname in zip(urls, fnames):
        try:
            if check_remote_file_exists(url) is False:
                raise ValueError
        except ValueError:
            print("File does not exist on ICAP HTTPS server.", url)
            return
        retrieve(url, fname)

    dset = xr.open_mfdataset(fnames, combine="nested", concat_dim="time")
    # dset["time"] = d

    return dset
