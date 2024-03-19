import pandas as pd

server = "ftp.star.nesdis.noaa.gov"
base_dir = "/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550/"


def build_urls(dates, *, daily=True, res=0.1, sat="noaa20"):
    """Construct URLs for downloading NEPS data.

    Parameters
    ----------
    dates : pd.DatetimeIndex or iterable of datetime
        Dates to download data for.
    daily : bool, optional
        Whether to download daily (default) or sub-daily data.
    res : float, optional
        Resolution of data in km, only used for sub-daily data.
    sat : str, optional
        Satellite platform, only used for sub-daily data.

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
    if daily:
        dates = dates.floor("D").unique()
    else:  # monthly
        dates = dates.floor("m").unique()
    sat = sat.lower()
    urls = []
    fnames = []
    print("Building VIIRS URLs...")
    base_url = f"https://www.star.nesdis.noaa.gov/pub/smcd/VIIRS_Aerosol/viirs_aerosol_gridded_data/{sat}/aod/eps/"
    if sat == "snpp":
        sat = "npp"
    for dt in dates:
        if daily:
            fname = "viirs_eps_{}_aod_{}_deg_{}_nrt.nc".format(
                sat, str(res).ljust(5, "0"), dt.strftime("%Y%m%d")
            )
        url = base_url + dt.strftime(r"%Y/") + fname
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
        _ = next(r.iter_content(10))
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


def open_dataset(date, satellite="noaa20", res=0.1, daily=True, add_timestamp=True):
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
        if satellite.lower() not in ("noaa20", "snpp"):
            raise ValueError
        elif satellite.lower() == "noaa20":
            sat = "noaa20"
        else:
            sat = "snpp"
    except ValueError:
        print("Invalid input for 'sat': Valid values are 'noaa20' or 'snpp'")

    # if (res != 0.1) or (res != 0.25):
    #    res = 0.1 # assume resolution is 0.1 if wrong value supplied

    urls, fnames = build_urls(d, sat=sat, res=res, daily=daily)
    url = urls.values[0]
    fname = fnames.values[0]

    try:
        if check_remote_file_exists(url) is False:
            raise ValueError
    except ValueError:
        print("File does not exist on NOAA HTTPS server.", url)
        return ValueError
    retrieve(url, fname)

    dset = xr.open_dataset(fname)

    if add_timestamp:
        dset["time"] = d
        dset = dset.expand_dims("time")
        dset = dset.set_coords(["time"])
    return dset


def open_mfdataset(dates, satellite="noaa20", res=0.1, daily=True):
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
        if satellite.lower() not in ("noaa20", "snpp"):
            raise ValueError
        elif satellite.lower() == "noaa20":
            sat = "noaa20"
        else:
            sat = "snpp"
    except ValueError:
        print("Invalid input for 'sat': Valid values are 'noaa20' or 'snpp'")

    urls, fnames = build_urls(d, sat=sat, res=res, daily=daily)

    for url, fname in zip(urls, fnames):
        try:
            if check_remote_file_exists(url) is False:
                raise ValueError
        except ValueError:
            print("File does not exist on NOAA HTTPS server.", url)
            return
        retrieve(url, fname)

    dset = xr.open_mfdataset(fnames, combine="nested", concat_dim={"time": d})
    dset["time"] = d

    return dset
