"""AirNow"""

import os

# this is written to retrive airnow data concatenate and add to pandas array
# for usage
from datetime import datetime

import pandas as pd

datadir = "."
cwd = os.getcwd()
url = None
dates = [
    datetime.strptime("2016-06-06 12:00:00", "%Y-%m-%d %H:%M:%S"),
    datetime.strptime("2016-06-06 13:00:00", "%Y-%m-%d %H:%M:%S"),
]
daily = False
objtype = "AirNow"
filelist = None
monitor_df = None
savecols = [
    "time",
    "siteid",
    "site",
    "utcoffset",
    "variable",
    "units",
    "obs",
    "time_local",
    "latitude",
    "longitude",
    "cmsa_name",
    "msa_code",
    "msa_name",
    "state_name",
    "epa_region",
]


def build_urls(dates, *, daily=False):
    """Construct AirNow file URLs for `dates`.

    Returns
    -------
    urls, fnames : pandas.Series
    """
    dates = pd.DatetimeIndex(dates)
    if daily:
        dates = dates.floor("D").unique()
    else:  # hourly
        dates = dates.floor("H").unique()

    urls = []
    fnames = []
    print("Building AIRNOW URLs...")
    base_url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"  # TODO: other S3 servers?
    for dt in dates:
        if daily:
            fname = "daily_data.dat"
        else:
            fname = dt.strftime(r"HourlyData_%Y%m%d%H.dat")
        # 2017/20170131/HourlyData_2017012408.dat
        url = base_url + dt.strftime(r"%Y/%Y%m%d/") + fname
        urls.append(url)
        fnames.append(fname)
    # https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2017/20170108/HourlyData_2016121506.dat
    # or https://files.airnowtech.org/?prefix=airnow/2017/20170108/

    # Note: files needed for comparison
    urls = pd.Series(urls, index=None)
    fnames = pd.Series(fnames, index=None)
    return urls, fnames


def read_csv(fn):
    """Short summary.

    Parameters
    ----------
    fn : string
        file name to read

    Returns
    -------
    type
        Description of returned object.

    """
    hourly_cols = [
        "date",
        "time",
        "siteid",
        "site",
        "utcoffset",
        "variable",
        "units",
        "obs",
        "source",
    ]
    daily_cols = ["date", "siteid", "site", "variable", "units", "obs", "hours", "source"]
    try:
        dft = pd.read_csv(
            fn,
            delimiter="|",
            header=None,
            error_bad_lines=False,
            warn_bad_lines=True,
            encoding="ISO-8859-1",
        )  # TODO: `error_bad_lines` is deprecated from v1.3
    except Exception:
        dft = pd.DataFrame(columns=hourly_cols)
        # TODO: warning message or error instead?
        # TODO: or hourly/daily option (to return proper empty df)?

    # Assign column names
    ncols = dft.columns.size
    daily = False
    if ncols == len(hourly_cols):
        dft.columns = hourly_cols
    elif ncols == len(hourly_cols) - 1:  # daily data
        daily = True
        dft.columns = daily_cols
    else:
        raise Exception(f"unexpected number of columns: {ncols}")

    dft["obs"] = dft.obs.astype(float)
    # ^ TODO: could use smaller float type, provided precision is low
    dft["siteid"] = dft.siteid.str.zfill(9)
    # ^ TODO: does nothing; and some site IDs are longer (12) or start with letters
    if not daily:
        dft["utcoffset"] = dft.utcoffset.astype(int)  # FIXME: some sites have fractional UTC offset

    return dft


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


def aggregate_files(dates=dates, *, download=False, n_procs=1, daily=False):
    """Short summary.

    Parameters
    ----------
    dates : array-like of datetime-like
        Passed to :func:`build_urls`.
    download : bool, optional
        Whether to first download the AirNow files to the local directory
        before loading.
    n_procs : int
        For Dask.

    Returns
    -------
    pandas.DataFrame
        Of the combined AirNow hourly files.
    """
    import dask
    import dask.dataframe as dd

    print("Aggregating AIRNOW files...")

    urls, fnames = build_urls(dates, daily=daily)
    if download:
        for url, fname in zip(urls, fnames):
            retrieve(url, fname)
        dfs = [dask.delayed(read_csv)(f) for f in fnames]
    else:
        dfs = [dask.delayed(read_csv)(f) for f in urls]
    dff = dd.from_delayed(dfs)
    df = dff.compute(num_workers=n_procs).reset_index()

    # Datetime conversion
    if daily:
        df["time"] = pd.to_datetime(df.date, format=r"%m/%d/%y", exact=True)
    else:
        df["time"] = pd.to_datetime(
            df.date + " " + df.time, format=r"%m/%d/%y %H:%M", exact=True
        )  # TODO: move to read_csv? (and some of this other stuff too?)
        df["time_local"] = df.time + pd.to_timedelta(df.utcoffset, unit="H")
    df.drop(["date"], axis=1, inplace=True)

    print("    Adding in Meta-data")
    df = get_station_locations(df)
    if daily:
        df = df[[col for col in savecols if col not in {"time_local", "utcoffset"}]]
    else:
        df = df[savecols]
    df.drop_duplicates(inplace=True)

    df = filter_bad_values(df)

    return df.reset_index(drop=True)


def add_data(dates, *, download=False, wide_fmt=True, n_procs=1, daily=False):
    """Retrieve and load AirNow data as a DataFrame.

    Note: to obtain full hourly data you must pass all desired hours
    in `dates`.

    Parameters
    ----------
    dates : array-like of datetime-like
        Passed to :func:`build_urls`.
    download : bool, optional
        Whether to first download the AirNow files to the local directory.
    wide_fmt : bool
    n_procs : int
        For Dask.
    daily : bool
        Whether to get daily data only
        (only unique days in `dates` will be used).

        Info: https://files.airnowtech.org/airnow/docs/DailyDataFactSheet.pdf

        Note: ``daily_data_v2.dat`` (includes AQI) is not available for all times,
        so we use ``daily_data.dat``.

    Returns
    -------
    pandas.DataFrame
    """
    from ..util import long_to_wide

    df = aggregate_files(dates=dates, download=download, n_procs=n_procs, daily=daily)
    if wide_fmt:
        df = (
            long_to_wide(df)
            .drop_duplicates(subset=["time", "latitude", "longitude", "siteid"])
            .reset_index(drop=True)
        )
        # TODO: shouldn't be any such dups (test)

    return df


def filter_bad_values(df, *, max=3000):
    """Mark ``obs`` values less than 0 or greater than `max` as NaN.

    Parameters
    ----------
    max : int

    Returns
    -------
    pandas.DataFrame
    """
    from numpy import NaN

    df.loc[(df.obs > max) | (df.obs < 0), "obs"] = NaN
    return df  # TODO: dropna here (since it is called `filter_bad_values`)?


def daterange(**kwargs):
    """Short summary.

    Parameters
    ----------
    begin : type
        Description of parameter `begin` (the default is '').
    end : type
        Description of parameter `end` (the default is '').

    Returns
    -------
    type
        Description of returned object.

    """
    return pd.date_range(**kwargs)


def get_station_locations(df):  # TODO: better name might be `add_station_locations`
    """Short summary.

    Returns
    -------
    type
        Description of returned object.

    """
    from .epa_util import read_monitor_file

    monitor_df = read_monitor_file(airnow=True)
    df = df.merge(monitor_df.drop_duplicates(), on="siteid", how="left", copy=False)
    # TODO: maybe eliminate need for drop dup here

    return df


def get_station_locations_remerge(df):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.

    Returns
    -------
    type
        Description of returned object.

    """
    df = pd.merge(df, monitor_df.drop(["Latitude", "Longitude"], axis=1), on="siteid")  # ,
    # how='left')
    return df
