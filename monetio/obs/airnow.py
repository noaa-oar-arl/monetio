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


def build_urls(dates):
    """Short summary.

    Returns
    -------
    helper function to build urls

    """

    furls = []
    fnames = []
    print("Building AIRNOW URLs...")
    # 2017/20170131/HourlyData_2017012408.dat
    url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"  # TODO: other S3 servers?
    for i in dates:
        f = url + i.strftime("%Y/%Y%m%d/HourlyData_%Y%m%d%H.dat")
        fname = i.strftime("HourlyData_%Y%m%d%H.dat")
        furls.append(f)
        fnames.append(fname)
    # https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2017/20170108/HourlyData_2016121506.dat

    # files needed for comparison
    url = pd.Series(furls, index=None)
    fnames = pd.Series(fnames, index=None)
    return url, fnames


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
    try:
        dft = pd.read_csv(
            fn, delimiter="|", header=None, error_bad_lines=False, encoding="ISO-8859-1"
        )
        cols = ["date", "time", "siteid", "site", "utcoffset", "variable", "units", "obs", "source"]
        dft.columns = cols
    except Exception:
        cols = ["date", "time", "siteid", "site", "utcoffset", "variable", "units", "obs", "source"]
        dft = pd.DataFrame(columns=cols)
    dft["obs"] = dft.obs.astype(
        float
    )  # TODO: could use smaller float type, provided precision is low
    dft["siteid"] = dft.siteid.str.zfill(
        9
    )  # TODO: does nothing; and some site IDs are longer (12) or start with letters
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


def aggregate_files(dates=dates, download=False, n_procs=1):
    """Short summary.

    Parameters
    ----------
    download : type
        Description of parameter `download` (the default is False).

    Returns
    -------
    type
        Description of returned object.

    """
    import dask
    import dask.dataframe as dd

    print("Aggregating AIRNOW files...")
    urls, fnames = build_urls(dates)
    if download:
        for url, fname in zip(urls, fnames):
            retrieve(url, fname)
        dfs = [dask.delayed(read_csv)(f) for f in fnames]
    else:
        dfs = [dask.delayed(read_csv)(f) for f in urls]
    dff = dd.from_delayed(dfs)
    df = dff.compute(num_workers=n_procs)
    df["time"] = pd.to_datetime(
        df.date + " " + df.time, format="%m/%d/%y %H:%M", exact=True
    )  # TODO: move to read_csv
    df.drop(["date"], axis=1, inplace=True)
    df["time_local"] = df.time + pd.to_timedelta(df.utcoffset, unit="H")
    print("    Adding in Meta-data")
    df = get_station_locations(df)  # FIXME: adds a bunch of extra rows
    df = df[savecols]
    df.drop_duplicates(inplace=True)
    df = filter_bad_values(df)
    return df


def add_data(dates, download=False, wide_fmt=True, n_procs=1):
    """Short summary.

    Parameters
    ----------
    dates : type
        Description of parameter `dates`.
    download : type
        Description of parameter `download` (the default is False).

    Returns
    -------
    type
        Description of returned object.

    """
    from ..util import long_to_wide

    df = aggregate_files(dates=dates, download=download, n_procs=n_procs)
    if wide_fmt:
        df = long_to_wide(df)
        return df.drop_duplicates(subset=["time", "latitude", "longitude", "siteid"])
    else:
        return df
    return df


def filter_bad_values(df):
    """Short summary.

    Returns
    -------
    type
        Description of returned object.

    """
    from numpy import NaN

    df.loc[(df.obs > 3000) | (df.obs < 0), "obs"] = NaN
    return df


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
    df = pd.merge(df, monitor_df, on="siteid")  # , how='left')
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
