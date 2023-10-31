"""AirNow"""

import os
import sys
from datetime import datetime

import pandas as pd

_today_monitor_df = None
_savecols = [
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
_TFinder = None


def build_urls(dates, *, daily=False):
    """Construct AirNow file URLs for `dates`.

    The files are in S3 storage, which can be explored at
    https://files.airnowtech.org/

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
    if sys.version_info < (3, 7):
        base_url = "https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/"
    else:
        base_url = "s3://files.airnowtech.org/airnow/"
    for dt in dates:
        if daily:
            fname = "daily_data.dat"
        else:
            fname = dt.strftime(r"HourlyData_%Y%m%d%H.dat")
        # e.g.
        # https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2017/20170108/HourlyData_2016121506.dat
        url = base_url + dt.strftime(r"%Y/%Y%m%d/") + fname
        urls.append(url)
        fnames.append(fname)

    # Note: files needed for comparison
    urls = pd.Series(urls, index=None)
    fnames = pd.Series(fnames, index=None)

    return urls, fnames


def read_csv(fn):
    """Read an AirNow CSV file.

    Parameters
    ----------
    fn : str
        File to read, passed to :func:`pandas.read_csv`.

    Returns
    -------
    pandas.DataFrame
        AirNow data in long format without site metadata.
        Additional processing done by :func:`aggregate_files` / :func:`add_data`
        not applied.
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
    dft = pd.read_csv(
        fn,
        delimiter="|",
        header=None,
        error_bad_lines=False,
        warn_bad_lines=True,
        encoding="ISO-8859-1",
    )  # TODO: `error_bad_lines` is deprecated from v1.3
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
    url : str
        To be retrieved.
    fname : str
        File path to save to.

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


def aggregate_files(dates, *, download=False, n_procs=1, daily=False, bad_utcoffset="drop"):
    """Load and combine multiple AirNow data files,
    returning a dataframe in long format with site metadata added.

    Parameters
    ----------
    dates : array-like of datetime-like
        Passed to :func:`build_urls`.
    download : bool, optional
        Whether to first download the AirNow files to the local directory
        before loading.
    n_procs : int
        For Dask.
    bad_utcoffset : {'null', 'drop', 'fix', 'leave'}, default: 'drop'
        How to handle bad UTC offsets
        (i.e. rows with UTC offset 0 but abs(longitude) > 20 degrees).
        ``'fix'`` will use ``timezonefinder`` if it is installed.

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
        df = df[[col for col in _savecols if col not in {"time_local", "utcoffset"}]]
    else:
        df = df[_savecols]
    df.drop_duplicates(inplace=True)

    df = filter_bad_values(df, bad_utcoffset=bad_utcoffset)

    return df.reset_index(drop=True)


def add_data(dates, *, download=False, wide_fmt=True, n_procs=1, daily=False, bad_utcoffset="drop"):
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
        Convert from long ('parameter' and 'value' columns)
        to wide format (each parameter gets its own column).
    n_procs : int
        For Dask.
    daily : bool
        Whether to get daily data only
        (only unique days in `dates` will be used).

        Info: https://files.airnowtech.org/airnow/docs/DailyDataFactSheet.pdf

        Note: ``daily_data_v2.dat`` (includes AQI) is not available for all time periods,
        so we use ``daily_data.dat``.
    bad_utcoffset : {'null', 'drop', 'fix', 'leave'}, default: 'drop'
        How to handle bad UTC offsets
        (i.e. rows with UTC offset 0 but abs(longitude) > 20 degrees).
        ``'fix'`` will use ``timezonefinder`` if it is installed.

    Returns
    -------
    pandas.DataFrame
    """
    from ..util import long_to_wide

    df = aggregate_files(
        dates=dates,
        download=download,
        n_procs=n_procs,
        daily=daily,
        bad_utcoffset=bad_utcoffset,
    )
    if wide_fmt:
        df = (
            long_to_wide(df)
            .drop_duplicates(subset=["time", "latitude", "longitude", "siteid"])
            .reset_index(drop=True)
        )
        # TODO: shouldn't be any such dups (test)

    return df


def filter_bad_values(df, *, max=3000, bad_utcoffset="drop"):
    """Mark ``obs`` values less than 0 or greater than `max` as NaN.

    .. note::
       `df` is modified in place.

    Parameters
    ----------
    max : int
    bad_utcoffset : {'null', 'drop', 'fix', 'leave'}, default: 'drop'
        How to handle bad UTC offsets
        (i.e. rows with UTC offset 0 but abs(longitude) > 20 degrees).
        ``'fix'`` will use ``timezonefinder`` if it is installed.

    Returns
    -------
    pandas.DataFrame
    """
    from numpy import NaN

    df.loc[(df.obs > max) | (df.obs < 0), "obs"] = NaN

    # Bad UTC offsets (GH #86)
    if "utcoffset" in df.columns:
        bad_rows = df.query("utcoffset == 0 and abs(longitude) > 20")
        if bad_utcoffset == "null":
            df.loc[bad_rows.index, "utcoffset"] = NaN
        elif bad_utcoffset == "drop":
            df.drop(bad_rows.index, inplace=True)
        elif bad_utcoffset == "fix":
            df.loc[bad_rows.index, "utcoffset"] = bad_rows.apply(
                lambda row: get_utcoffset(row.latitude, row.longitude),
                axis="columns",
            )
        elif bad_utcoffset == "leave":
            pass
        else:
            raise ValueError("`bad_utcoffset` must be one of: 'null', 'drop', 'fix', 'leave'")

    return df  # TODO: dropna here (since it is called `filter_bad_values`)?


def get_utcoffset(lat, lon):
    """Get UTC offset for standard time (hours).

    Will use timezonefinder and pytz if installed.
    Otherwise will guess based on the lon (and warn).

    Parameters
    ----------
    lat, lon : float
        Latitude and longitude of the (single) location.

    Returns
    -------
    float
    """
    import warnings

    import numpy as np

    if not np.isscalar(lat) or not np.isscalar(lon):
        raise TypeError("lat and lon must be scalars")

    try:
        import pytz
        import timezonefinder
    except ImportError:
        warnings.warn(
            "timezonefinder and/or pytz not installed, guessing UTC offset based on longitude"
        )
        do_guess = True
    else:
        do_guess = False

    if do_guess:
        lon_ = (lon + 180) % 360 - 180  # Ensure lon in [-180, 180)
        return round(lon_ / 15, 0)

    else:
        global _TFinder

        if _TFinder is None:
            _TFinder = timezonefinder.TimezoneFinder(in_memory=True)

        finder = _TFinder
        tz_str = finder.timezone_at(lng=lon, lat=lat)
        tz = pytz.timezone(tz_str)
        uo = tz.utcoffset(datetime(2020, 1, 1), is_dst=False).total_seconds() / 3600
        return uo


def get_station_locations(df, *, today=True):
    """Add site metadata to dataframe `df`.

    Parameters
    ----------
    df : pandas.DataFrame
        Data, with ``siteid`` column.
    today : bool
        Use the "today" site metadata file
        (faster, but may not cover all sites in the period).
        Otherwise, use combined site metadata from each date in `df`.

    Returns
    -------
    pandas.DataFrame
        `df` merged with site metadata from
        :func:`monetio.obs.epa_util.read_airnow_monitor_file`.
    """
    from .epa_util import read_airnow_monitor_file

    if today:
        global _today_monitor_df

        if _today_monitor_df is None:
            meta = read_airnow_monitor_file(date=None)
            _today_monitor_df = meta
        else:
            meta = _today_monitor_df
    else:
        dates = sorted(df.time.dt.floor("D").unique())
        meta = (
            pd.concat([read_airnow_monitor_file(date=date) for date in dates])
            .drop_duplicates(subset=["siteid"])
            .reset_index(drop=True)
        )

    df = df.merge(meta, on="siteid", how="left", copy=False)

    return df
