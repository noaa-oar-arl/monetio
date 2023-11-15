"""AirNow"""

import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

_hourly_cols = [
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
_daily_cols = [
    "date",
    "siteid",
    "site",
    "variable",
    "units",
    "obs",
    "hours",
    "source",
]
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
_today_monitor_df = None
_TFinder = None


def build_urls(dates, *, daily=False):
    """Construct AirNow file URLs for `dates`.

    The files are in S3 storage, which can be explored at
    https://files.airnowtech.org/

    Parameters
    ----------
    dates : array-like of datetime-like
        Datetimes to load.
        If ``daily=True``, all unique days in `dates`.
        Otherwise, all unique hours in `dates`.

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
    print("Building AirNow URLs...")
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


def read_csv(fn, *, daily=None):
    """Read an AirNow CSV file.

    Parameters
    ----------
    fn
        File to read, passed to :func:`pandas.read_csv`.
    daily : bool, optional
        Is this is a daily (``True``) or hourly (``False``) file?
        By default, attempt to determine based on the file name.

    Returns
    -------
    pandas.DataFrame
        AirNow data in long format without site metadata.
        Additional processing done by :func:`aggregate_files` / :func:`add_data`
        not applied.
    """
    from monetio.util import _get_pandas_version

    pd_ver = _get_pandas_version()

    if daily is None:
        if isinstance(fn, Path):
            fn_str = fn.as_posix()
        else:
            fn_str = str(fn)

        if fn_str.endswith("daily_data.dat"):
            daily = True
        elif re.search(r"HourlyData_[0-9]{10}.dat", fn_str) is not None:
            daily = False
        else:
            raise ValueError("Could not determine if file is daily or hourly")

    dtype = {"siteid": str, "obs": float}
    if daily:
        names = _daily_cols
    else:
        names = _hourly_cols
        dtype.update(utcoffset=float)

    if pd_ver < (1, 3):
        on_bad = dict(
            error_bad_lines=False,
            warn_bad_lines=True,
        )
    else:
        on_bad = dict(on_bad_lines="warn")

    df = pd.read_csv(
        fn,
        delimiter="|",
        header=None,
        names=names,
        parse_dates=False,
        dtype=dtype,
        encoding="ISO-8859-1",
        **on_bad,
    )

    # TODO: pandas v2 path using `date_format`?

    if daily:
        df["time"] = pd.to_datetime(df["date"], format=r"%m/%d/%y", exact=True)
    else:
        df["time"] = pd.to_datetime(
            df["date"] + " " + df["time"], format=r"%m/%d/%y %H:%M", exact=True
        )
    df = df.drop(columns=["date"])

    return df


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


def aggregate_files(
    dates,
    *,
    download=False,
    n_procs=1,
    daily=False,
    bad_utcoffset="drop",
    today_meta=True,
):
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
    daily : bool
        Daily or hourly (default) data?
    bad_utcoffset : {'null', 'drop', 'fix', 'leave'}, default: 'drop'
        How to handle bad UTC offsets
        (i.e. rows with UTC offset 0 but abs(longitude) > 20 degrees).
        ``'fix'`` will use ``timezonefinder`` if it is installed.
    today_meta : bool
        Whether to use the "today" site metadata file
        (faster, but may not cover all sites in the period).
        Otherwise, use combined site metadata from each unique date.

    Returns
    -------
    pandas.DataFrame
        Of the combined AirNow hourly files.
    """
    import dask
    import dask.dataframe as dd

    urls, fnames = build_urls(dates, daily=daily)
    if download:
        print("Downloading AirNow data files...")
        for url, fname in zip(urls, fnames):
            retrieve(url, fname)
        dfs = [dask.delayed(read_csv)(f) for f in fnames]
    else:
        dfs = [dask.delayed(read_csv)(f) for f in urls]
    df_lazy = dd.from_delayed(dfs)
    print("Reading AirNow data files...")
    df = df_lazy.compute(num_workers=n_procs).reset_index(drop=True)

    # It seems that sometimes there are some duplicate rows
    df = df.drop_duplicates().reset_index(drop=True)

    # Add local standard time column
    if not daily:
        df["time_local"] = df["time"] + pd.to_timedelta(df["utcoffset"], unit="H")

    print("Adding in site metadata...")
    df = get_station_locations(df, today=today_meta, n_procs=n_procs, merge=True)
    if daily:
        df = df[[col for col in _savecols if col not in {"time_local", "utcoffset"}]]
    else:
        df = df[_savecols]

    df = filter_bad_values(df, bad_utcoffset=bad_utcoffset)

    return df.reset_index(drop=True)


def add_data(
    dates,
    *,
    download=False,
    wide_fmt=True,
    n_procs=1,
    daily=False,
    bad_utcoffset="drop",
    today_meta=True,
):
    """Retrieve and load AirNow data as a DataFrame.

    Note: to obtain full hourly data you must pass all desired hours
    in `dates`.

    Parameters
    ----------
    dates : array-like of datetime-like
        Datetimes corresponding to the desired unique days or hours to load.
        A continuous period can be created with :func:`pandas.date_range`.
        Passed to :func:`build_urls`.
    download : bool, optional
        Whether to first download the AirNow files to the local directory.
    wide_fmt : bool
        Convert from long ('parameter' and 'value' columns)
        to wide format (each parameter gets its own column).
    n_procs : int
        For Dask.
    daily : bool
        Whether to get daily data
        (only unique days in `dates` will be used).
        Default: false (hourly data).

        Info: https://files.airnowtech.org/airnow/docs/DailyDataFactSheet.pdf

        Note: ``daily_data_v2.dat`` (includes AQI) is not available for all time periods,
        so we use ``daily_data.dat``.
    bad_utcoffset : {'null', 'drop', 'fix', 'leave'}, default: 'drop'
        How to handle bad UTC offsets
        (i.e. rows with UTC offset 0 but abs(longitude) > 20 degrees).
        ``'fix'`` will use ``timezonefinder`` if it is installed.
    today_meta : bool
        Whether to use the "today" site metadata file
        (faster, but may not cover all sites in the period).
        Otherwise, use combined site metadata from each unique date.

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
        today_meta=today_meta,
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
            df = df.drop(index=bad_rows.index)
        elif bad_utcoffset == "fix":
            df.loc[bad_rows.index, "utcoffset"] = bad_rows.apply(
                lambda row: get_utcoffset(row.latitude, row.longitude),
                axis="columns",
            )
        elif bad_utcoffset == "leave":
            pass
        else:
            raise ValueError("`bad_utcoffset` must be one of: 'null', 'drop', 'fix', 'leave'")

    return df  # TODO: optionally dropna here (since it is called `filter_bad_values`)?


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


def get_station_locations(df, *, today=True, n_procs=1, merge=True):
    """Add site metadata to dataframe `df`.

    Parameters
    ----------
    df : pandas.DataFrame
        Data, with ``siteid`` column.
    today : bool
        Use the "today" site metadata file
        (faster, but may not cover all sites in the period).
        Otherwise, use combined site metadata from each date in `df`.
    n_procs : int
        For Dask.
        Used if `today` is false and `df` has multiple dates (unique days) and `n_procs` > 1.
    merge : bool
        Whether to merge the site metadata into `df` (default),
        or return just the site metadata.

    Returns
    -------
    pandas.DataFrame
        `df` merged with site metadata from
        :func:`monetio.obs.epa_util.read_airnow_monitor_file`.
        OR, if `merge` is false, just the site metadata.
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
        dates = sorted(pd.Timestamp(d) for d in df.time.dt.floor("D").unique())
        if len(dates) > 1 and n_procs > 1:
            import dask
            import dask.dataframe as dd

            dfs = [dask.delayed(read_airnow_monitor_file)(date=date) for date in dates]
            df_lazy = dd.from_delayed(dfs)
            meta = df_lazy.compute(num_workers=n_procs).reset_index(drop=True)
        else:
            meta = pd.concat(
                [read_airnow_monitor_file(date=date) for date in dates], ignore_index=True
            )

        # A large percentage of day-to-day duplicates are expected
        meta = meta.drop_duplicates(subset=["siteid"]).reset_index(drop=True)

    if merge:
        return df.merge(meta, on="siteid", how="left", copy=False)
    else:
        return meta
