"""Time parsing utilities."""

import numpy as np
import pandas as pd


def parse_ioapi_times(yyyymmdd, hhmmss):
    """
    Vectorized parsing of IOAPI (CMAQ/CAMx) TFLAG dates and times.

    Parameters
    ----------
    yyyymmdd : np.ndarray or array-like
        Array of dates in YYYYDDD format (Julian day).
    hhmmss : np.ndarray or array-like
        Array of times in HHMMSS format.

    Returns
    -------
    np.ndarray
        Array of datetime64[ns] objects.

    Examples
    --------
    >>> parse_ioapi_times([2023001], [120000])
    array(['2023-01-01T12:00:00.000000000'], dtype='datetime64[ns]')
    """
    y = np.asanyarray(yyyymmdd)
    h = np.asanyarray(hhmmss)

    # 1. Extract components using math
    years = (y // 1000).astype(int)
    days = (y % 1000).astype(int)

    hours = (h // 10000).astype(int)
    minutes = ((h // 100) % 100).astype(int)
    seconds = (h % 100).astype(int)

    # 2. Convert to nanoseconds since epoch
    # We use pandas to handle the variable year starts (leap years)
    unique_years = np.unique(years)
    year_to_start = {
        y_val: pd.to_datetime(str(y_val), format="%Y").to_datetime64() for y_val in unique_years
    }

    year_starts = np.array(
        [year_to_start[y_val] for y_val in years.ravel()], dtype="datetime64[ns]"
    ).reshape(y.shape)

    # 3. Combine using datetime arithmetic
    # Julian day 1 is the first day of the year (0 offset)
    day_offset = (days - 1).astype("timedelta64[D]")
    time_offset = (
        hours.astype("timedelta64[h]")
        + minutes.astype("timedelta64[m]")
        + seconds.astype("timedelta64[s]")
    )

    # Force ns to match project expectations and avoid discrepancies with Pandas 3.0+
    return (year_starts + day_offset + time_offset).astype("datetime64[ns]")


def parse_wrf_times(times_arr):
    """
    Vectorized parsing of WRF/RAQMS character array or string times.

    Parameters
    ----------
    times_arr : np.ndarray or array-like
        Array of times as strings or character arrays.

    Returns
    -------
    np.ndarray
        Array of datetime64[ns] objects.

    Examples
    --------
    >>> parse_wrf_times(["2023-01-01_12:00:00"])
    array(['2023-01-01T12:00:00.000000000'], dtype='datetime64[ns]')
    """
    t = np.asanyarray(times_arr)

    if t.ndim > 1:
        # It's likely a character array (..., DateStrLen)
        last_dim = t.shape[-1]
        if t.dtype.kind == "U":
            # Unicode: join along the last axis
            orig_shape = t.shape
            flat_times = t.reshape(-1, last_dim)
            s = np.array(["".join(row) for row in flat_times])
            s = s.reshape(orig_shape[:-1])
        else:
            # Bytes: use view if C-contiguous
            if t.flags.c_contiguous:
                s = t.view(f"S{last_dim}").squeeze(-1)
            else:
                orig_shape = t.shape
                flat_times = t.reshape(-1, last_dim)
                s = np.array([b"".join(row) for row in flat_times])
                s = s.reshape(orig_shape[:-1])
    else:
        s = t

    # Replace '_' with ' ' for pandas compatibility (WRF format: YYYY-MM-DD_HH:MM:SS)
    if s.dtype.kind in {"S", "a"}:
        s = np.char.replace(s, b"_", b" ")
        s = s.astype(str)
    else:
        s = np.char.replace(s.astype(str), "_", " ")

    # Force ns to match project expectations and avoid discrepancies with Pandas 3.0+
    # We use pd.to_datetime but return a numpy array to be backend-agnostic in apply_ufunc
    res = pd.to_datetime(s.ravel()).values.astype("datetime64[ns]")
    return res.reshape(s.shape)


def parse_yyyymmdd_hhmm(yyyymmdd, hhmm):
    """
    Vectorized parsing of YYYYMMDD and HHMM (or HHMMSS) dates and times.

    Parameters
    ----------
    yyyymmdd : np.ndarray or array-like
        Array of dates in YYYYMMDD format.
    hhmm : np.ndarray or array-like
        Array of times in HHMM or HHMMSS format.

    Returns
    -------
    np.ndarray
        Array of datetime64[ns] objects.

    Examples
    --------
    >>> parse_yyyymmdd_hhmm([20230101], [1200])
    array(['2023-01-01T12:00:00.000000000'], dtype='datetime64[ns]')
    """
    y = np.asanyarray(yyyymmdd)
    h = np.asanyarray(hhmm)

    # Use float for intermediate to handle NaNs if present
    years = (y // 10000).astype(float)
    months = ((y // 100) % 100).astype(float)
    days = (y % 100).astype(float)

    # HHMMSS check: 2400 is the threshold (HHMM max is 2359)
    try:
        # Use nanmax safely for arrays, or standard max for scalars
        h_max = np.nanmax(h) if h.size > 0 else 0
        is_hhmmss = h_max >= 10000
    except (ValueError, TypeError):
        is_hhmmss = False

    if is_hhmmss:
        hours = (h // 10000).astype(float)
        minutes = ((h // 100) % 100).astype(float)
        seconds = (h % 100).astype(float)
    else:
        hours = (h // 100).astype(float)
        minutes = (h % 100).astype(float)
        seconds = np.zeros_like(h, dtype=float)

    df_dict = {
        "year": years.ravel(),
        "month": months.ravel(),
        "day": days.ravel(),
        "hour": hours.ravel(),
        "minute": minutes.ravel(),
        "second": seconds.ravel(),
    }

    # Use coerce to handle any invalid dates produced by math on NaNs
    res = pd.to_datetime(pd.DataFrame(df_dict), errors="coerce")

    # Return with original shape
    # We use .values to return a numpy array from the pandas series
    return res.values.astype("datetime64[ns]").reshape(y.shape)
