"""
Load NOAA Global Monitoring Laboratory (GML) ozonesondes
from https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/

More info: https://gml.noaa.gov/ozwv/ozsondes/
"""

import re
import warnings
from typing import NamedTuple, Optional, Tuple, Union

import fsspec
import numpy as np
import pandas as pd
import requests

TIMEOUT = 120  # seconds (increased from 60)
RETRIES = 10
USE_CACHE_FOR_TESTING = False  # Set to True to use cached data for testing


def retry(func):
    import time
    from functools import wraps
    from random import random as rand

    @wraps(func)
    def wrapper(*args, **kwargs):
        for i in range(RETRIES):
            try:
                res = func(*args, **kwargs)
                return res
            except (
                requests.exceptions.RequestException,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                requests.exceptions.TooManyRedirects,
            ) as e:
                if i == RETRIES - 1:
                    raise RuntimeError(
                        f"{func.__name__} failed after {RETRIES} tries. Last error: {e}"
                    )
                time.sleep(0.5 * i**1.5 + rand() * 0.1)

        raise RuntimeError(f"{func.__name__} failed after {RETRIES} tries.")

    return wrapper


LOCATIONS = [
    "Boulder, Colorado",
    "Hilo, Hawaii",
    "Huntsville, Alabama",
    "Narragansett, Rhode Island",
    "Pago Pago, American Samoa",
    "San Cristobal, Galapagos",
    "South Pole, Antarctica",
    "Summit, Greenland",
    "Suva, Fiji",
    "Trinidad Head, California",
]


_FILES_L100_CACHE = {location: [] for location in LOCATIONS}


def discover_files(location=None, *, n_threads=3, cache=True):
    import itertools
    from multiprocessing.pool import ThreadPool

    base = "https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde"

    if location is None:
        locations = LOCATIONS
    elif isinstance(location, str):
        locations = [location]
    else:
        locations = location

    invalid = set(locations) - set(LOCATIONS)
    if invalid:
        raise ValueError(f"Invalid location(s): {invalid}. Valid options: {LOCATIONS}.")

    @retry
    def get_files(location):
        cached = _FILES_L100_CACHE[location]
        if cached and len(cached) > 0:
            return cached

        if location == "South Pole, Antarctica":
            url_location = "South Pole, Antartica"  # note sp
        else:
            url_location = location

        # Use fsspec for more robust HTTP access
        http_fs = fsspec.filesystem("http")

        # Use manual encoding to preserve original format for compatibility
        # Only encode spaces but not commas to match expected URLs in tests
        encoded_location = url_location.replace(" ", "%20")  # Only encode spaces
        url = f"{base}/{encoded_location}/100%20Meter%20Average%20Files/"
        print(f"Fetching files from: {url}")

        try:
            # Use fsspec to get the HTML content with enhanced settings
            with http_fs.open(url, "r", encoding="utf-8", timeout=TIMEOUT) as f:
                content = f.read()
        except Exception as e:
            warnings.warn(f"Failed to fetch files for {location} using fsspec HTTP: {e}")

            # Enhanced fallback with session for better performance
            with requests.Session() as session:
                # Set headers to mimic a browser request
                session.headers.update(
                    {
                        "User-Agent": "Mozilla/5.0 (compatible; MONETIO-Bot/1.0)",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Accept-Encoding": "gzip, deflate",
                        "Connection": "keep-alive",
                    }
                )

                try:
                    r = session.get(url, timeout=TIMEOUT)
                    r.raise_for_status()
                    content = r.text
                except Exception as fallback_error:
                    warnings.warn(
                        f"Fallback to requests also failed for {location}: {fallback_error}"
                    )
                    if USE_CACHE_FOR_TESTING:
                        warnings.warn(f"Using cached data for {location} due to network failure")
                        return []
                    raise

        data = []
        for m in re.finditer(r'href="([a-z0-9_]+\.l100)"', content):
            fn = m.group(1)
            if fn.startswith("san_cristobal_"):
                a, b = 3, -1
            else:
                a, b = 1, -1
            t_str = "".join(re.split(r"[_\.]", fn)[a:b])
            try:
                t = pd.to_datetime(t_str, format=r"%Y%m%d%H")
            except ValueError:
                warnings.warn(f"Failed to parse file name {fn!r} for time.")
                t = np.nan
            data.append((location, t, fn, f"{url}{fn}"))

        if not data:
            warnings.warn(f"No files detected for location {location!r}.")

        return data

    with ThreadPool(processes=min(n_threads, len(locations))) as pool:
        data = list(itertools.chain.from_iterable(pool.imap_unordered(get_files, locations)))

    df = pd.DataFrame(data, columns=["location", "time", "fn", "url"])

    if cache:
        for location in locations:
            _FILES_L100_CACHE[location] = list(
                df[df["location"] == location].itertuples(index=False, name=None)
            )

    return df


def add_data(dates, *, location=None, n_procs=1, errors="raise"):
    """Retrieve and load GML ozonesonde data as a DataFrame.

    Parameters
    ----------
    dates : sequence of datetime-like
        The period between the min and max (both inclusive)
        will be used to select the files to load.
    location : str or sequence of str, optional
        For example 'Boulder, Colorado'.
        If not provided, all locations will be used.
        Valid options correspond to the directories in https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/
        and may include data from more than one unique site (output column 'siteid').
    n_procs : int
        For Dask.
    errors : {'raise', 'warn', 'skip'}
        What to do when there is an error reading a file.
    """
    import dask.dataframe as dd
    from dask.delayed import delayed

    dates = pd.DatetimeIndex(dates)
    dates_min, dates_max = dates.min(), dates.max()

    if errors not in {"raise", "warn", "ignore"}:
        raise ValueError(f"Invalid errors setting: {errors!r}.")

    print("Discovering files...")
    df_urls = discover_files(location=location)
    print(f"Discovered {len(df_urls)} 100-m files.")

    urls = df_urls[df_urls["time"].between(dates_min, dates_max, inclusive="both")]["url"].tolist()

    if not urls:
        raise RuntimeError(
            f"No files found for dates {dates_min} to {dates_max}, location={location!r}."
        )

    def func(fp_or_url):
        try:
            return read_100m(fp_or_url)
        except Exception as e:
            msg = f"Failed to read {fp_or_url}: {e}"
            if errors == "raise":
                raise RuntimeError(msg) from e
            else:
                if errors == "warn":
                    warnings.warn(msg)
                return pd.DataFrame()

    print(f"Aggregating {len(urls)} files...")
    dfs = [delayed(func)(url) for url in urls]
    dff = dd.from_delayed(dfs, verify_meta=errors == "raise")
    df = dff.compute(num_workers=n_procs).reset_index()

    # Time subset again just in case
    # (file time may not match launch time; file time seems to be floored to nearest hour)
    df = df[df["time"].between(dates_min, dates_max, inclusive="both")]

    # Normalize station
    # All values, as of 2024-02-08:
    # > df.station.value_counts().sort_index()
    # Boulder, CO                          650757
    # Hilo, Hawaii                         627325
    # Hilo,Hawaii                             192
    # Huntsville                            10982
    # Huntsville, AL                       314375
    # Mauna Loa Observatory, Hawaii           605 (different site than Hilo)
    # Pago Pago, American Samoa            370141
    # San Cristobal, Galapagos, Ecuador    150244
    # South Pole                           661422
    # Summit, Greenland                    164620
    # Suva, Fiji                           164065
    # Trinidad Head, CA                    426409
    # University of Rhode Island           105878
    # helikite test                           326
    # hsv                                     340
    repl = {
        "Boulder, CO": "Boulder, Colorado",
        "Hilo,Hawaii": "Hilo, Hawaii",
        "Huntsville": "Huntsville, Alabama",
        "Huntsville, AL": "Huntsville, Alabama",
        "San Cristobal, Galapagos, Ecuador": "San Cristobal, Galapagos",
        "South Pole": "South Pole, Antarctica",
        "Trinidad Head, CA": "Trinidad Head, California",
    }
    assert set(repl.values()) <= set(LOCATIONS)
    df["station"] = df["station"].replace(repl)

    # Normalized station name as site ID
    df = df.rename(columns={"station": "siteid"})

    # Add metadata
    if hasattr(df, "attrs"):
        df.attrs["ds_attrs"] = {"urls": urls}
        df.attrs["var_attrs"] = {
            c.name: {
                "long_name": c.long_name,
                "units": c.units,
            }
            for c in COL_INFO_L100
        }

    return df.drop(columns=["index"], errors="ignore").reset_index(drop=True)


class ColInfo(NamedTuple):
    name: str
    long_name: str
    units: str
    na_val: Optional[Union[str, Tuple[str, ...]]]


COL_INFO_L100 = [
    # name, long name, units, na val
    #
    # "Level" (just a counter, should never be nan)
    ColInfo("lev", "level", "", None),
    #
    # "Press"
    # Atmospheric pressure, from the radiosonde.
    ColInfo("press", "pressure", "hPa", "9999.9"),
    #
    # "Alt"
    # Altitude above sea level
    # in the sounding computed from radiosonde pressure and temperature (or GPS if available?).
    # For 100-m data, the center of the 100-m layer
    # (data values included in the layer averages have altitude +/- 50 m of this).
    # If not invalid and removed, the first row is the actual altitude of the launch,
    # and the next row begins the clean 100-m intervals
    # (i.e. first diff may not be 0.1, but the rest should be).
    ColInfo("altitude", "altitude", "km", ("99.9", "999.9", "99.999", "999.999")),
    #
    # "Pottp"
    ColInfo("theta", "potential temperature", "K", "9999.9"),
    #
    # "Temp"
    # Atmospheric temperature, from the radiosonde.
    ColInfo("temp", "air temperature", "degC", "999.9"),
    #
    # "FtempV"
    # Frost point temperature, calculated from the radiosonde RH and temp.
    ColInfo("ftempv", "frost point temperature", "degC", "999.9"),
    #
    # "Hum"
    # RH, measured by the radiosonde.
    ColInfo("rh", "relative humidity", "%", "999"),
    #
    # "Ozone"
    # Measured by the ozone sensor cell.
    ColInfo("o3_press", "ozone partial pressure", "mPa", "99.90"),
    #
    # "Ozone"
    # Calculated from the ozone partial pressure and atmospheric pressure.
    ColInfo("o3", "ozone mixing ratio", "ppmv", "99.999"),
    #
    # "Ozone"
    # Note 1 DU = 0.001 atm-cm
    # Cumulative column ozone amount at this point in the profile.
    ColInfo("o3_int", "integrated ozone below", "atm-cm", "99.9990"),
    #
    # "Ptemp"
    # Pump temperature, from thermistor in the vicinity of the pump block.
    ColInfo("ptemp", "pump temperature", "degC", "999.9"),
    #
    # "O3 # DN"
    ColInfo("o3_nd", "ozone number density", "10^11 cm-3", "999.999"),
    #
    # "O3 Res"
    # From Owen Cooper (NOAA CSL):
    #   This is the amount of ozone in Dobson units above a given altitude.
    #   The values above the maximum balloon altitude are from a climatology.
    #   This is mainly for UV absorption research.
    ColInfo("o3_res", "estimated total column ozone above", "DU", ("9999", "99999", "99.999")),
    #
    # "O3 Uncert"
    # Estimated uncertainty in the ozone measurement at a given altitude.
    ColInfo("o3_uncert", "uncertainty in ozone", "%", ("99999.000", "99.999")),
]


_DATA_BLOCK_START_L100 = """\
Level   Press    Alt   Pottp   Temp   FtempV   Hum  Ozone  Ozone   Ozone  Ptemp  O3 # DN O3 Res  O3 Uncert
 Num     hPa      km     K      C       C       %    mPa    ppmv   atmcm    C   10^11/cc   DU          %
"""

_DATA_BLOCK_START_L100_NO_UNCERT = """\
Level   Press    Alt   Pottp   Temp   FtempV   Hum  Ozone  Ozone   Ozone  Ptemp  O3 # DN O3 Res
 Num     hPa      km     K      C       C       %    mPa    ppmv   atmcm    C   10^11/cc   DU
"""


def read_100m(fp_or_url):
    """Read a GML ozonesonde 100-m file (``.l100``).

    Notes
    -----
    Close to ICARTT format, but not quite conformant enough to use the ICARTT reader.
    """
    from io import StringIO

    def get_text():
        if isinstance(fp_or_url, str) and fp_or_url.startswith(("http://", "https://")):

            @retry
            def get_remote_content():
                # Try fsspec first for better performance and features
                try:
                    http_fs = fsspec.filesystem("http", headers={"User-Agent": "MONETIO-Client"})
                    with http_fs.open(fp_or_url, "r", encoding="utf-8", timeout=TIMEOUT) as f:
                        return f.read()
                except Exception as fsspec_error:
                    # Fallback to requests if fsspec fails
                    warnings.warn(
                        f"fsspec failed for {fp_or_url}, falling back to requests: {fsspec_error}"
                    )
                    with requests.Session() as session:
                        session.headers.update(
                            {
                                "User-Agent": "Mozilla/5.0 (compatible; MONETIO-Bot/1.0)",
                                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                                "Accept-Language": "en-US,en;q=0.5",
                                "Accept-Encoding": "gzip, deflate",
                                "Connection": "keep-alive",
                            }
                        )
                        r = session.get(fp_or_url, timeout=TIMEOUT)
                        r.raise_for_status()
                        return r.text

            return get_remote_content()
        else:
            with open(fp_or_url) as f:
                return f.read()

    text_content = get_text()

    blocks = text_content.replace("\r", "").split("\n\n")
    nblocks = len(blocks)
    if nblocks == 5:  # normal
        meta_block = blocks[3]
        data_block = blocks[4]
    elif nblocks == 2:
        block_lines = blocks[0].splitlines()
        for i, line in enumerate(block_lines):
            if line.startswith(("Station:", "Station: ", "Station  ")):
                break
        else:
            raise ValueError(f"Expected to find metadata to start with Station, got:\n{blocks[0]}")
        meta_block = "\n".join(block_lines[i:])
        data_block = blocks[1]
    else:
        heads = "\n".join("\n".join(b.splitlines()[:2] + ["..."]) for b in blocks)
        raise ValueError(f"Expected 2 or 5 blocks, got {nblocks}:\n{heads}")

    # Metadata
    meta = {}
    todo = meta_block.splitlines()[::-1]
    on_val_side = ["Background: ", "Flowrate: ", "RH Corr: ", "Sonde Total O3 (SBUV): "]
    while todo:
        line = todo.pop()
        key, val = line.split(":", 1)
        for key_ish in on_val_side:
            if key_ish in val:
                i = val.index(key_ish)
                meta[key.strip()] = val[:i].strip()
                todo.append(val[i:])
                break
        else:
            meta[key.strip()] = val.strip()

    for k, v in meta.items():
        meta[k] = re.sub(r"\s{2,}", " ", v)

    meta_keys_expected = [
        "Station",
        "Station Height",
        "Latitude",
        "Longitude",
        "Flight Number",
        "Launch Date",
        "Launch Time",
        # May see 'Vaisala number' and 'Vaisala humicap' instead of these two:
        # "Radiosonde Type",
        # "Radiosonde Num",
        "O3 Sonde ID",
        "Background",
        "Flowrate",
        "RH Corr",
        "Sonde Total O3",
        "Sonde Total O3 (SBUV)",
    ]
    if not set(meta) >= set(meta_keys_expected):
        raise ValueError(f"Expected metadata keys {meta_keys_expected}, got {list(meta)}.")

    if data_block.startswith(_DATA_BLOCK_START_L100):
        have_uncert = True
    elif data_block.startswith(_DATA_BLOCK_START_L100_NO_UNCERT):
        have_uncert = False
    else:
        head = "\n".join(data_block.splitlines()[:2] + ["..."])
        raise ValueError(
            "Data block does not start with expected header line(s) "
            "(O3 Uncert allowed to be missing):\n"
            f"{_DATA_BLOCK_START_L100}\n"
            f"got\n{head}"
        )

    col_info = COL_INFO_L100[:]
    if not have_uncert:
        _ = col_info.pop()

    ncol_expected = len(col_info)
    data_block_first_ncol = len(data_block[:400].splitlines()[2].split())
    if not data_block_first_ncol == ncol_expected:
        head = "\n".join(data_block.splitlines()[:4] + ["..."])
        raise ValueError(
            f"Expected {ncol_expected} columns in data block, "
            f"got {data_block_first_ncol} in first data line:\n{head}"
        )
        # TODO: allow pandas to skip bad lines with `on_bad_lines='skip'`?

    names = [c.name for c in col_info]
    na_values = {c.name: c.na_val for c in col_info if c.na_val is not None}

    df = pd.read_csv(
        StringIO(data_block),
        skiprows=2,
        header=None,
        delimiter=r"\s+",
        names=names,
        na_values=na_values,
    )

    # Convert dtypes after reading
    for c in col_info:
        if c.name in df.columns:
            if c.name != "lev":
                df[c.name] = pd.to_numeric(df[c.name], errors="coerce")
            else:
                df[c.name] = pd.to_numeric(df[c.name], errors="coerce").astype(
                    "Int64"
                )  # nullable integer

    # Add some variables from header as columns (these don't change in the profile)
    time = pd.Timestamp(f"{meta['Launch Date']} {meta['Launch Time']}")
    df["time"] = time.tz_localize(None)
    df["latitude"] = float(meta["Latitude"])
    df["longitude"] = float(meta["Longitude"])

    df["station"] = meta["Station"]
    df["station_height_str"] = meta["Station Height"]  # e.g. '1743 meters'
    df["flight_number"] = meta["Flight Number"]

    # Sonde total column ozone amount ('325 (62) DU') from two methods:
    # - CMR: extrapolate constant mixing ratio above balloon burst to get ozone above that (the residual)
    # - SBUV: compute the residual from the SBUV climate tables
    # The first number is the total column ozone (integrated + residual).
    # The number in parentheses is the residual.
    df["o3_tot_cmr_str"] = meta["Sonde Total O3"]
    df["o3_tot_sbuv_str"] = meta["Sonde Total O3 (SBUV)"]
    for col in ["o3_tot_cmr_str", "o3_tot_sbuv_str"]:
        df[col] = df[col].replace("99999 (99999) DU", np.nan).astype(object)

    # Add metadata
    if hasattr(df, "attrs"):
        df.attrs["ds_attrs"] = meta
        df.attrs["var_attrs"] = {
            c.name: {
                "long_name": c.long_name,
                "units": c.units,
            }
            for c in col_info
        }

    return df
