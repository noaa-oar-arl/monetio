"""
Load NOAA Global Monitoring Laboratory (GML) ozonesondes
from https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/

More info: https://gml.noaa.gov/ozwv/ozsondes/
"""
import re
import warnings
from typing import NamedTuple, Optional

import numpy as np
import pandas as pd
import requests

PLACES = [
    "Boulder, Colorado",
    "Hilo, Hawaii",
    "Huntsville, Alabama",
    "Narragansett, Rhode Island",
    "Pago Pago, American Samoa",
    "San Cristobal, Galapagos",
    "South Pole, Antartica",  # note sp
    "Summit, Greenland",
    "Suva, Fiji",
    "Trinidad Head, California",
]


def discover_files(place=None, *, n_threads=3):
    import itertools
    from multiprocessing.pool import ThreadPool

    base = "https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde"

    if place is None:
        places = PLACES
    elif isinstance(place, str):
        places = [place]
    else:
        places = place

    invalid = set(places) - set(PLACES)
    if invalid:
        raise ValueError(f"Invalid place(s): {invalid}. Valid options: {PLACES}.")

    def get_files(place):
        url = f"{base}/{place}/100 Meter Average Files/".replace(" ", "%20")
        print(url)
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = []
        for m in re.finditer(r'href="([a-z0-9_]+\.l100)"', r.text):
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
            data.append((place, t, fn, f"{url}{fn}"))
        if not data:
            warnings.warn(f"No files detected for place {place!r}.")
        return data

    with ThreadPool(processes=min(n_threads, len(places))) as pool:
        data = list(itertools.chain.from_iterable(pool.imap_unordered(get_files, places)))

    df = pd.DataFrame(data, columns=["place", "time", "fn", "url"])

    return df


def add_data(dates, *, place=None, n_procs=1):
    """Retrieve and load GML ozonesonde data as a DataFrame.

    Parameters
    ----------
    dates : sequence of datetime-like
    place : str or sequence of str, optional
        For example 'Boulder, Colorado'.
        If not provided, all places will be used.
    n_procs : int
        For Dask.
    """
    import dask
    import dask.dataframe as dd

    dates = pd.DatetimeIndex(dates)
    dates_min, dates_max = dates.min(), dates.max()

    print("Discovering files...")
    df_urls = discover_files(place=place)
    print(f"Discovered {len(df_urls)} 100-m files.")

    urls = df_urls[df_urls["time"].between(dates_min, dates_max, inclusive="both")]["url"].tolist()

    if not urls:
        raise RuntimeError(f"No files found for dates {dates_min} to {dates_max}, place={place}.")

    print(f"Aggregating {len(urls)} files...")
    dfs = [dask.delayed(read_100m)(f) for f in urls]
    dff = dd.from_delayed(dfs)
    df = dff.compute(num_workers=n_procs).reset_index()

    # Time subset again in case of times in files extending
    df = df[df["time"].between(dates_min, dates_max, inclusive="both")]

    return df


class ColInfo(NamedTuple):
    name: str
    long_name: str
    units: str
    na_val: Optional[str]


COL_INFO_100m = [
    # name, long name, units, na val
    #
    # "Level" (just a counter, should never be nan)
    ColInfo("lev", "level", "", None),
    #
    # "Press"
    ColInfo("press", "radiosonde corrected pressure", "hPa", "9999.9"),
    #
    # "Alt"
    # TODO: not sure about this na val
    ColInfo("altitude", "altitude", "km", "999.999"),
    #
    # "Pottp"
    ColInfo("theta", "potential temperature", "K", "9999.9"),
    #
    # "Temp"
    ColInfo("temp", "radiosonde corrected temperature", "degC", "999.9"),
    #
    # "FtempV"
    ColInfo("ftempv", "frost point temperature (radiosonde)", "degC", "999.9"),
    #
    # "Hum"
    ColInfo("rh", "radiosonde corrected relative humidity", "%", "999"),
    #
    # "Ozone"
    ColInfo("o3_press", "ozone partial pressure", "mPa", "99.90"),
    #
    # "Ozone"
    ColInfo("o3", "ozone mixing ratio", "ppmv", "99.999"),
    #
    # "Ozone"
    # note 1 DU = 0.001 atm-cm
    # TODO: goes up with height so could be ozone below?
    ColInfo("o3_cm", "total ozone", "atm-cm", "99.9990"),
    #
    # "Ptemp"
    ColInfo("ptemp", "pump temperature", "degC", "999.9"),
    #
    # "O3 # DN"
    ColInfo("o3_nd", "ozone number density", "10^11 cm-3", "999.999"),
    #
    # "O3 Res"
    # TODO: goes down with height so could be total ozone above?
    ColInfo("o3_col", "total column ozone above", "DU", "9999"),
    #
    # "O3 Uncert"
    # TODO: uncertainty in which ozone value?
    ColInfo("o3_uncert", "uncertainty in ozone", "%", "99999.000"),
]


def read_100m(fp_or_url):
    """Read a GML ozonesonde 100-m file (``.l100``).

    Notes
    -----
    Close to ICARTT format, but not quite conformant enough to use the ICARTT reader.
    """
    from io import StringIO

    if isinstance(fp_or_url, str) and fp_or_url.startswith(("http://", "https://")):
        r = requests.get(fp_or_url, timeout=10)
        r.raise_for_status()
        text = r.text
    else:
        with open(fp_or_url) as f:
            text = f.read()

    blocks = text.replace("\r", "").split("\n\n")
    assert len(blocks) == 5

    # Metadata
    meta = {}
    todo = blocks[3].splitlines()[::-1]
    blah = ["Background: ", "Flowrate: ", "RH Corr: ", "Sonde Total O3 (SBUV): "]
    while todo:
        line = todo.pop()
        key, val = line.split(":", 1)
        for key_ish in blah:
            if key_ish in val:
                i = val.index(key_ish)
                meta[key.strip()] = val[:i].strip()
                todo.append(val[i:])
                break
        else:
            meta[key.strip()] = val.strip()

    for k, v in meta.items():
        meta[k] = re.sub(r"\s{2,}", " ", v)

    assert list(meta) == [
        "Station",
        "Station Height",
        "Latitude",
        "Longitude",
        "Flight Number",
        "Launch Date",
        "Launch Time",
        "Radiosonde Type",
        "Radiosonde Num",
        "O3 Sonde ID",
        "Background",
        "Flowrate",
        "RH Corr",
        "Sonde Total O3",
        "Sonde Total O3 (SBUV)",
    ]

    assert len(blocks[4].splitlines()[2].split()) == len(COL_INFO_100m) == 14

    names = [c.name for c in COL_INFO_100m]
    dtype = {c.name: float for c in COL_INFO_100m}
    dtype["lev"] = int
    na_values = {c.name: c.na_val for c in COL_INFO_100m if c.na_val is not None}

    df = pd.read_csv(
        StringIO(blocks[4]),
        skiprows=2,
        header=None,
        delimiter=r"\s+",
        names=names,
        dtype=dtype,
        na_values=na_values,
    )

    # This is close to "Pottp" but not exactly the same
    theta_calc = (df.temp + 273.15) * (df.press / 1000) ** (-0.286)  # noqa: F841

    time = pd.Timestamp(f"{meta['Launch Date']} {meta['Launch Time']}")

    df["time"] = time.tz_localize(None)
    df["latitude"] = float(meta["Latitude"])
    df["longitude"] = float(meta["Longitude"])

    if hasattr(df, "attrs"):
        df.attrs["ds_attrs"] = meta
        df.attrs["var_attrs"] = {
            c.name: {
                "long_name": c.long_name,
                "units": c.units,
            }
            for c in COL_INFO_100m
        }

    return df
