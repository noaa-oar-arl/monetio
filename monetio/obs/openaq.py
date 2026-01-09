"""Get v1 (government-only) OpenAQ data from AWS.

https://openaq.org/

https://openaq-fetches.s3.amazonaws.com/index.html
"""

import json
import sys
import warnings

import pandas as pd
from numpy import nan

_PY39_PLUS = sys.version_info >= (3, 9)

_URL_CAP_RANDOM_SAMPLE = False  # if false, take from end of list
_URL_CAP = None  # set to int to limit number of files loaded for testing


def add_data(dates, *, n_procs=1, wide_fmt=True):
    """Add OpenAQ data from the OpenAQ S3 bucket.

    https://openaq-fetches.s3.amazonaws.com

    Note that the source files are daily, so requesting a single day or single hour within it
    will take the same amount of time.

    Parameters
    ----------
    dates : pandas.DateTimeIndex or list of datetime objects
        Dates of data to fetch.
    n_procs : int
        For Dask.

    Returns
    -------
    pandas.DataFrame
    """
    a = OPENAQ()
    return a.add_data(dates, num_workers=n_procs, wide_fmt=wide_fmt)


def read_json(fp_or_url, *, verbose=False):
    """Read a JSON file from the OpenAQ S3 bucket, returning dataframe in original long format.

    Parameters
    ----------
    fp_or_url : str or path-like
        File path or URL.

    Returns
    -------
    pandas.DataFrame
    """
    from time import perf_counter

    import numpy as np

    tic = perf_counter()

    df = pd.read_json(fp_or_url, lines=True)

    # "attribution" is complex to deal with, just drop for now
    # Seems like it can be null or a list of attribution dicts with "name" and "url"
    df = df.drop(columns="attribution")

    # Expand nested columns
    # Multiple ways to do this, e.g.
    # - pd.DataFrame(df.date.tolist())
    #   Seems to be fastest for one, works if only one level of nesting
    # - pd.json_normalize(df["date"])
    # - pd.json_normalize(json.loads(df["date"].to_json(orient="records")))
    #   With this method, can apply to multiple columns at once
    df = df.dropna(subset=["coordinates"])
    to_expand = ["date", "averagingPeriod", "coordinates"]
    new = pd.json_normalize(json.loads(df[to_expand].to_json(orient="records")))

    # Convert to time
    # If we just apply `pd.to_datetime`, we get
    # - utc -> datetime64[ns, UTC]
    # - local -> obj (datetime.datetime with tzinfo=tzoffset(None, ...))
    #
    # But we don't need localization, we just want non-localized UTC time and UTC offset.
    #
    # To get the UTC time, e.g.:
    # - pd.to_datetime(new["date.utc"]).dt.tz_localize(None)
    #   These are comparable but this seems slightly faster.
    # - pd.to_datetime(new["date.utc"].str.slice(None, -1))
    #
    # To get UTC offset
    # (we can't subtract the two time arrays since different dtypes), e.g.:
    # - pd.to_timedelta(new["date.local"].str.slice(-6, None)+":00")
    #   Seems to be slightly faster
    # - pd.to_datetime(new["date.local"]).apply(lambda t: t.utcoffset())
    time = pd.to_datetime(new["date.utc"]).dt.tz_localize(None)
    utcoffset = pd.to_timedelta(new["date.local"].str.slice(-6, None) + ":00")
    time_local = time + utcoffset

    # Convert averaging period to timedelta
    value = new["averagingPeriod.value"]
    units = new["averagingPeriod.unit"]
    unique_units = units.dropna().unique()
    averagingPeriod = pd.Series(np.full(len(new), nan, dtype="timedelta64[ns]"))
    for unit in unique_units:
        is_unit = units == unit
        averagingPeriod.loc[is_unit] = pd.to_timedelta(value[is_unit], unit=unit)

    # Apply new columns
    df = df.drop(columns=to_expand).assign(
        time=time,
        time_local=time_local,
        utcoffset=utcoffset,
        latitude=new["coordinates.latitude"],
        longitude=new["coordinates.longitude"],
        averagingPeriod=averagingPeriod,
    )

    if verbose:
        print(f"{perf_counter() - tic:.3f}s")

    return df


def read_json2(fp_or_url, *, verbose=False):
    """Read a JSON file from the OpenAQ S3 bucket, returning dataframe in original long format.

    This provides 'attribution', while :func:`read_json` does not.

    Parameters
    ----------
    fp_or_url : str or path-like
        File path or URL.

    Returns
    -------
    pandas.DataFrame
    """
    import datetime
    from time import perf_counter

    tic = perf_counter()

    if isinstance(fp_or_url, str) and fp_or_url.startswith(("http", "s3")):
        import requests

        if fp_or_url.startswith("s3"):
            fp_or_url = fp_or_url.replace(
                "s3://openaq-fetches/", "https://openaq-fetches.s3.amazonaws.com/"
            )

        r = requests.get(fp_or_url, stream=True, timeout=2)
        r.raise_for_status()
    else:
        raise NotImplementedError

    names = [
        "time",
        "utcoffset",
        "latitude",
        "longitude",
        #
        "parameter",
        "value",
        "unit",
        #
        "averagingPeriod",
        #
        "location",
        "city",
        "country",
        #
        "attribution",
        "sourceName",
        "sourceType",
        "mobile",
    ]
    rows = []
    for line in r.iter_lines():
        if line:
            data = json.loads(line)
            coords = data.get("coordinates")
            if coords is None:
                if verbose:
                    print("Skipping row since no coords:", data)
                continue

            # Time
            time = datetime.datetime.fromisoformat(data["date"]["utc"][:-1])
            time_local_str = data["date"]["local"]
            h = int(time_local_str[-6:-3])
            m = int(time_local_str[-2:])
            utcoffset = datetime.timedelta(hours=h, minutes=m)

            # Averaging period
            ap = data.get("averagingPeriod")
            if ap is not None:
                val = data["averagingPeriod"]["value"]
                unit = data["averagingPeriod"]["unit"]
                averagingPeriod = datetime.timedelta(**{unit: val})
            else:
                averagingPeriod = None

            # Attribution
            attrs = data.get("attribution")
            if attrs is not None:
                attr_names = [a["name"] for a in attrs]
                if verbose:
                    if len(attr_names) > 1:
                        print(f"Taking first of {len(attr_names)}:", attr_names)
                attr_name = attr_names[0]  # Just the (hopefully) primary one

            rows.append(
                (
                    time,
                    utcoffset,
                    data["coordinates"]["latitude"],
                    data["coordinates"]["longitude"],
                    #
                    data["parameter"],
                    data["value"],
                    data["unit"],
                    #
                    averagingPeriod,
                    #
                    data["location"],
                    data["city"],
                    data["country"],
                    #
                    attr_name,
                    data["sourceName"],
                    data["sourceType"],
                    data["mobile"],
                )
            )

    df = pd.DataFrame(rows, columns=names)

    df["time_local"] = df["time"] + df["utcoffset"]

    if verbose:
        print(f"{perf_counter() - tic:.3f}s")

    return df


class OPENAQ:
    NON_MOLEC_PARAMS = [
        "pm1",
        "pm25",
        "pm4",
        "pm10",
        "bc",
    ]
    """Parameters that are not molecules and should be in µg/m³ units."""

    PPM_TO_UGM3 = {
        "o3": 1990,
        "co": 1160,
        "no2": 1900,
        "no": 1240,
        "so2": 2650,
        "ch4": 664,
        "co2": 1820,
    }
    """Conversion factors from ppmv to µg/m³.

    Based on

    - air average molecular weight: 29 g/mol
    - air density: 1.2 kg m -3

    and rounded to 3 significant figures.
    """

    def __init__(self, *, engine="pandas"):
        """
        Parameters
        ----------
        engine : str, optional
            _description_, by default "pandas"

        Raises
        ------
        ValueError
            _description_
        """
        from functools import partial

        import s3fs

        self.fs = s3fs.S3FileSystem(anon=True)
        self.s3bucket = "openaq-fetches/realtime"

        if engine == "pandas":
            self.read = partial(read_json, verbose=False)
        elif engine == "python":
            self.read = partial(read_json2, verbose=False)
        else:
            raise ValueError("engine must be 'pandas' or 'python'.")
        self.engine = engine

    def _get_available_days(self, dates):
        """
        Parameters
        ----------
        dates : datetime-like or list of datetime-like
            ``pd.to_datetime`` will be applied.
        """
        # Get all day folders
        folders = self.fs.ls(self.s3bucket)
        days = [folder.split("/")[2] for folder in folders]
        dates_available = pd.Series(
            pd.to_datetime(days, format=r"%Y-%m-%d", errors="coerce"),
            name="dates",
        )

        # Filter by requested dates
        dates_requested = pd.Series(
            pd.to_datetime(dates).floor(freq="D"),
            name="dates",
        ).drop_duplicates()

        dates_have = pd.merge(dates_available, dates_requested, how="inner")["dates"]
        if dates_have.empty:
            raise ValueError(f"No data available for requested dates: {dates_requested}.")

        return dates_have

    def _get_files_in_day(self, date):
        """
        Parameters
        ----------
        date
            datetime-like object with ``.strftime`` method.
        """
        sdate = date.strftime(r"%Y-%m-%d")
        files = self.fs.ls(f"{self.s3bucket}/{sdate}")
        return files

    def build_urls(self, dates):
        """
        Parameters
        ----------
        dates : datetime-like or list of datetime-like
            ``pd.to_datetime`` will be applied.
        """
        dates_ = self._get_available_days(dates)
        urls = []
        for date in dates_:
            files = self._get_files_in_day(date)
            urls.extend(f"s3://{f}" for f in files)
        return urls

    def add_data(self, dates, *, num_workers=1, wide_fmt=True):
        """Get data for `dates`, using `num_workers` Dask workers.

        Parameters
        ----------
        num_workers : int
            Number of Dask workers to use to read the JSON files.
        wide_fmt : bool
            If True, return data in wide format
            (each parameter gets its own column,
            as opposed to long format with 'parameter', 'value', and 'units' columns).
            Accordingly, convert units to consistent units
            (ppmv for molecules, µg/m³ for others)
            and rename columns to reflect units.
        """
        import hashlib

        import dask
        import dask.dataframe as dd

        dates = pd.to_datetime(dates)
        if isinstance(dates, pd.Timestamp):
            dates = pd.DatetimeIndex([dates])
        dates = dates.sort_values()

        # Get URLs
        urls = self.build_urls(dates)
        print(f"Will load {len(urls)} files.")
        if len(urls) > 0:
            print(urls[0])
        if len(urls) > 2:
            print("...")
        if len(urls) > 1:
            print(urls[-1])

        if _URL_CAP is not None and len(urls) > _URL_CAP:
            if _URL_CAP_RANDOM_SAMPLE:
                import random

                urls = random.sample(urls, _URL_CAP)
            else:
                urls = urls[-_URL_CAP:]

        # Read JSON files
        func = self.read
        dfs = [dask.delayed(func)(url) for url in urls]
        df_lazy = dd.from_delayed(dfs)
        df = df_lazy.compute(num_workers=num_workers)

        # Ensure data within requested time window
        df = df.loc[(df.time >= dates.min()) & (df.time <= dates.max())]

        # Measurements like air comp shouldn't be negative
        non_neg_units = [
            "ng/m3",
            "particles/cm³",
            "ppb",
            "ppm",
            "ugm3",
            "umol/mol",
            "µg/m³",
        ]
        df.loc[df.unit.isin(non_neg_units) & (df.value < 0), "value"] = nan
        # Assume value 0 implies below detection limit

        if wide_fmt:
            # Convert to consistent units for molecules (ppmv)
            # (For a certain parameter, different site-times may have different units.)
            for vn, f in self.PPM_TO_UGM3.items():
                is_ug = (df.parameter == vn) & (df.unit == "µg/m³")
                df.loc[is_ug, "value"] /= f
                df.loc[is_ug, "unit"] = "ppm"

            # Ensure consistent units
            non_molec = self.NON_MOLEC_PARAMS
            good = (df[~df.parameter.isin(non_molec)].unit.dropna() == "ppm").all()
            if not good:
                unique_params = sorted(df.parameter.unique())
                molec = [p for p in unique_params if p not in non_molec]
                raise ValueError(f"Expected these species to all be in ppm now: {molec}.")
            good = (df[df.parameter.isin(non_molec)].unit.dropna() == "µg/m³").all()
            if not good:
                raise ValueError(f"Expected these species to all be in µg/m³: {non_molec}.")

            # Determine averaging periods for each parameter
            aps = {}
            for p, g in df.groupby("parameter"):
                aps[p] = g.averagingPeriod.dropna().unique()
            mult_ap_lines = []
            for p, ap in aps.items():
                if len(ap) > 1:
                    counts = df.averagingPeriod.loc[df.parameter == p].dropna().value_counts()
                    s_counts = ", ".join(f"'{v}' ({n})" for v, n in counts.items())
                    mult_ap_lines.append(f"{p!r}: {s_counts}")
            if mult_ap_lines:
                s_mults = "\n".join(f"- {s}" for s in mult_ap_lines)
                warnings.warn(
                    "Multiple averaging periods for"
                    f"\n{s_mults}"
                    "\nWill select data with averaging period 1H. "
                    "Use wide_fmt=False if you want all data."
                )

            # Pivot to wide format (each parameter gets its own column)
            index = [
                "time",
                "time_local",
                "latitude",
                "longitude",
                "utcoffset",
                "location",
                "city",
                "country",
                "attribution",  # currently only in Python reader
                "sourceName",
                "sourceType",
                "mobile",
                # "averagingPeriod",  # different parameters may have different standard averaging periods
            ]
            if self.engine == "pandas":
                index.remove("attribution")

            # NOTE: 1H is the most common averaging period by far
            # NOTE: seems that some sites have dupe rows with city == "N/A"
            na_locations = ["Wampanoag Laboratory"]
            df = (
                df[
                    (df.averagingPeriod == pd.Timedelta("1H"))
                    & ~(df.location.isin(na_locations) & (df.city == "N/A"))
                ]
                .pivot_table(
                    values="value",
                    index=index,
                    columns="parameter",
                )
                .reset_index()
            )
            df["averagingPeriod"] = pd.Timedelta("1H")  # TODO: could just not include
            df = df.rename(columns={p: f"{p}_ugm3" for p in self.NON_MOLEC_PARAMS}, errors="ignore")
            df = df.rename(columns={p: f"{p}_ppm" for p in self.PPM_TO_UGM3}, errors="ignore")

        # Construct site IDs
        # Sometimes, at a given time, there are multiple measurements at the same lat/lon
        # with different location names.
        # Occasionally, there are rows that appear to actual duplicates
        # (e.g. all same except one col is null in one or something)
        if _PY39_PLUS:

            def do_hash(b):
                return hashlib.sha1(b, usedforsecurity=False).hexdigest()

        else:

            def do_hash(b):
                return hashlib.sha1(b).hexdigest()

        # to_hash = df.latitude.astype(str) + " " + df.longitude.astype(str)
        to_hash = df.location + " " + df.latitude.astype(str) + " " + df.longitude.astype(str)
        df["siteid"] = df.country + "_" + to_hash.str.encode("utf-8").apply(do_hash).str.slice(0, 7)

        return df


# Need to make an assumption about NOx MW
OPENAQ.PPM_TO_UGM3["nox"] = OPENAQ.PPM_TO_UGM3["no2"]
