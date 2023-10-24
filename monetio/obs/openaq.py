"""Get v1 (government-only) OpenAQ data from AWS.

https://openaq.org/
https://openaq-fetches.s3.amazonaws.com/index.html
"""
import json

import pandas as pd
from numpy import NaN

_URL_CAP = None  # set to int to limit number of files loaded for testing


def add_data(dates, n_procs=1):
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
    return a.add_data(dates, num_workers=n_procs)


def read_json(fp_or_url, *, verbose=False):
    """Read a JSON file from the OpenAQ S3 bucket, returning dataframe in non-wide format.

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
    averagingPeriod = pd.Series(np.full(len(new), NaN, dtype="timedelta64[ns]"))
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
    """Read a JSON file from the OpenAQ S3 bucket, returning dataframe in non-wide format.

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
    def __init__(self, *, engine="pandas"):
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

    def add_data(self, dates, *, num_workers=1):
        """Get data for `dates`, using `num_workers` Dask workers."""
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
            import random

            urls = random.sample(urls, _URL_CAP)

        func = self.read
        dfs = [dask.delayed(func)(url) for url in urls]
        df_lazy = dd.from_delayed(dfs)
        df = df_lazy.compute(num_workers=num_workers)

        # Ensure consistent units, e.g. ppm for molecules
        self._fix_units(df)
        non_molec = ["pm1", "pm25", "pm4", "pm10", "bc"]
        good = (df[~df.parameter.isin(non_molec)].unit.dropna() == "ppm").all()
        if not good:
            unique_params = sorted(df.parameter.unique())
            molec = [p for p in unique_params if p not in non_molec]
            raise ValueError(f"Expected these species to all be in ppm now: {molec}.")
        good = (df[df.parameter.isin(non_molec)].unit.dropna() == "µg/m³").all()
        if not good:
            raise ValueError(f"Expected these species to all be in µg/m³: {non_molec}.")

        # Pivot to wide format
        df = self._pivot_table(df)

        # Construct site IDs
        df["siteid"] = (
            df.country
            + "_"
            + df.latitude.round(3).astype(str)
            + "N_"
            + df.longitude.round(3).astype(str)
            + "E"
        )

        return df.loc[(df.time >= dates.min()) & (df.time <= dates.max())]

    def _fix_units(self, df):
        """In place, convert units to ppm for molecules."""
        df.loc[df.value <= 0] = NaN
        # For a certain parameter, different site-times may have different units.
        # https://docs.openaq.org/docs/parameters
        # These conversion factors are based on
        # - air average molecular weight: 29 g/mol
        # - air density: 1.2 kg m -3
        # rounded to 3 significant figures.
        fs = {"co": 1160, "o3": 1990, "so2": 2650, "no2": 1900, "ch4": 664, "no": 1240}
        fs["nox"] = fs["no2"]  # Need to make an assumption about NOx MW
        for vn, f in fs.items():
            is_ug = (df.parameter == vn) & (df.unit == "µg/m³")
            df.loc[is_ug, "value"] /= f
            df.loc[is_ug, "unit"] = "ppm"

    def _pivot_table(self, df):
        """Convert to wide format, with one column per parameter."""

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
            "averagingPeriod",
        ]
        if self.engine == "pandas":
            index.remove("attribution")

        # Pivot
        wide = df.pivot_table(
            values="value",
            index=index,
            columns="parameter",
        ).reset_index()

        # Include units in variable names
        wide = wide.rename(
            dict(
                # molec
                co="co_ppm",
                o3="o3_ppm",
                no2="no2_ppm",
                so2="so2_ppm",
                ch4="ch4_ppm",
                no="no_ppm",
                # non-molec
                pm1="pm1_ugm3",
                pm25="pm25_ugm3",
                pm4="pm4_ugm3",
                pm10="pm10_ugm3",
                bc="bc_ugm3",
                #
                nox="nox_ppm",
            ),
            axis="columns",
            errors="ignore",
        )

        return wide
