"""Get AQ data from the OpenAQ v3 REST API.

Visit https://docs.openaq.org/using-the-api/api-key to get an API key
and set environment variable ``OPENAQ_API_KEY`` to use it.

For example, in Bash:

.. code-block:: bash

   export OPENAQ_API_KEY="your_api_key_here"

https://openaq.org/

https://api.openaq.org/docs#/v3
"""

import functools
import logging
import os
import warnings
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import requests

HERE = Path(__file__).parent

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("OPENAQ_API_KEY", None)
if API_KEY is not None:
    API_KEY = API_KEY.strip()
    if len(API_KEY) != 64:
        warnings.warn(f"API key length is {len(API_KEY)}, expected 64")

_PPM_TO_UGM3 = {
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

# NOx assumption
_PPM_TO_UGM3["nox"] = _PPM_TO_UGM3["no2"]

_NON_MOLEC_PARAMS = [
    "pm1",
    "pm25",
    "pm4",
    "pm10",
    "bc",
]
"""Parameters that are not molecules and should be in µg/m³ units."""


def _api_key_warning(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if API_KEY is None:
            warnings.warn(
                "Non-cached requests to the OpenAQ v3 web API will be slow without an API key "
                "or requests will fail (HTTP error 401). "
                "Obtain one (https://docs.openaq.org/using-the-api/api-key) "
                "and set your OPENAQ_API_KEY environment variable.",
                stacklevel=2,
            )
        return func(*args, **kwargs)

    return wrapper


_BASE_URL = "https://api.openaq.org"
_ENDPOINTS = {
    "locations": "/v3/locations",
    "parameters": "/v3/parameters",
    "sensors": "/v3/sensors",
}


def _consume(endpoint, *, params=None, timeout=10, retry=5, limit=500, npages=None):
    """Consume a paginated OpenAQ API endpoint.

    Parameters
    ----------
    endpoint : str
        API endpoint, e.g. ``'/v3/locations'``, ``'/v3/parameters'``, ``'/v3/sensors'``,
        ``'/v3/sensors/<sensor id>/measurements'``.
    params : dict, optional
        Parameters for the GET request to the API.
        Don't pass ``limit``, ``page``, or ``offset`` here, since they are covered
        by the `limit` and `npages` kwargs.
    timeout : float or tuple
        Seconds to wait for the server before giving up. Passed to ``requests.get``.
    retry : int
        Number of times to retry the request if it times out.
    limit : int
        Max number of results per page.
    npages : int, optional
        Number of pages to fetch.
        By default, try to fetch as many as needed to get all results.

    Notes
    -----
    Rate limit info: https://docs.openaq.org/using-the-api/rate-limits
    """
    import time
    from random import random as rand

    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    if not endpoint.startswith("/v3"):
        endpoint = "/v3" + endpoint
    url = _BASE_URL + endpoint

    if params is None:
        params = {}

    if npages is None:
        # Maximize
        # "limit + offset must be <= 100_000"
        # where offset = limit * (page - 1)
        # => limit * page <= 100_000
        # and also page must be <= 6_000
        npages = min(100_000 // limit, 6_000)

    params["limit"] = limit

    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY,
        "User-Agent": "monetio",
    }

    data = []
    for page in range(1, npages + 1):
        params["page"] = page

        tries = 0
        r = None  # Initialize r to avoid unbound variable error
        while tries < retry:
            logger.debug(f"GET {url} params={params}")
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            tries += 1
            if r.status_code == 408:
                logger.info(f"request timed out (try {tries}/{retry})")
                time.sleep(tries + 0.1 * rand())
            elif r.status_code == 429:
                logger.info(f"rate limited (try {tries}/{retry})")
                ratelimit_reset = int(r.headers.get("X-Ratelimit-Reset", 30))
                logger.info(f"sleeping for {ratelimit_reset} s (rate limit reset)")
                time.sleep(ratelimit_reset + 0.1 * rand())
            else:
                break

        if r is None:
            raise requests.RequestException("Request failed after retries")

        r.raise_for_status()

        ratelimit_remaining = int(r.headers["X-Ratelimit-Remaining"])
        logger.info(f"rate limit count remaining: {ratelimit_remaining}")
        if ratelimit_remaining < 1:
            ratelimit_reset = int(r.headers["X-Ratelimit-Reset"])
            logger.info(f"sleeping for {ratelimit_reset} s (rate limit reset)")
            time.sleep(ratelimit_reset + 0.1 * rand())

        this_data = r.json()
        found = (
            this_data["meta"]["found"]
            if this_data and "meta" in this_data and "found" in this_data["meta"]
            else 0
        )
        n = len(this_data["results"])
        logger.info(f"page={page} found={found!r} n={n}")
        if n == 0:
            break
        data.extend(this_data["results"])
        if n < limit:
            logger.info(f"note: results returned ({n}) < limit ({limit})")
            break

    if isinstance(found, str) and found.startswith(">"):
        print(f"warning: some query results not fetched ('found' is {found!r})")
    elif isinstance(found, int) and len(data) < found:
        print(f"warning: some query results not fetched (found={found}, got {len(data)} results)")

    return data


@_api_key_warning
def get_locations(**kwargs):
    """Get available site info (including site IDs) from OpenAQ v3 API.

    kwargs are passed to :func:`_consume`.

    https://api.openaq.org/docs#/v3/locations_get_v3_locations_get
    """

    import gzip
    import json

    from filelock import FileLock

    kwargs["limit"] = kwargs.get("limit", 1000)

    p = HERE / "openaq_locations_data.json.gz"

    def have_cache():
        if p.is_file():
            now = pd.Timestamp.now(tz="UTC")
            mtime = pd.Timestamp.fromtimestamp(p.stat().st_mtime, tz="UTC")
            logger.info(f"locations cache file exists, mtime {mtime:%Y-%m-%d %H:%M:%SZ}")
            if now - mtime < pd.Timedelta(days=7):
                return True
            else:
                logger.info("locations cache file is old, will refresh")
        else:
            logger.info("locations cache file not found, will create")
        return False

    data = None
    with FileLock(p.as_posix() + ".lock"):
        if not have_cache():
            data = _consume(_ENDPOINTS["locations"], **kwargs)
            with gzip.open(p, "wt") as f:
                json.dump(data, f)

    if data is None:
        logger.info("using cached locations data")
        with gzip.open(p, "rt") as f:
            data = json.load(f)

    # Some fields with scalar values to take
    some_scalars = [
        "id",
        "name",
        "locality",
        "timezone",
        "isMobile",
        "isMonitor",
        "distance",
    ]

    # We will convert the keys of these dicts to columns
    some_dicts = ["country", "owner", "provider"]

    data2 = []
    for d in data:
        # Example (k v):
        # - id 3
        # - name NMA - Nima
        # - locality None
        # - timezone Africa/Accra
        # - country {'id': 152, 'code': 'GH', 'name': 'Ghana'}
        # - owner {'id': 4, 'name': 'Unknown Governmental Organization'}
        # - provider {'id': 209, 'name': 'Dr. Raphael E. Arku and Colleagues'}
        # - isMobile False
        # - isMonitor True
        # - instruments [{'id': 2, 'name': 'Government Monitor'}]
        # - sensors [
        #     {'id': 6, 'name': 'pm10 µg/m³', 'parameter': {'id': 1, 'name': 'pm10', 'units': 'µg/m³', 'displayName': 'PM10'}},
        #     {'id': 5, 'name': 'pm25 µg/m³', 'parameter': {'id': 2, 'name': 'pm25', 'units': 'µg/m³', 'displayName': 'PM2.5'}}
        #   ]
        # - coordinates {'latitude': 5.58389, 'longitude': -0.19968}
        # - licenses None
        # - bounds [-0.19968, 5.58389, -0.19968, 5.58389]
        # - distance None
        # - datetimeFirst {'utc': '2016-03-23T20:00:00Z', 'local': '2016-03-23T15:00:00-05:00'}}
        # - datetimeLast None

        # Pull out some data
        first_time = d["datetimeFirst"]
        if first_time is not None:
            first_time = first_time.get("utc", None)
        last_time = d["datetimeLast"]
        if last_time is not None:
            last_time = last_time.get("utc", None)
        lat = d["coordinates"]["latitude"]
        lon = d["coordinates"]["longitude"]
        parameters = []
        parameter_ids = []
        sensor_ids = []
        for s in d["sensors"]:
            parameters.append(s["parameter"]["name"])
            parameter_ids.append(s["parameter"]["id"])
            sensor_ids.append(str(s["id"]))

        # Start by taking selected scalars
        d2 = {k: d[k] for k in some_scalars}

        # Convert some dict values to multiple columns
        for k in some_dicts:
            for kk, vv in d[k].items():
                d2[f"{k}_{kk}"] = vv

        d2.update(
            first_time=first_time,
            last_time=last_time,
            latitude=lat,
            longitude=lon,
            parameters=parameters,
            parameter_ids=parameters,
            sensor_ids=sensor_ids,
        )

        data2.append(d2)

    df = pd.DataFrame(data2).rename(
        columns={
            "isMobile": "is_mobile",
            "isMonitor": "is_monitor",
        }
    )

    # Compute datetimes
    for col in ["first_time", "last_time"]:
        i = df[col].notnull()
        assert df.loc[i, col].str.slice(-1, None).eq("Z").all()
        df[col] = pd.to_datetime(df[col].str.slice(0, -1))

    # Site ID
    df = df.rename(columns={"id": "siteid"})
    df["siteid"] = df.siteid.astype(str)
    maybe_dupe_rows = df[df.siteid.duplicated(keep=False)].sort_values("siteid")
    if not maybe_dupe_rows.empty:
        logger.info(
            f"note: found {len(maybe_dupe_rows)} rows with duplicate site IDs:\n{maybe_dupe_rows}"
        )
    df = df.drop_duplicates("siteid", keep="first").reset_index(drop=True)

    return df


@_api_key_warning
def get_sensors(location_id, **kwargs):
    """Get sensors for a location (ID; aka 'siteid')."""

    # Doesn't seem to be paging properly?
    # (Next page always has the same n)
    # So set to one page for now
    kwargs["limit"] = kwargs.get("limit", 1000)
    kwargs["npages"] = kwargs.get("npages", 1)

    data2 = []
    for d in _consume(f"/v3/locations/{location_id}/sensors", **kwargs):
        first_time = d["datetimeFirst"]
        if first_time is not None:
            first_time = first_time.get("utc", None)
        last_time = d["datetimeLast"]
        if last_time is not None:
            last_time = last_time.get("utc", None)

        d2 = {
            "id": str(d["id"]),
            "name": d["name"],
            "parameter": d["parameter"]["name"],
            "parameter_id": d["parameter"]["id"],
            "first_time": first_time,
            "last_time": last_time,
        }

        data2.append(d2)

    df = pd.DataFrame(data2)

    # Compute datetimes
    for col in ["first_time", "last_time"]:
        i = df[col].notnull()
        assert df.loc[i, col].str.slice(-1, None).eq("Z").all()
        df[col] = pd.to_datetime(df[col].str.slice(0, -1))

    return df


@_api_key_warning
def get_parameters(**kwargs):
    """Get supported parameter info from OpenAQ v3 API.

    kwargs are passed to :func:`_consume`.
    """

    data = _consume(_ENDPOINTS["parameters"], **kwargs)

    df = pd.DataFrame(data).rename(columns={"displayName": "display_name"})

    return df


def get_latlonbox_sites(latlonbox, **kwargs):
    """From all available sites, return those within a lat/lon box.

    kwargs are passed to :func:`_consume`.

    Parameters
    ----------
    latlonbox : array-like of float
        ``[lat1, lon1, lat2, lon2]`` (lower-left corner, upper-right corner)
    """
    lat1, lon1, lat2, lon2 = latlonbox
    sites = get_locations(**kwargs)

    in_box = (
        (sites.latitude >= lat1)
        & (sites.latitude <= lat2)
        & (sites.longitude >= lon1)
        & (sites.longitude <= lon2)
    )
    # TODO: need to account for case of box crossing antimeridian

    return sites[in_box].reset_index(drop=True)


def _to_wide_fmt(df):
    # Normalize units
    for vn, f in _PPM_TO_UGM3.items():
        is_ug = (df.parameter == vn) & (df.units == "µg/m³")
        df.loc[is_ug, "value"] /= f
        df.loc[is_ug, "units"] = "ppm"

    # Warn if inconsistent units
    p_units = df.groupby("parameter").units.unique()
    unique = p_units.apply(len).eq(1)
    if not unique.all():
        p_units_non_unique = p_units[~unique]
        warnings.warn(f"inconsistent units among parameters:\n{p_units_non_unique}")

    # Certain metadata should be unique for a given site but sometimes aren't
    # (e.g. location names of different specificity, slight differences in lat/lon coords)
    # TODO: would be nice to have location name too
    for col in ["latitude", "longitude"]:
        site_col = df.groupby("siteid")[col].unique()
        unique = site_col.apply(len).eq(1)
        if not unique.all():
            site_col_non_unique = site_col[~unique]
            warnings.warn(
                f"non-unique {col!r} among site IDs:\n{site_col_non_unique}" "\nUsing first."
            )
            df = df.drop(columns=[col]).merge(
                site_col.str.get(0),
                left_on="siteid",
                right_index=True,
                how="left",
            )

    # Pivot
    index = [
        "siteid",
        "time",
        "latitude",
        "longitude",
        "time_local",
        "utcoffset",
        #
        "country",
        #
        "sensor_id",
        "is_mobile",
        "is_monitor",
        "period_label",
    ]
    if "time_local" not in df.columns:  # daily data
        index.remove("time_local")
    assert sorted(index + ["parameter", "value", "units"]) == sorted(df.columns)

    dupes = df[df.duplicated(keep=False)]
    if not dupes.empty:
        logging.info(f"found {len(dupes)} duplicated rows")
    for col in index:
        if df[col].isnull().all():
            index.remove(col)
            warnings.warn(f"dropping {col!r} from index for wide fmt (all null)")
    df = (
        df.drop_duplicates(keep="first")
        .pivot_table(
            values="value",
            index=index,
            columns="parameter",
        )
        .reset_index()
    )

    # Rename so that units are clear
    df = df.rename(columns={p: f"{p}_ugm3" for p in _NON_MOLEC_PARAMS}, errors="ignore")
    df = df.rename(columns={p: f"{p}_ppm" for p in _PPM_TO_UGM3}, errors="ignore")

    return df


@_api_key_warning
def add_data(
    dates,
    *,
    parameters=None,
    raw=None,
    hourly=None,
    daily=None,
    country=None,
    sites=None,
    entity=None,
    sensor_type=None,
    sensor_ids=None,
    query_time_split=None,
    wide_fmt=False,  # FIXME: probably want to default to True
    **kwargs,
):
    """Get OpenAQ API v3 data, including low-cost sensors.

    Parameters
    ----------
    dates : datetime-like or array-like of datetime-like
        One desired date/time or
        an array, of which the min and max will be used
        as inclusive time bounds of the desired data.
    parameters : str or list of str, optional
        For example, ``'o3'`` or ``['pm25', 'o3']`` (default).
    raw, hourly, daily : bool, optional
        Select product (use only one of these).
        By default, raw data is returned.
    country : str or list of str, optional
        For example, ``'US'`` or ``['US', 'CA']`` (two-letter country codes).
        Default: full dataset (no limitation by country).
    sites : list of str, optional
        Site ID(s) to include, e.g. a specific known site
        or group of sites from :func:`get_latlonbox_sites`.
        Note that in the OpenAQ API, these are called 'location IDs'
        and are integers, not strings.
        We use strings here for consistency with other MONETIO obs readers.
        Default: full dataset (no limitation by site).
    entity : str or list of str, optional
        Options: ``'government'``, ``'research'``, ``'community'``.
        Default: full dataset (no limitation by entity).
    sensor_type : str or list of str, optional
        Options: ``'low-cost sensor'``, ``'reference grade'``.
        Default: full dataset (no limitation by sensor type).
    sensor_ids : str or list of str, optional
        Sensor ID(s) to include.
        Default: full dataset (no limitation by sensor).
    query_time_split
        Frequency to use when splitting the web API queries in time,
        in a format that ``pandas.to_timedelta`` will understand.
        There is a 100k limit on the number of results you can get from a single query.
        In this version of the OpenAQ web API, each sensor has its own endpoint
        and so is a separate query,
        but 100k equates to more than 10 years of hourly data.
        For many use cases, data from a single sensor fits in one page
        (the default page size, controlled by `limit`, is 500).
        Time splitting might still be useful if you are requesting
        a long record from a single sensor, for example,
        to allow for multi-threaded requesting.
        Set to ``None`` for no time splitting (default).
        Default: no time splitting
        Ignored if only one date/time is provided.
    wide_fmt : bool
        Convert dataframe to wide format (one column per parameter).
        Note that for some variables that involves conversion from
        µg/m³ to ppmv.
        This conversion is based on an average air molecular weight of 29 g/mol
        and an air density of 1.2 kg/m³.
        Use ``wide_fmt=False`` if you want to do the conversion yourself.
        In some cases, the conversion to wide format also reduces the amount of data returned.
    retry : int, default: 5
        Number of times to retry an API request if it times out.
    timeout : float, default: 10
        Seconds to wait for the server before giving up, for a single request.
    threads : int, optional
        Number of threads to use for fetching data.
        Default: no multi-threading.
    """

    dates = pd.to_datetime(dates)
    if pd.api.types.is_scalar(dates):
        dates = pd.DatetimeIndex([dates])
    dates = dates.dropna()
    if dates.empty:
        raise ValueError("must provide at least one datetime-like")

    if parameters is None:
        parameters = ["pm25", "o3"]
    elif isinstance(parameters, str):
        parameters = [parameters]

    if all([raw is None, hourly is None, daily is None]):
        # Default to raw
        # FIXME: probably want hourly to be the default
        raw = True
        hourly = False
        daily = False
    else:
        # User specified one (or more)
        if raw is None:
            raw = False
        if hourly is None:
            hourly = False
        if daily is None:
            daily = False
    if sum([raw, hourly, daily]) != 1:
        raise ValueError("exactly one of raw, hourly, daily can be True")
    endpt_tpl = "/v3/sensors/{sensor_id}/measurements"
    if hourly:
        endpt_tpl += "/hourly"
    elif daily:
        endpt_tpl += "/daily"

    # The API seems to assume local time if tz is not set.
    # Usually we _do_ want/mean UTC, but for daily, we want to select dates,
    # which are local time, and we don't want worry about tz differences
    # for different sites.
    if dates.tz is None and not daily:
        dates = dates.tz_localize("UTC")
    if daily and dates.tz is not None:
        warnings.warn("ignoring tz for daily data")
        dates = dates.tz_localize(None)

    query_dt = pd.to_timedelta(query_time_split) if len(dates) > 1 else None
    date_min, date_max = dates.min(), dates.max()
    if query_dt is not None:
        if query_dt <= pd.Timedelta(0):
            raise ValueError(
                f"query_time_split must be positive, got {query_dt} from {query_time_split!r}"
            )
        if date_min == date_max:
            raise ValueError(
                "must provide at least two unique datetimes to use query_time_split. "
                "Set query_time_split=None to disable time splitting."
            )
    if hourly or raw:
        date_max = date_max + pd.Timedelta(hours=1)
    elif daily:
        date_max = date_max + pd.Timedelta(days=1)

    def iter_time_slices():
        # Seems that (from <= time < to) == [from , to) is used
        if query_dt is not None:
            t = date_min
            while t < date_max:
                t_next = min(t + query_dt, date_max)
                yield t, t_next
                t = t_next
        else:
            yield date_min, date_max

    # Discover locations
    print("loading locations...")
    meta = get_locations()
    print(f"{len(meta)} locations detected")

    # Narrow locations based on user input
    if country is not None:
        meta = meta.query("country_code == @country")
    if sites is not None:
        meta = meta.query("siteid == @sites")
    if entity is not None:
        raise NotImplementedError  # TODO: not sure what to use for this
    if sensor_type is not None:
        meta = meta.assign(
            sensor_type=meta["is_monitor"].map(
                {
                    True: "reference grade",
                    False: "low-cost sensor",
                }
            )
        )
        meta = meta.query("sensor_type == @sensor_type")
    meta = meta[
        (meta.first_time <= date_max.tz_localize(None))
        & (meta.last_time >= date_min.tz_localize(None))
    ]

    # Pick sensors that have the desired parameters
    sensors = meta.explode(["sensor_ids", "parameters"], ignore_index=True).rename(
        columns={"sensor_ids": "sensor_id", "parameters": "parameter"}
    )
    sensors = sensors.query("parameter == @parameters")
    sensor_limit = kwargs.pop("sensor_limit", None)  # for testing
    if sensor_limit is not None:
        sensors = sensors.iloc[:sensor_limit]
    if sensor_ids is not None:
        sensors = sensors.query("sensor_id == @sensor_ids")
    print(
        f"requesting data from {len(sensors)} sensor(s) "
        f"at {sensors.siteid.nunique()} unique location(s)"
    )

    def iter_queries():
        for sensor_id in sensors["sensor_id"]:
            for t_from, t_to in iter_time_slices():
                yield sensor_id, {
                    "datetime_from": t_from,
                    "datetime_to": t_to,
                }

    threads = kwargs.pop("threads", None)

    def tfunc(tup):
        sensor_id, params = tup
        endpt = endpt_tpl.format(sensor_id=sensor_id)
        return [
            {
                "value": d["value"],
                "parameter_id": d["parameter"]["id"],
                "period_label": d["period"]["label"],
                "time_from_utc": d["period"]["datetimeFrom"]["utc"],
                "time_from_local": d["period"]["datetimeFrom"]["local"],
                "time_to_utc": d["period"]["datetimeTo"]["utc"],
                "time_to_local": d["period"]["datetimeTo"]["local"],
                "sensor_id": sensor_id,
            }
            for d in _consume(endpt, params=params, **kwargs)
        ]

    tic = perf_counter()
    if threads is not None:
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(tfunc, tup) for tup in iter_queries()]
            data = []
            done, not_done = concurrent.futures.wait(
                futures,
                return_when="FIRST_EXCEPTION",
            )
            logger.info(f"{len(done)} tasks done, {len(not_done)} not done")
            for future in done:
                e = future.exception()
                if e is not None:
                    logger.error(f"thread raised {e!r}")
                    for future in not_done:
                        future.cancel()
                    raise RuntimeError("exception raised in thread") from e
                data.extend(future.result())
    else:
        data = []
        for tup in iter_queries():
            this_data = tfunc(tup)
            data.extend(this_data)
    logger.info(f"took {pd.Timedelta(seconds=(perf_counter() - tic))} s to fetch data")

    df = pd.DataFrame(data)
    if df.empty:
        print("warning: no data found")
        return df

    # Convert times to naive datetime, e.g.
    # {'utc': '2019-08-01T04:00:00Z', 'local': '2019-08-01T00:00:00-04:00'}}
    for col in ["time_from", "time_to"]:
        df[f"{col}_utc"] = pd.to_datetime(df[f"{col}_utc"]).dt.tz_localize(None)
        df[f"{col}_local"] = pd.to_datetime(df[f"{col}_local"].str.slice(0, 19))

    utcoffset = df["time_from_local"] - df["time_from_utc"]

    # Choose time
    df = df.assign(
        time=df["time_from_utc"],  # left-labelled
        time_local=df["time_from_local"],
        utcoffset=utcoffset,
    ).drop(
        columns=[
            "time_from_utc",
            "time_from_local",
            "time_to_utc",
            "time_to_local",
        ]
    )

    if raw:
        df = df[
            df.time.between(
                date_min.tz_localize(None),
                date_max.tz_localize(None) - pd.Timedelta(hours=1),
                inclusive="both",
            )
        ]
    elif daily:
        # We only need the date
        assert df["time_local"].eq(df["time_local"].dt.floor("D")).all()
        df = df.assign(time=df["time_local"]).drop(columns=["time_local"])

    # Get site info in from meta df
    df = df.merge(
        sensors[
            [
                "country_code",
                "siteid",
                "latitude",
                "longitude",
                "sensor_id",
                "is_mobile",
                "is_monitor",
            ]
        ],
        on="sensor_id",
        how="left",
    ).rename(
        columns={
            "country_code": "country",
        }
    )

    # Add parameter info
    parameters = get_parameters().rename(
        columns={
            "id": "parameter_id",
            "name": "parameter",
        }
    )
    df = df.merge(
        parameters[["parameter_id", "parameter", "units"]],
        on="parameter_id",
        how="left",
    ).drop(columns="parameter_id")

    # Most variables invalid if < 0
    # > preferredUnit.value_counts()
    # ppb              19
    # µg/m³            13
    # ppm              10
    # particles/cm³     8
    # %                 3  relative humidity
    # umol/mol          1
    # ng/m3             1
    # deg               1  wind direction
    # m/s               1  wind speed
    # deg_c             1
    # hpa               1
    # ugm3              1
    # c                 1
    # f                 1
    # mb                1
    # iaq               1
    non_neg_units = [
        "particles/cm³",
        "ppm",
        "ppb",
        "umol/mol",
        "µg/m³",
        "ugm3",
        "ng/m3",
        "iaq",
        #
        "%",
        #
        "m/s",
        #
        "hpa",
        "mb",
    ]
    df.loc[df.units.isin(non_neg_units) & (df.value < 0), "value"] = np.nan

    col_order = [
        "parameter",
        "value",
        "units",
        "time",
        "siteid",
        "latitude",
        "longitude",
        "time_local",
        "utcoffset",
        "country",
        "sensor_id",
        "is_mobile",
        "is_monitor",
        "period_label",
    ]
    if daily:
        col_order.remove("time_local")
    assert sorted(df.columns) == sorted(col_order)
    df = df[col_order]

    if wide_fmt:
        df = _to_wide_fmt(df)

    return df
