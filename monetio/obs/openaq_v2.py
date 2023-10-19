"""Get AQ data from the OpenAQ v2 REST API.

https://api.openaq.org/docs#/v2
"""
import json
import os

import pandas as pd
import requests

API_KEY = os.environ.get("OPENAQ_API_KEY", None)
if API_KEY is None:
    print(
        "warning: non-cached requests to the OpenAQ v2 web API will be slow without an API key. "
        "Obtain one and set your OPENAQ_API_KEY environment variable."
    )


def _consume(url, *, params=None, timeout=10, retry=5, limit=500, npages=None):
    """Consume a paginated OpenAQ API endpoint."""
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
    }

    data = []
    for page in range(1, npages + 1):
        params["page"] = page

        tries = 0
        while tries < retry:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 408:
                tries += 1
                print(f"warning: request timed out (try {tries}/{retry})")
            else:
                break
        r.raise_for_status()

        this_data = r.json()
        found = this_data["meta"]["found"]
        n = len(this_data["results"])
        print(f"page={page} found={found!r} n={n}")
        if n == 0:
            break
        if n < limit:
            print(f"note: results returned ({n}) < limit ({limit})")
        data.extend(this_data["results"])

    if isinstance(found, str) and found.startswith(">"):
        print(f"warning: some query results not fetched ('found' is {found!r})")
    elif isinstance(found, int) and len(data) < found:
        print(f"warning: some query results not fetched (found={found}, got {len(data)} results)")

    return data


def get_locations(**kwargs):
    """Get available site info (including IDs) from OpenAQ v2 API.

    https://api.openaq.org/docs#/v2/locations_get_v2_locations_get
    """

    data = _consume("https://api.openaq.org/v2/locations", **kwargs)

    # Some fields with scalar values to take
    some_scalars = [
        "id",
        "name",
        "city",
        "country",
        # "entity",  # all null (from /measurements we do get values)
        "isMobile",
        # "isAnalysis",  # all null
        # "sensorType",  # all null (from /measurements we do get values)
        "firstUpdated",
        "lastUpdated",
    ]

    data2 = []
    for d in data:
        lat = d["coordinates"]["latitude"]
        lon = d["coordinates"]["longitude"]
        parameters = [p["parameter"] for p in d["parameters"]]
        manufacturer = d["manufacturers"][0]["manufacturerName"] if d["manufacturers"] else None
        d2 = {k: d[k] for k in some_scalars}
        d2.update(
            latitude=lat,
            longitude=lon,
            parameters=parameters,
            manufacturer=manufacturer,
        )
        data2.append(d2)

    df = pd.DataFrame(data2)

    # Compute datetimes (the timestamps are already in UTC, but with tz specified)
    assert (df.firstUpdated.str.slice(-6, None) == "+00:00").all()
    df["firstUpdated"] = pd.to_datetime(df.firstUpdated.str.slice(0, -6))
    assert df.lastUpdated.str.slice(-6, None).eq("+00:00").all()
    df["lastUpdated"] = pd.to_datetime(df.lastUpdated.str.slice(0, -6))

    # Site ID
    df = df.rename(columns={"id": "siteid"})
    df["siteid"] = df.siteid.astype(str)
    df = df.drop_duplicates("siteid", keep="first").reset_index(drop=True)  # seem to be some dupes

    return df


def get_parameters(**kwargs):
    """Get supported parameter info from OpenAQ v2 API."""

    data = _consume("https://api.openaq.org/v2/parameters", **kwargs)

    df = pd.DataFrame(data)

    return df


def get_latlonbox_sites(latlonbox, **kwargs):
    """
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


def add_data(
    dates,
    *,
    parameters=None,
    search_radius=None,
    query_time_split="1H",
    **kwargs,
):
    """Get OpenAQ API v2 data, including low-cost sensors.

    Parameters
    ----------
    search_radius : dict
        Mapping coords (lat, lon) [deg] to search radius [m] (max of 25 km).
    """

    dates = pd.DatetimeIndex(dates)
    if parameters is None:
        parameters = ["pm25", "o3"]
    elif isinstance(parameters, str):
        parameters = [parameters]
    query_dt = pd.to_timedelta(query_time_split)
    date_min, date_max = dates.min(), dates.max()
    if date_min == date_max:
        raise ValueError("must provide at least two unique datetimes")

    def iter_time_slices():
        # seems that (from < time <= to) == (from , to] is used
        # i.e. `from` is exclusive, `to` is inclusive
        one_sec = pd.Timedelta(seconds=1)
        t = date_min
        while t < date_max:
            t_next = t + query_dt
            yield t - one_sec, t_next
            t = t_next

    params = {}
    data = []
    for parameter in parameters:
        params.update(parameter=parameter)
        for t_from, t_to in iter_time_slices():
            params.update(
                date_from=t_from,
                date_to=t_to,
            )
            if search_radius is not None:
                for coords, radius in search_radius.items():
                    if not 0 < radius <= 25_000:
                        raise ValueError(f"invalid radius {radius!r}")
                    params.update(
                        coordinates=f"{coords[0]:.8f},{coords[1]:.8f}",
                        radius=radius,
                    )
                    print(
                        f"parameter={parameter!r} t_from='{t_from}' t_to='{t_to}' "
                        f"coords={coords} radius={radius}"
                    )
                    data_ = _consume(
                        "https://api.openaq.org/v2/measurements",
                        params=params,
                        **kwargs,
                    )
                    data.extend(data_)
            else:
                print(f"parameter={parameter!r} t_from='{t_from}' t_to='{t_to}'")
                data_ = _consume(
                    "https://api.openaq.org/v2/measurements",
                    params=params,
                    **kwargs,
                )
                data.extend(data_)

    df = pd.DataFrame(data)
    if df.empty:
        print("warning: no data found")
        return df

    #  #   Column       Non-Null Count  Dtype
    # ---  ------       --------------  -----
    #  0   locationId   2000 non-null   int64
    #  1   location     2000 non-null   object
    #  2   parameter    2000 non-null   object
    #  3   value        2000 non-null   float64
    #  4   date         2000 non-null   object
    #  5   unit         2000 non-null   object
    #  6   coordinates  2000 non-null   object
    #  7   country      2000 non-null   object
    #  8   city         0 non-null      object  # None
    #  9   isMobile     2000 non-null   bool
    #  10  isAnalysis   0 non-null      object  # None
    #  11  entity       2000 non-null   object
    #  12  sensorType   2000 non-null   object

    to_expand = ["date", "coordinates"]
    new = pd.json_normalize(json.loads(df[to_expand].to_json(orient="records")))

    time = pd.to_datetime(new["date.utc"]).dt.tz_localize(None)
    # utcoffset = pd.to_timedelta(new["date.local"].str.slice(-6, None) + ":00")
    # time_local = time + utcoffset
    # ^ Seems some have negative minutes in the tz, so this method complains
    time_local = pd.to_datetime(new["date.local"].str.slice(0, 19))
    utcoffset = time_local - time

    # TODO: null case??
    lat = new["coordinates.latitude"]
    lon = new["coordinates.longitude"]

    df = df.drop(columns=to_expand).assign(
        time=time,
        time_local=time_local,
        utcoffset=utcoffset,
        latitude=lat,
        longitude=lon,
    )

    return df
