"""Get AQ data from the OpenAQ v2 REST API."""
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


def _consume(url, *, params=None, timeout=10, limit=500, npages=None):
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
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
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

    return data


def get_locations(**kwargs):
    """Get locations from OpenAQ v2 API."""
    return _consume("https://api.openaq.org/v2/locations", **kwargs)


def add_data():
    """Get OpenAQ API v2 data, including low-cost sensors."""

    # t_from = "2023-09-04T"
    # t_to = "2023-09-04T23:59:59"

    t_from = "2023-09-03T23:59:59"
    # ^ seems to be necessary to get 0 UTC
    # so I guess (from < time <= to) == (from , to] is used
    # i.e. `from` is exclusive, `to` is inclusive
    t_to = "2023-09-04T23:00:00"

    res_limit_per_page = 500  # max number of results per page
    n_pages = 50  # max number of pages

    data = []
    for page in range(1, n_pages + 1):
        print(f"page {page}")
        r = requests.get(
            "https://api.openaq.org/v2/measurements",
            headers={
                "Accept": "application/json",
                "X-API-Key": API_KEY,
            },
            params={
                "date_from": t_from,
                "date_to": t_to,
                "limit": res_limit_per_page,
                # Number of results in response
                # Default: 100
                # "limit + offset must be <= 100_000"
                # where offset = limit * (page - 1)
                # => limit * page <= 100_000
                "page": page,
                # Page in query results
                # Must be <= 6000
                "parameter": ["o3", "pm25", "pm10", "co", "no2"],
                # There are (too) many parameters!
                "country": "US",
                # "city": ["Boulder", "BOULDER", "Denver", "DENVER"],
                # Seems like PurpleAir sensors (often?) don't have city listed
                # But can get them with the coords + radius search
                "coordinates": "39.9920859,-105.2614118",  # CSL-ish
                # lat/lon, "up to 8 decimal points of precision"
                "radius": 10_000,  # meters
                # Search radius has a max of 25_000 (25 km)
                "include_fields": ["sourceType", "sourceName"],  # not working
            },
            timeout=10,
        )
        r.raise_for_status()
        this_data = r.json()
        found = this_data["meta"]["found"]
        print(f"found {found}")
        n = len(this_data["results"])
        if n == 0:
            break
        if n < res_limit_per_page:
            print(f"note: results returned ({n}) < limit ({res_limit_per_page})")
        data.extend(this_data["results"])

    if isinstance(found, str) and found.startswith(">"):
        print("warning: some query results not fetched")

    df = pd.DataFrame(data)
    assert not df.empty

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
