"""Get AQ data from the OpenAQ v2 REST API."""
import json

import pandas as pd
import requests

t_from = "2023-09-04"
t_to = "2023-09-04T23:59:59"

n_pages = 20

data = []
for page in range(1, n_pages + 1):
    print(page)
    r = requests.get(
        "https://api.openaq.org/v2/measurements",
        headers={
            "Accept": "application/json",
            # "X-API-Key": "",  # TODO
        },
        params={
            "date_from": t_from,
            "date_to": t_to,
            "limit": 100,
            # Number of results in response
            # Default: 100
            # "limit + offset must be <= 100_000"
            # where offset = limit * (page - 1)
            # => limit * page <= 100_000
            "page": page,
            # Must be <= 6000
            "parameter": ["pm25", "no2", "o3"],
            # There are (too) many parameters!
        },
        timeout=10,
    )
    r.raise_for_status()
    this_data = r.json()
    data.extend(this_data["results"])

df = pd.DataFrame(data)

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
