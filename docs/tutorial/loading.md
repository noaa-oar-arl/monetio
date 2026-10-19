# Loading Point Data

This section provides a tutorial on how to use MONETIO to load observational datasets.

First, we import the necessary libraries:

```python
import numpy as np  # numpy
import pandas as pd  # pandas
import monetio as mio  # observations from MONETIO
import matplotlib.pyplot as plt  # plotting
import seaborn as sns  # better color palettes
import cartopy.crs as ccrs  # map projections
import cartopy.feature as cfeature  # political and geographic features
```

## AirNow

AirNow is the near real-time dataset for air composition and meteorology measurements.

> The U.S. EPA AirNow program is the national repository of real-time air quality data and forecasts for the United States.
> AirNow is the vehicle for providing timely Air Quality Index (AQI) information to the public,
> media outlets, other federal agencies and their applications, and to the research community.
>
> -- [AirNow Website](https://www.airnow.gov/)

AirNow data can be downloaded and aggregated using the `mio.load` function. For example, let's say that we want to look at data from 2018-05-01 to 2018-05-05.

```python
dates = pd.date_range(start="2018-05-01", end="2018-05-05", freq="h")
```

Now a simple one-stop command to return the pandas `DataFrame` of the aggregated data:

```python
df = mio.load("airnow", files=dates)
```

This provides a structured `DataFrame`.

```python
df.head()
```

## EPA AQS

MONETIO is able to use the EPA AQS data that is collected and reported on an hourly and daily time scale.

> "The Air Quality System (AQS) contains ambient air pollution data collected by EPA, state, local, and tribal air pollution control agencies from over thousands of monitors." - <https://www.epa.gov/aqs>

We will begin by loading hourly ozone concentrations from 2018.

```python
# first determine the dates
dates = pd.date_range(start="2018-01-01", end="2018-12-31", freq="h")
# load the data
df = mio.load("aqs", files=dates, param=["OZONE"])
```

If you would rather daily data to get the 8HR max ozone concentration or daily maximum
concentration you can add the `daily=True` kwarg.

```python
df = mio.load("aqs", files=dates, param=["OZONE"], daily=True)
```

### Available Measurements

- O3 (OZONE)
- PM2.5 (PM2.5)
- PM10
- SO2
- NO2
- CO
- VOC
- Speciated PM (SPEC)
- Wind Speed and Direction (WIND, WS, WDIR)
- Temperature (TEMP)

## AERONET

> "The AERONET (AErosol RObotic NETwork) project is a federation of ground-based remote sensing aerosol networks established by NASA and PHOTONS." - <https://aeronet.gsfc.nasa.gov>

MONETIO uses the AERONET web services to access data.

### Loading AOD and SDA

Aeronet is global data so we are going to look at a single day.

```python
dates = pd.date_range(start="2017-09-25", end="2017-09-26", freq="h")
```

Now let's assume that we want to read the Aerosol Optical Depth Level 1.5 data:

```python
df = mio.load("aeronet", files=dates, product="AOD15")
df.head()
```

To restrict to a region, define a latitude/longitude box `[latmin, lonmin, latmax, lonmax]`:

```python
df = mio.load("aeronet", files=dates, product="AOD15", latlonbox=[2.0, -21, 38, 37])
```
