******************
Observations
******************

This section will aid in how to use MONET to load observational datasets available.

First we will import several libraries to aid for in the future.

.. code::   python

    import numpy as np          # numpy
    import pandas as pd         # pandas
    from monet.obs import *     # observations from MONET
    import matplotlib.pyplot as plt # plotting
    import seaborn as sns       # better color palettes
    import cartopy.crs as ccrs  # map projections
    import cartopy.feature as cfeature # politcal and geographic features


EPA AQS
-------

MONET is able to use the EPA AQS data that is collected and reported on an hourly and daily time scale.

    "The Air Quality System (AQS) contains ambient air pollution data collected by EPA, state, local, and tribal air pollution control agencies from over thousands of monitors.  AQS also contains meteorological data, descriptive information about each monitoring station (including its geographic location and its operator), and data quality assurance/quality control information.  AQS data is used to:
    assess air quality,
    evaluate State Implementation Plans for non-attainment areas,
    prepare reports for Congress as mandated by the Clean Air Act." - https://www.epa.gov/aqs

We will begin by loading hourly ozone concentrations from 2018.  The EPA AQS data
is seperated into yearly files and seperate files for hourly and daily data.  The
files are also seperated by which variable is measured.  For instance, hourly ozone files
for the entire year of 2018 are found in https://aqs.epa.gov/aqsweb/airdata/hourly_44201_2018.zip.
We will first load a single variable and then add multiple later on.

.. code::  python

  #first determine the dates
  dates = pd.date_range(start='2018-01-01', end='2018-12-31', freq='H')
  # load the data
  df = aqs.add_data(dates, param=['OZONE'])

If you would rather daily data to get the 8HR max ozone concentration or daily maximum
concentration you can add the *daily* kwarg.

.. code::   python

  df = aqs.add_data(dates, param=['OZONE'], daily=True)

As in AirNow you can download the data to the local disk using the *download*

.. code::   python

  df = aqs.add_data(dates, param=['OZONE'], daily=True, download=True)


Available Measurements
^^^^^^^^^^^^^^^^^^^^^^

* O3 (OZONE)
* PM2.5 (PM2.5)
* PM2.5_frm (PM2.5)
* PM10
* SO2
* NO2
* CO
* NONOxNOy
* VOC
* Speciated PM (SPEC)
* Speciated PM10 (PM10SPEC)
* Wind Speed and Direction (WIND, WS, WDIR)
* Temperature (TEMP)
* Relative Humidity and Dew Point Temperature (RHDP)

Loading Multiple Measurements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's load variables PM10 and OZONE using hourly data to get an idea of how to get multiple variables:

.. code::   python

  df = aqs.add_data(dates, param=['OZONE','PM10'])

Loading Specfic Network
^^^^^^^^^^^^^^^^^^^^^^^

Sometimes you may want to load a specific network that is available in the AQS data
files.  For instance, lets load data from the Chemical Speciation Network (CSN;
https://www.epa.gov/amtic/chemical-speciation-network-csn).
As of writting this tutorial we will load the 2017 data as it is complete.

.. code::   python

    dates = pd.date_range(start='2017-01-01', end='2018-01-01', freq='H')
    df = aqs.add_data(dates,param=['SPEC'], network='CSN', daily=True )

Available Networks
^^^^^^^^^^^^^^^^^^

* NCORE (https://www3.epa.gov/ttn/amtic/ncore.html)
* CSN (https://www.epa.gov/amtic/chemical-speciation-network-csn)
* CASTNET (https://www.epa.gov/castnet)
* IMPROVE (http://vista.cira.colostate.edu/Improve/)
* PAMS (https://www.epa.gov/amtic/photochemical-assessment-monitoring-stations-pams)
* SCHOOL AIR TOXICS (https://www3.epa.gov/ttnamti1/airtoxschool.html)
* NEAR ROAD (NO2; https://www.epa.gov/amtic/no2-monitoring-near-road-monitoring)
* NATTS (https://www3.epa.gov/ttnamti1/natts.html)


NADP
----

NADP is a composed of five regional networks; NTN, AIRMoN, AMoN, AMNet, and MDN.
MONET allows you to read data from any of the five networks with a single call by
specifying the wanted network.

To add data from any of the networks it is a simple call using the nadp object.  As
an example, to load data from the NTN network the call would look like:

.. code:: python

  df = nadp.add_data(dates, network='NTN')

To read data from another network simply replace the network with the name of the
wanted network.  The network name must be a string but is case insensitive.

NTN
^^^

    "The NTN is the only network providing a long-term record of precipitation chemistry across the United States.

    Sites predominantly are located away from urban areas and point sources of pollution. Each site has a precipitation
    chemistry collector and gage. The automated collector ensures that the sample is exposed only during precipitation (wet-only-sampling)."
    - https://nadp.slh.wisc.edu/NTN/

Available Measurements
======================

* H+ (ph)
* Ca2+ (ca)
* Mg2+ (mg)
* Na+ (na)
* K+ (k)
* SO42- (so4)
* NO3- (no3)
* Cl- (cl)
* NH4+ (nh4)

MDN
^^^

    "The MDN is the only network providing a longterm record of total mercury (Hg) concentration and deposition in precipitation in the United States and Canada. All MDN sites follow standard procedures and have uniform precipitation chemistry collectors and gages. The automated collector has the same basic design as the NTN collector but is modified to preserve mercury. Modifications include a glass funnel, connecting tube, bottle for collecting samples, and an insulated enclosure to house this sampling train. The funnel and connecting tube reduce sample exposure to the open atmosphere and limit loss of dissolved mercury. As an additional sample preservation measure, the collection bottle is charged with 20 mL of a one percent hydrochloric acid solution."
    - https://nadp.slh.wisc.edu/MDN/

Available Measurements
======================

* net concentration of methyl mercury in ng/L (conc)
* precipitation amount (in inches) reported by the raingage for the entire sampling period. (raingage)
* Mg2+ (mg)
* Na+ (na)
* K+ (k)
* SO42- (so4)
* NO3- (no3)
* Cl- (cl)
* NH4+ (nh4)

IMPROVE
-------

to do...

OpenAQ
------

to do.....

CEMS
----

to do.....

Climate Reference Network
-------------------------

to do.....

Integrated Surface Database
---------------------------

.. code::   python

    dates = [pd.Timestamp('2012-01-01'), pd.Timestamp('2012-12-31')]

.. code::   python

    area = [-105.0, -97, 44.5, 49.5]

Now a simple one stop command to return the pandas :py:class:`~pandas.DataFrame`
of the data on the given dates.  MONET reads the hourly data from the ISD LITE database.

.. code::   python

    from monet.obs import ish
    df = ish.add_data(dates, country=None, box=area, resample=False)


Or you can create your own instance of the ISH class.

.. code:: python

    from monet.obs import ish_mod
    metdata = ish_mod.ISH()
    df = metdata.add_data(dates, country=None, box=area, resample=False)

To see what data is in the DataFrame simply output the column header values

.. code:: python

    print(df.colums.values)

Available Measurements
^^^^^^^^^^^^^^^^^^^^^^

* dew point (dpt)
* temperature (t)
* visibility (vsb)
* wind speed (ws)
* wind direction (wdir)

The ISD (ISH) database contains latitude, longitude, station name, station id,
time, dew point (dpt), temperature (t), visibility (vsb),
wind speed (ws), wind direction (wdir), as well as various quality flags.

ICARTT
------

MONET is capable of reading the NASA ICARTT data format (https://www-air.larc.nasa.gov/missions/etc/IcarttDataFormat.htm).
Many field campaigns save data in ICARTT format.  Methods are available to combine flight data.

.. code:: python

  from monet.obs import icartt

  f = icartt.add_data('filename')

This will return a xarray.Dataset.  If you would prefer a pandas.DataFrame you
can use the icartt.get_data function.  This will try to automatically rename a
few columns like latitude and longitude and time from the data array and return
a monet compatible pandas.DataFrame.

.. code:: python

  df = icartt.get_data(f)
