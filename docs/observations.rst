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
    chemistry collector and gauge. The automated collector ensures that the sample is exposed only during precipitation (wet-only-sampling)."
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

    "The MDN is the only network providing a longterm record of total mercury (Hg) concentration and deposition in precipitation in the United States and Canada. All MDN sites follow standard procedures and have uniform precipitation chemistry collectors and gauges. The automated collector has the same basic design as the NTN collector but is modified to preserve mercury. Modifications include a glass funnel, connecting tube, bottle for collecting samples, and an insulated enclosure to house this sampling train. The funnel and connecting tube reduce sample exposure to the open atmosphere and limit loss of dissolved mercury. As an additional sample preservation measure, the collection bottle is charged with 20 mL of a one percent hydrochloric acid solution."
    - https://nadp.slh.wisc.edu/MDN/

Available Measurements
======================

* net concentration of methyl mercury in ng/L (conc)
* precipitation amount (in inches) reported by the rain gauge for the entire sampling period. (rain gauge)
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

    print(df.columns.values)

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
