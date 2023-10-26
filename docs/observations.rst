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
