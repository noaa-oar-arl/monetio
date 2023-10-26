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
