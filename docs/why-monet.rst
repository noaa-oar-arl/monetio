Overview: Why MONET?
=====================

Features
--------

Retrieving, loading, and combining data and putting into a common format
is the core of MONET.  MONET uses the pandas_ and xarray_ data formats for data
analysis.

- Open point observations in a common format.  pandas_ excels at working with tabular data or point measurements. It is used for time series analysis and statistical measures.
- Open model and satellite data in a common format.  xarray_ is used when N-dimensional arrays are needed.
- Retrieving observational datasets for given time and space.
- Efficiently combine/interpolate model and observational datasets.
- Provide easy plotting using proven tools in python
- Perform statistics between model runs or observations or models and observations.

Gallery
-------


.. figure:: https://raw.githubusercontent.com/noaa-oar-arl/MONET/master/sample_figures/pm2.5_timeseries.jpg
   :alt: Time Series

   Time Series

.. figure:: https://raw.githubusercontent.com/noaa-oar-arl/MONET/master/sample_figures/pm2.5_timeseries_rmse.jpg
   :alt: Time Series of RMSE

   Time Series of RMSE

.. figure:: https://raw.githubusercontent.com/noaa-oar-arl/MONET/master/sample_figures/ozone_spatial.jpg
   :alt: Spatial Plots

   Spatial Plots

|Scatter Plots| |PDFS Plots| |Difference Scatter Plots| |Difference PDFS
Plots|

.. |Scatter Plots| image:: https://raw.githubusercontent.com/noaa-oar-arl/MONET/master/sample_figures/no2_scatter.jpg
.. |PDFS Plots| image:: https://raw.githubusercontent.com/noaa-oar-arl/MONET/master/sample_figures/no2_pdf.jpg
.. |Difference Scatter Plots| image:: https://raw.githubusercontent.com/noaa-oar-arl/MONET/master/sample_figures/no2_diffscatter.jpg
.. |Difference PDFS Plots| image:: https://raw.githubusercontent.com/noaa-oar-arl/MONET/master/sample_figures/no2_diffpdf.jpg


.. _ndarray: https://numpy.org/doc/stable/reference/arrays.ndarray.html
.. _netCDF: http://www.unidata.ucar.edu/software/netcdf
.. _pandas: https://pandas.pydata.org
.. _xarray: https://xarray.pydata.org
