===
API
===

.. contents::
   :depth: 3
   :local:


Data sources
============

Point observations
------------------

In general, these modules provide an ``add_data`` function for retrieving data,
for example, :func:`monetio.aeronet.add_data`.

AERONET
^^^^^^^

.. automodule:: monetio.aeronet

Functions
+++++++++

.. autosummary::

   monetio.aeronet.add_data
   monetio.aeronet.add_local
   monetio.aeronet.get_valid_sites

.. autofunction:: monetio.aeronet.add_data
.. autofunction:: monetio.aeronet.add_local
.. autofunction:: monetio.aeronet.get_valid_sites

AirNow
^^^^^^

.. automodule:: monetio.airnow

Functions
+++++++++

.. autosummary::

   monetio.airnow.add_data
   monetio.airnow.read_csv

.. autofunction:: monetio.airnow.add_data
.. autofunction:: monetio.airnow.read_csv

AQS
^^^

.. automodule:: monetio.aqs

Functions
+++++++++

.. autosummary::

   monetio.aqs.add_data

.. autofunction:: monetio.aqs.add_data


Profile observations
--------------------

.. automodule:: monetio.geoms

.. autosummary::

   monetio.geoms.open_dataset

.. autofunction:: monetio.geoms.open_dataset


Utility functions
=================

There are a few top-level utility functions.

.. autosummary::

   monetio.rename_latlon
   monetio.rename_to_monet_latlon
   monetio.dataset_to_monet
   monetio.coards_to_netcdf

Grid tools
----------

.. autosummary::

   monetio.grids



* :ref:`genindex`
* :ref:`modindex`
