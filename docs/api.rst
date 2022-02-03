
Get in touch
============

Ask questions, suggest features or view source code `on GitHub`_.

If an issue arrises please post on the `GitHub issues`_.


API
===

Data sources
------------

Point observations
^^^^^^^^^^^^^^^^^^

In general, these modules provide an ``add_data`` function for retrieving data,
for example, :func:`monetio.aeronet.add_data`.

AERONET
"""""""

.. automodule:: monetio.aeronet

.. autosummary::

   monetio.aeronet.add_data
   monetio.aeronet.add_local
   monetio.aeronet.get_valid_sites

.. autofunction:: monetio.aeronet.add_data
.. autofunction:: monetio.aeronet.add_local
.. autofunction:: monetio.aeronet.get_valid_sites


Utility functions
-----------------

There are a few top-level utility functions.

.. autosummary::

   monetio.rename_latlon
   monetio.rename_to_monet_latlon
   monetio.dataset_to_monet
   monetio.coards_to_netcdf


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _GitHub issues: https://github.com/noaa-oar-arl/monetio/issues
.. _on GitHub: https://github.com/noaa-oar-arl/monetio
