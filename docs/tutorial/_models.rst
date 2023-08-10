******************
Opening Model Data
******************

MONET is capable of opening output from several different models.  This tutorial will
demonstrate how to open, extract variables and quickly display the results.

CMAQ
----

CMAQ is a 3D photochemical grid model developed at the U.S. EPA to simulate air
composition.  MONET is able to read the output IOAPI output and format it to be
compatible with it's datastream.

As an example, lets open some CMAQ data from the Hawaiian volcanic eruption in 2018.
First we will set the path to the data files


.. code-block:: python

    import monet

    cmaqfile = monet.__path__ + '/../data/aqm.t12z.aconc.ncf'
    gridcro2d = monet.__path__ + '/../data/aqm.t12z.grdcro2d.ncf'

    from monet.models import *

    c = cmaq.open_dataset(flist=cmaqfile, grid=gridcro2d)

This will return an :py:class:`~xarray.Dataset`.  The dataset is also still stored
in the :py:class:`~cmaq` object as :py:class:`~cmaq.dset`.

more here


FV3-CHEM in MONET
-----------------

FV3-CHEM is the experimental implementation of the NASA Gocart model
into NOAA's Next Generation Global Prediction System. This tutorial will
explain how to open read and use MONET to explore output from the
FV3-CHEM system.

Currently, FV3 outputs data in three formats, i.e., nemsio, grib2, and
netcdf. There are slight issues with both the nemsio and grib2 data that
will be explained below but needless to say they need to be converted
with precossors.

Convert nemsio2nc4
==================

The first format that needs to be converted is the nemsio data. NOTE: The nemsio format
will be discontinued as of GFSv16.  This step will not be needed to read the output data.
nemsio is a binary format used within EMC for GFSv15 and previous versions. As a work around a converter was created to convert
the binary nemsio data into netCDF, nemsio2nc4.py.

This is a command-line script that converts the nemsio data output by
the FV3 system to netcdf4 files using a combination of mkgfsnemsioctl
found within the fv3 workflow and the climate data operators (cdo). This
tool is available on github at https://github.com/bbakernoaa/nemsio2nc4

::

    nemsio2nc4.py --help
    usage: nemsio2nc4.py [-h] -f FILES [-n NPROCS] [-v]

    convert nemsio file to netCDF4 file

    optional arguments:
      -h, --help            show this help message and exit
      -f FILES, --files FILES
                            input nemsio file name (default: None)
      -v, --verbose         print debugging information (default: False)

Example Usage
~~~~~~~~~~~~~

If you want to convert a single nemsio data file to netcdf4, it can be
done like this:

::

    nemsio2nc4.py -v -f 'gfs.t00z.atmf000.nemsio'
    mkgfsnemsioctl: /gpfs/hps3/emc/naqfc/noscrub/Barry.Baker/FV3CHEM/exec/mkgfsnemsioctl
    cdo: /naqfc/noscrub/Barry.Baker/python/envs/monet/bin/cdo
    Executing: /gpfs/hps3/emc/naqfc/noscrub/Barry.Baker/FV3CHEM/exec/mkgfsnemsioctl gfs.t00z.atmf000.nemsio
    Executing: /naqfc/noscrub/Barry.Baker/python/envs/monet/bin/cdo -f nc4 import_binary gfs.t00z.atmf000.nemsio.ctl gfs.t00z.atmf000.nemsio.nc4
    cdo import_binary: Processed 35 variables [1.56s 152MB]

To convert multiple files you can simple use the hot keys available in
linux terminals.

::

     nemsio2nc4.py -v -f 'gfs.t00z.atmf0*.nemsio'

Convert fv3grib2nc4
-------------------

Although there are several ways in which to read grib data from python
such as pynio, a converter was created due to the specific need to
distinguish aerosols which these readers do not process well.

fv3grib2nc4.py like nemsio2nc4.py tool is a command line tool created to
convert the grib2 aerosol data to netcdf files. fv3grib2nc4.py will
create separate files for each of the three layer types; '1 hybrid
layer', 'entire atmosphere', and 'surface'. These are the three layers
that currently hold aerosol data. The tool is available at
https://github.com/bbakernoaa/fv3grib2nc4

::

    fv3grib2nc4.py --help
    usage: fv3grib2nc4.py [-h] -f FILES [-v]

    convert nemsio file to netCDF4 file

    optional arguments:
      -h, --help            show this help message and exit
      -f FILES, --files FILES
                            input nemsio file name (default: None)
      -v, --verbose         print debugging information (default: False) ```

    ### Example Usage

    If you want to convert a single grib2 data file to netcdf4, it can be done like this:

    fv3grib2nc4.py -v -f 'gfs.t00z.master.grb2f000' wgrib2:
    /nwprod2/grib\_util.v1.0.0/exec/wgrib2 Executing:
    /nwprod2/grib\_util.v1.0.0/exec/wgrib2 gfs.t00z.master.grb2f000 -match
    "entire atmosphere:" -nc\_nlev 1 -append -set\_ext\_name 1 -netcdf
    gfs.t00z.master.grb2f000.entire\_atm.nc Executing:
    /nwprod2/grib\_util.v1.0.0/exec/wgrib2 gfs.t00z.master.grb2f000 -match
    "1 hybrid level:" -append -set\_ext\_name 1 -netcdf
    gfs.t00z.master.grb2f000.hybrid.nc Executing:
    /nwprod2/grib\_util.v1.0.0/exec/wgrib2 gfs.t00z.master.grb2f000 -match
    "surface:" -nc\_nlev 1 -append -set\_ext\_name 1 -netcdf
    gfs.t00z.master.grb2f000.surface.nc\`\`\`

To convert multiple files you can simple use the hot keys available in
linux terminals.

::

     fv3grib2nc4.py -v -f 'gfs.t00z.master.grb2f0*'

MONETIO and FV3CHEM
-------------------

Using MONET with FV3-Chem is much like using MONET with other model
outputs. It tries to recognize where the files came from (nemsio, grib2,
etc....) and then processes the data, renaming coordinates (lat lon to
latitude and longitude) and processing variables like geopotential
height and pressure if available. First lets import ``monet`` and
``fv3chem`` from MONET

.. code-block:: python

    import matplotlib.pyplot as plt
    import monetio as mio

To open a single file

.. code-block:: python

    f = mio.fv3chem.open_dataset('/Users/barry/Desktop/temp/gfs.t00z.atmf006.nemsio.nc4')
    print(f)


.. parsed-literal::

    /Users/barry/Desktop/temp/gfs.t00z.atmf006.nemsio.nc4
    <xarray.Dataset>
    Dimensions:    (time: 1, x: 384, y: 192, z: 64)
    Coordinates:
      * time       (time) datetime64[ns] 2018-07-01T06:00:00
      * x          (x) float64 0.0 0.9375 1.875 2.812 ... 356.2 357.2 358.1 359.1
      * y          (y) float64 89.28 88.36 87.42 86.49 ... -87.42 -88.36 -89.28
      * z          (z) float64 1.0 2.0 3.0 4.0 5.0 6.0 ... 60.0 61.0 62.0 63.0 64.0
        longitude  (y, x) float64 0.0 0.9375 1.875 2.812 ... 356.2 357.2 358.1 359.1
        latitude   (y, x) float64 89.28 89.28 89.28 89.28 ... -89.28 -89.28 -89.28
    Data variables:
        ugrd       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        vgrd       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dzdt       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        delz       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        tmp        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dpres      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        spfh       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        clwmr      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        rwmr       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        icmr       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        snmr       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        grle       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        cld_amt    (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        o3mr       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        so2        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        sulf       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dms        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        msa        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        pm25       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        bc1        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        bc2        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        oc1        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        oc2        (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dust1      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dust2      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dust3      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dust4      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        dust5      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        seas1      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        seas2      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        seas3      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        seas4      (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        pm10       (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
        pressfc    (time, y, x) float32 dask.array<shape=(1, 192, 384), chunksize=(1, 192, 384)>
        hgtsfc     (time, y, x) float32 dask.array<shape=(1, 192, 384), chunksize=(1, 192, 384)>
        geohgt     (time, z, y, x) float32 dask.array<shape=(1, 64, 192, 384), chunksize=(1, 64, 192, 384)>
    Attributes:
        CDI:          Climate Data Interface version 1.9.5 (http://mpimet.mpg.de/...
        Conventions:  CF-1.6
        history:      Thu Dec 20 17:46:09 2018: cdo -f nc4 import_binary gfs.t00z...
        CDO:          Climate Data Operators version 1.9.5 (http://mpimet.mpg.de/...


Notice this object f has dimensions of (time,z,y,x) with 2d coordinates
of latitude and longitude. You can get more information on single
variables such as pm25 simply by printing the variable.

.. code-block:: python

    print(f.pm25)


.. parsed-literal::

    <xarray.DataArray 'pm25' (time: 1, z: 64, y: 192, x: 384)>
    dask.array<shape=(1, 64, 192, 384), dtype=float32, chunksize=(1, 64, 192, 384)>
    Coordinates:
      * time       (time) datetime64[ns] 2018-07-01T06:00:00
      * x          (x) float64 0.0 0.9375 1.875 2.812 ... 356.2 357.2 358.1 359.1
      * y          (y) float64 89.28 88.36 87.42 86.49 ... -87.42 -88.36 -89.28
      * z          (z) float64 1.0 2.0 3.0 4.0 5.0 6.0 ... 60.0 61.0 62.0 63.0 64.0
        longitude  (y, x) float64 0.0 0.9375 1.875 2.812 ... 356.2 357.2 358.1 359.1
        latitude   (y, x) float64 89.28 89.28 89.28 89.28 ... -89.28 -89.28 -89.28
    Attributes:
        long_name:  model layer


Here units are not included because it is not stored in the nemsio
format.
