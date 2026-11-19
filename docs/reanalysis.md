# Reanalysis and analysis products

The ERA5, GDAS, CFSR, and CFSv2 readers return xarray datasets. Set
`use_dask=True` to keep their data lazy; pass `chunks` to control the Dask chunks
for GRIB2 and local file reads.

## ERA5 from local files

ERA5 accepts local NetCDF files as before. GRIB and GRIB2 paths use Monetio's
`grib2io` backend:

```python
import monetio as mio

ds = mio.load("era5", files="era5.grib2", use_dask=True)
```

Remote GRIB2 paths can also be passed to `files`. The cloud dependencies are
available with `pip install 'monetio[cloud]'`; GRIB2 reads additionally require
the native `grib2io` library and its NCEPLIBS-g2c dependency.

## ERA5 from Google's public ARCO archive

The ARCO ERA5 reader opens the public hourly, pressure-level Zarr store directly
from Google Cloud Storage. Choose data by date, variables, pressure level, and
optional bounding box. Variable names follow Monetio's canonical names.

```python
ds = mio.load(
    "era5",
    source="arco",
    dates=slice("2020-01-01", "2020-01-02"),
    variables=["temperature_2m", "u_wind_10m", "temperature"],
    levels=[500, 850],  # applies to pressure-level variables
    bbox=(-130, 20, -60, 55),  # west, south, east, north
    use_dask=True,
)
```

Install `pip install 'monetio[cloud]'` for `gcsfs` and `zarr`. Public ARCO
requests are anonymous by default. Dates beyond the final ERA5 release are
rejected unless `include_era5t=True` is set to permit preliminary ERA5T data.

## ERA5 from AWS Icechunk

The Earthmover AWS Open Data edition is a separate Icechunk/Zarr v3 store. Use
`source="aws"`; Monetio opens only the needed `single`, `pressure`, or `500hPa`
group. Choose `layout="spatial"` for maps and regions (the default) or
`layout="temporal"` for longer time series:

```python
ds = mio.load(
    "era5",
    source="aws",
    dates=slice("2018-01-01", "2018-01-02"),
    variables=["temperature_2m", "u_wind_10m", "temperature"],
    levels=[500, 850],
    bbox=(-130, 20, -60, 55),
    use_dask=True,
)
```

The `cloud` extra also installs Icechunk and PCodec for this source. This public
AWS dataset is separate from Google's ARCO Zarr archive and from ERA5T's daily
updates. See the [AWS Marketplace listing](https://aws.amazon.com/marketplace/pp/prodview-ggs5znhej4ol2)
for its data layout and update cadence.

## GDAS

GDAS supports local GRIB2 files and URL generation for the NOAA AWS public
dataset or NOMADS:

```python
ds = mio.load(
    "gdas",
    dates="2025-03-24",
    hour=0,
    lead_time=[0, 3],
    source="aws",  # or "nomads"
    use_dask=True,
)
```

The reader uses the same GRIB2 backend as GFS. AWS reads need the `s3fs`
dependency from `monetio[cloud]` or `monetio[all]`.

## NCEP CFSR and CFSv2

The NCEP reanalysis reader retains `product="reanalysis1"` and
`product="reanalysis2"`. It also routes `product="cfsr"` and
`product="cfsv2"` to six-hourly pressure-level GRIB2 analysis files in the
NOAA NCEI archive:

```python
ds = mio.load("ncep_reanalysis", product="cfsv2", dates="2020-01-02", hour=6)
```

Alternatively, use the dedicated `cfsr` and `cfsv2` sources. Both accept local
files through `files=...` and support Dask-backed reads.
