# GHRSST Level 4 Sea Surface Temperature

MONETIO's `ghrsst` reader loads the daily global Level 4 analyses used by NOAA EMC:

- NCEI AVHRR OI (`ghrsst_ncei_avhrr_anl`), native 0.25° grid.
- OSPO Geo-Polar Blended (`ghrsst_ospo_geopolar_anl`), native 0.05° grid.

The reader preserves source variables and metadata, including `analysed_sst`, `analysis_error`, `mask`, and `sea_ice_fraction` when present. Packed values and fill values are decoded by the NetCDF backend. The reader does not apply an ocean/ice quality filter, convert units, or regrid the products. It standardizes geographic coordinates to `latitude` and `longitude` and gridded dimensions to `y` and `x`.

## Read local files

```python
import monetio

ds = monetio.load(
    "ghrsst",
    files="daily-analysis.nc",
    product="ghrsst_ncei_avhrr_anl",  # optional when metadata identifies the product
)
```

Pass a list of files for a daily series. Compatible files are ordered by decoded observation time. Mixed products, mismatched grids, duplicate timestamps, and missing files fail with a named error.

## Read an archive date

```python
from monetio.readers.ghrsst import GHRSSTReader

reader = GHRSSTReader()
urls = reader.build_urls(
    ["2024-02-28", "2024-02-29"],
    product="ghrsst_ncei_avhrr_anl",
)
ds = reader.open_dataset(urls, product="ghrsst_ncei_avhrr_anl")
```

The default archive roots are the public NCEI AVHRR OI and OSPO Geo-Polar Blended directories. Override the root with `base_url` when using a mirror. A date lookup requires exactly one product file for each date; missing files, access failures, and ambiguous revisions are errors. You can also supply a complete HTTPS file URL directly through `files`.

## Dask and chunks

The default read is eager. `use_dask=True` or `defer=True` returns Dask-backed science variables; `defer=True` is an explicit request to postpone array evaluation. Both products support caller-selected `chunks`, including local and HTTPS inputs and multi-file series.

```python
ds = reader.open_dataset(
    urls,
    product="ghrsst_ncei_avhrr_anl",
    use_dask=True,
    chunks={"time": 1, "lat": 256, "lon": 512},
)
# Select or compute the variables needed for the analysis.
subset = ds.analysed_sst.isel(y=slice(0, 100), x=slice(0, 100)).compute()
ds.close()
```

Choose chunks for the downstream operation and available worker memory. When the storage chunk layout is known, align Dask chunks with it or use a multiple of the storage chunk size; the reader does not force a global rechunk. Close the returned dataset when finished so backend resources are released.

## Time interpretation

The default `time_mode="native"` keeps the timestamps encoded in the source file. Select `time_mode="emc"` only for a native file that needs the alignment shift in NOAA-EMC's verification workflow:

```python
ncei = reader.open_dataset(file, product="ghrsst_ncei_avhrr_anl", time_mode="emc")
ospo = reader.open_dataset(file, product="ghrsst_ospo_geopolar_anl", time_mode="emc")
```

EMC mode adds 86,400 seconds to NCEI AVHRR OI times and 43,200 seconds to OSPO Geo-Polar Blended times. The selected mode and offset are recorded in dataset attributes. The source file is never modified. Do not request EMC mode for a copy that has already been adjusted upstream; the reader cannot reliably infer prior adjustment without provenance in that copy.
