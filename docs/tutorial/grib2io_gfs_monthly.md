# Monthly GFS T2M with monetio (GRIB2 + VirtualiZarr)

This example mirrors a GRIB2-first cloud workflow for GFS on NOAA S3.
It uses monetio's GFS reader, which enforces `engine="grib2io"` and routes
VirtualiZarr through the GRIB2 parser.

## Overview

- Data source: `s3://noaa-gfs-bdp-pds`
- Reader: `monetio.readers.gfs.GFSReader`
- Engine: `grib2io` (enforced)
- Parser: `grib2` (enforced for NCEP GRIB)

## Example

```python
import pandas as pd
import xarray as xr

from monetio.readers.gfs import GFSReader

reader = GFSReader()

# Daily 00Z analyses for one month.
dates = pd.date_range("2025-01-01", "2025-01-31", freq="D")

# Optional: persist kerchunk references for faster repeat opens.
reference_file = "gfs_jan2025_t2m.refs.json"

ds = reader.open_dataset(
    dates=dates,
    hour=0,
    lead_time=0,
    use_virtualizarr=True,
    virtualizarr_file=reference_file,
    # Explicit options are optional; monetio applies safe defaults for S3 GRIB2.
    storage_options={"anon": True},
    # Add your grib2io filters if desired, for example selecting temperature.
    # filters={"shortName": "TMP", "typeOfFirstFixedSurface": 103},
)

# monetio harmonizes valid_time to time and ensures time is a dimension.
print(ds)
print(ds.dims)

# If temperature exists, compute a simple monthly mean field.
if "temperature" in ds:
    monthly_mean = ds["temperature"].mean("time")
    print(monthly_mean)
```

## Notes

- For NCEP GRIB readers, monetio rejects non-`grib2io` engines.
- If VirtualiZarr GRIB2 parser support is unavailable, monetio raises an error
  instead of falling back to a non-GRIB parser.
- For remote reads, monetio applies conservative timeout/concurrency defaults
  that can be overridden by passing explicit values.

## Easy GEFS AOD550

For GEFS chemistry data, monetio now provides one-call helpers that wrap
the common grib2io options from the notebook workflow:

```python
import pandas as pd

from monetio.readers.gfs import GEFSReader

reader = GEFSReader()
dates = pd.date_range("2025-01-01", "2025-01-31", freq="D")

ds = reader.open_aerosol_aod550(dates=dates, hour=0, lead_time=0)
print(ds)

# Single chemistry variable (example: 2 m TMP if present in selected product).
ds_var = reader.open_chem(
  dates=dates,
  short_name="TMP",
  type_of_first_fixed_surface=103,
  value_of_first_fixed_surface=2,
)

# Multiple variables by shortName list.
ds_multi = reader.open_chem(
  dates=dates,
  short_name=["totAOD550", "DUST", "TMP"],
)

# All chemistry variables in the file(s): leave short_name unset.
ds_all = reader.open_chem(dates=dates)
```

By default this helper uses:

- `product="aerosol"` (GEFS chemistry path)
- `engine="grib2io"` (enforced by the NCEP reader)
- `use_icechunk=True`
- `storage_options={"anon": True}`

The AOD550 shortcut additionally applies:

- `filters={"shortName": "totAOD550", "typeOfFirstFixedSurface": 10}`
