# Requirements: Native grib2io Kerchunk/Icechunk GRIB2 Read Path
## Feature Reference: NOAA-MDL/grib2io PR #246

---

## 1. Problem Statement

MONETIO's current GRIB2 cloud-native read path routes through an independent
VirtualiZarr pipeline (`_select_store`, `open_virtual_mfdataset`, `obspec_utils`
registry, `_open_via_icechunk`) before handing off to xarray.  This path is
complex, fragile against upstream VirtualiZarr API changes, and duplicates work
that grib2io PR #246 now performs natively inside the xarray backend itself.

grib2io PR #246 exposes:

```python
xr.open_dataset(url, engine="grib2io", use_icechunk=True|False,
                storage_options=..., filters=...,
                max_workers=..., network_timeout=...,
                max_concurrent_requests=..., chunks=...)

xr.open_mfdataset(url_list, engine="grib2io", use_icechunk=True|False, ...)
```

The grib2io backend internally:
1. Fetches the `.idx` sidecar file to get byte offsets — no full GRIB2 scan.
2. Applies `filters` to select only the requested messages (~700× speedup on GFS).
3. Either keeps an in-process Icechunk virtual store (`use_icechunk=True`) or
   performs direct byte-range reads from S3/HTTP (`use_icechunk=False`).

MONETIO must be updated so that `engine="grib2io"` routes directly through
this native interface rather than the bespoke MONETIO VirtualiZarr stack.

---

## 2. Scope

### In Scope
- `monetio/readers/drivers.py` — `XarrayDriver.open()`
- `monetio/readers/base.py` — `GriddedReader.open_dataset()`
- `monetio/readers/grib2.py` — `Grib2Reader`
- `monetio/readers/ncep_pds.py` — `NCEPPDSReader`
- `monetio/readers/gfs.py` — `GFSReader`, `GEFSReader`, `GDASReader`
- `monetio/readers/nam.py` — `NAMReader`
- `monetio/readers/rap.py` — `RAPReader`
- `monetio/readers/rrfs.py` — `RRFSReader`
- Associated unit and integration tests

### Out of Scope
- NetCDF/HDF5/Zarr read paths (non-GRIB2 engines)
- `PandasDriver` (observation readers)
- WCOSS operational deployment (requires separate EE2 validation)
- grib2io PR #246 itself — MONETIO consumes the API, does not modify grib2io

---

## 3. Functional Requirements

### REQ-F01 — Native grib2io Route
When `engine="grib2io"` is passed to any MONETIO reader or `XarrayDriver.open()`,
the call **must** be delegated directly to `xr.open_dataset` or `xr.open_mfdataset`
with `engine="grib2io"` and all relevant grib2io-specific kwargs forwarded
(**without** triggering the MONETIO VirtualiZarr pipeline).

### REQ-F02 — Icechunk Toggle
A boolean parameter `use_icechunk` (default `False`) **must** be accepted by:
- `XarrayDriver.open()`
- `GriddedReader.open_dataset()`
- `NCEPPDSReader.open_dataset()`
- All concrete NCEP readers (GFS, GEFS, GDAS, NAM, RAP, RRFS)

When `use_icechunk=True`, `use_icechunk=True` **must** be forwarded to grib2io.
When `use_icechunk=False` (default), it **must** be forwarded as `use_icechunk=False`.

### REQ-F03 — Filter Pass-Through
A `filters` dict **must** be accepted and forwarded to grib2io.  When `filters`
is provided, it **must not** be consumed or modified by MONETIO before reaching
grib2io.

### REQ-F04 — Performance Parameters
The following grib2io performance parameters **must** be accepted and forwarded:
- `max_workers` (int | None)
- `network_timeout` (int, seconds)
- `max_concurrent_requests` (int | None)

### REQ-F05 — Dask / Chunking
`chunks` kwarg **must** be forwarded unchanged to `xr.open_dataset` /
`xr.open_mfdataset`.  When `use_dask=True` is set and `chunks` is absent,
MONETIO **must** inject `chunks={}` as today.

### REQ-F06 — Backward Compatibility
The existing `use_virtualizarr` parameter path **must** remain operational for
non-GRIB2 engines (HDF5/NetCDF, etc.).  Callers passing `engine="grib2io"` with
`use_virtualizarr=True` **must** receive a `DeprecationWarning` and be redirected
to the native grib2io path.

### REQ-F07 — Deprecated Icechunk Params
The existing `icechunk_repo`, `icechunk_url`, and `virtualizarr_backend="icechunk"`
parameters **must** continue to function for `engine="grib2io"` with a
`DeprecationWarning`, translating to `use_icechunk=True`.

### REQ-F08 — Storage Options
`storage_options` **must** be accepted and forwarded to grib2io for remote
(S3 / HTTP) paths.  Safe defaults (`{"anon": True}` for public S3 buckets)
**must** still be injected when `storage_options` is not provided by the caller.

### REQ-F09 — Multi-file Support
When multiple URLs are provided, `xr.open_mfdataset` **must** be used with
`engine="grib2io"`.  grib2io handles merging of multi-file datasets natively.

### REQ-F10 — Error Propagation
`OSError` wrapping of grib2io exceptions **must** be preserved so downstream
callers still receive a consistent error type from MONETIO.

---

## 4. Non-Functional Requirements

### REQ-NF01 — EE2 / Pangeo Stack
The implementation **must** remain backend-agnostic (Dask/NumPy).  No
`.compute()`, `.values`, or `.load()` calls may be introduced inside the driver.

### REQ-NF02 — Optional Dependency
`grib2io[icechunk]` and `grib2io[kerchunk]` extras are optional.  If they are
not installed, `use_icechunk=False` behaviour (direct reads) **must** still work.
A clear `ImportError` with install guidance **must** surface when
`use_icechunk=True` is requested but icechunk is absent.

### REQ-NF03 — Log Hygiene
`FutureWarning` and `DeprecationWarning` emitted by grib2io icechunk internals
(e.g. `LocalFileSystem storage is not safe`) **must** be actively suppressed via
`warnings.filterwarnings` at the driver boundary.

### REQ-NF04 — Retry Logic
Transient network errors (`IcechunkError`, timeout, DNS failure) **must** be
retried via the existing `_call_with_retries` helper.  Default: 3 attempts,
1-second base sleep with exponential back-off.

### REQ-NF05 — Test Coverage (Dual-Backend)
Each new or modified driver function **must** have `pytest` tests covering:
1. Eager NumPy path (`use_icechunk=False`, `chunks` absent)
2. Lazy Dask path (`use_dask=True` or `chunks={}`)
3. `use_icechunk=True` path (mocked or real if `grib2io[icechunk]` available)
4. `filters` pass-through
5. `DeprecationWarning` emission for legacy params

### REQ-NF06 — Verification Command
Per the Aero Protocol, after implementation run:
```
pre-commit run --all-files
```
and then:
```
conda run -n mdt pytest tests/test_grib2.py tests/test_drivers_grib2io_s3.py -v
```

---

## 5. Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-01 | `GFSReader().open_dataset(dates=...)` with `use_icechunk=True` completes without error against the `noaa-gfs-bdp-pds` public S3 bucket (using `mdt` env). |
| AC-02 | `GEFSReader().open_aerosol_aod550(...)` with `use_icechunk=True` returns an `xr.Dataset` with `totAOD550`. |
| AC-03 | A single-file local GRIB2 open via `Grib2Reader` produces the same data variables as a direct `xr.open_dataset(..., engine="grib2io")` call. |
| AC-04 | Passing `use_virtualizarr=True` with `engine="grib2io"` emits `DeprecationWarning` and still opens the file correctly. |
| AC-05 | Passing `icechunk_repo=...` emits `DeprecationWarning` and still opens the file correctly. |
| AC-06 | All existing tests in `tests/test_grib2.py` and `tests/test_drivers_grib2io_s3.py` pass. |
| AC-07 | `pre-commit run --all-files` exits 0. |
