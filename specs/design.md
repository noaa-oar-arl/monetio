# Design: Native grib2io Kerchunk/Icechunk GRIB2 Read Path
## Feature Reference: NOAA-MDL/grib2io PR #246

---

## 1. Design Goals

1. Route all MONETIO GRIB2 reads through the native grib2io xarray backend
   (`engine="grib2io"`) with full option pass-through.
2. Remove redundant VirtualiZarr orchestration for GRIB2 while preserving
   existing non-GRIB2 behavior.
3. Maintain backward compatibility for legacy MONETIO arguments and warnings.
4. Preserve defensive retry behavior for transient cloud/network failures.
5. Keep implementation low-risk and minimally invasive to current public APIs.

---

## 2. Current vs Target Architecture

### 2.1 Current GRIB2 Path (MONETIO)

For many GRIB2 cloud reads, MONETIO currently may:
1. Expand paths in `FileUtility.expand_paths`.
2. Enter `XarrayDriver.open(..., use_virtualizarr=True/False, ...)`.
3. If virtual route selected, build object-store registry via `_select_store`.
4. Invoke `virtualizarr.open_virtual_mfdataset`.
5. Optionally serialize refs to JSON.
6. Optionally persist refs to Icechunk via `_open_via_icechunk`.
7. Re-open through `xr.open_dataset(..., engine="zarr")`.

This duplicates capability already provided by grib2io PR #246 and introduces
extra moving parts.

### 2.2 Target GRIB2 Path (Native)

When `engine == "grib2io"`, MONETIO will:
1. Expand paths and normalize options.
2. Forward directly to:
   - `xr.open_dataset(...)` for single input
   - `xr.open_mfdataset(...)` for multiple inputs
3. Pass through native grib2io backend arguments:
   `use_icechunk`, `filters`, `storage_options`, `max_workers`,
   `network_timeout`, `max_concurrent_requests`, `chunks`, and other backend kwargs.
4. Wrap call in existing retry helper `_call_with_retries`.

Non-GRIB2 paths continue to use existing MONETIO behavior.

---

## 3. Detailed Design

## 3.1 `monetio/readers/drivers.py` (Primary change)

### 3.1.1 New Internal Branch Function
Add a private helper to centralize native GRIB2 delegation:

- `_open_grib2io_native(file_list: list[str], xr_kwargs: dict, use_dask: bool, use_cubed: bool, retry_attempts: int, retry_base_sleep: float) -> xr.Dataset`

Responsibilities:
1. Ensure `xr_kwargs["engine"] = "grib2io"`.
2. Inject default `chunks={}` only when lazy mode requested and no explicit chunks provided.
3. Preserve and forward backend parameters untouched.
4. Choose `xr.open_dataset` vs `xr.open_mfdataset` by file count.
5. Apply `_call_with_retries` around open call.
6. Return dataset without triggering eager load.

### 3.1.2 Native Dispatch Condition
Inside `XarrayDriver.open(...)`, after kwargs normalization and path expansion,
introduce early dispatch:

- Determine `engine = xr_kwargs.get("engine")`
- If `engine == "grib2io"`, call `_open_grib2io_native(...)` and short-circuit.

This short-circuit bypasses MONETIO VirtualiZarr/obspec_utils for GRIB2 only.

### 3.1.3 Legacy Argument Translation
Current signature includes deprecated parameters:
- `virtualizarr_backend`
- `icechunk_repo`
- `icechunk_url`
- `use_icechunk`

For `engine == "grib2io"`:
1. If `virtualizarr_backend == "icechunk"`: emit `DeprecationWarning`, set `use_icechunk=True`.
2. If `icechunk_repo is not None`: emit `DeprecationWarning`, map to `icechunk_url`.
3. If `icechunk_url` present and `use_icechunk` absent: set `use_icechunk=True`.
4. Forward only modern keys (`use_icechunk`, `icechunk_url` if backend supports it).

### 3.1.4 `use_virtualizarr=True` With GRIB2
If caller sets both:
- `engine == "grib2io"`
- `use_virtualizarr == True`

Emit `DeprecationWarning`:
"For engine='grib2io', use_virtualizarr is ignored; native backend path is used."

Then proceed with native grib2io path.

### 3.1.5 Storage Defaults
For S3 inputs in GRIB2 route:
- If `storage_options` missing, inject public-safe default (`{"anon": True}`),
  consistent with current `NCEPPDSReader` behavior.
- Do not overwrite explicit user-provided storage options.

### 3.1.6 Warning Hygiene
At native dispatch boundary, add targeted warning filtering to suppress known noisy
deprecation/future warnings from optional icechunk internals while preserving
actionable user warnings.

Approach:
- Use local `warnings.catch_warnings()` around open call.
- Filter specific message patterns only (not blanket suppression).

### 3.1.7 Retry Semantics
Use existing `_call_with_retries` wrapper for both single and multi-file opens.
No new retry utility is added.

---

## 3.2 `monetio/readers/base.py`

No signature-breaking changes required.

`GriddedReader.open_dataset(...)` already forwards:
- `use_icechunk`
- `icechunk_url`
- deprecated args
- `use_dask`

Design action:
- Keep signatures unchanged.
- Update docstrings to clarify that for GRIB2 these options route to native
  grib2io xarray backend behavior.

---

## 3.3 `monetio/readers/grib2.py`

`Grib2Reader.open_dataset(...)` already sets `engine="grib2io"` default and
forwards options to driver.

Design action:
- Preserve method signature and defaults.
- Ensure `filters` are forwarded unchanged as backend kwargs.
- Keep harmonization and history update unchanged.

---

## 3.4 NCEP Reader Family

Files:
- `monetio/readers/ncep_pds.py`
- `monetio/readers/gfs.py`
- `monetio/readers/nam.py`
- `monetio/readers/rap.py`
- `monetio/readers/rrfs.py`

Design action:
- No external API changes.
- Preserve existing remote safety defaults in `NCEPPDSReader`:
  `storage_options`, `max_workers`, `network_timeout`,
  `max_concurrent_requests`, retry knobs.
- Ensure these are not consumed before reaching native grib2io call.

---

## 4. Data Flow (Target)

1. User invokes reader (`GEFSReader.open_dataset`, `Grib2Reader.open_dataset`, etc.).
2. Reader forwards to `GriddedReader` then `XarrayDriver.open`.
3. `XarrayDriver.open` expands file paths and normalizes kwargs.
4. If `engine == "grib2io"`, driver enters native branch.
5. Driver calls xarray open function with full backend kwargs + retry wrapper.
6. Dataset returned; existing harmonize/history logic remains unchanged.

---

## 5. Error Handling Design

1. Keep top-level `OSError("XarrayDriver failed to open files...")` wrapping.
2. Preserve original exception as chained cause (`from e`).
3. Retry only transient network signatures using existing helper.
4. Do not retry deterministic schema/argument errors.

---

## 6. Test Design

## 6.1 Unit Tests (Driver Dispatch)

Create/extend tests to verify:
1. `engine="grib2io"` triggers native path and bypasses VirtualiZarr internals.
2. Single file uses `xr.open_dataset`; multi file uses `xr.open_mfdataset`.
3. `filters`, `use_icechunk`, `storage_options`, and perf kwargs pass through untouched.
4. `use_virtualizarr=True` + GRIB2 emits deprecation warning and still works.
5. `icechunk_repo` translation emits deprecation warning.

Technique:
- Monkeypatch xarray open functions and assert call signatures.

## 6.2 Reader-Level Tests

For `GEFSReader` and `Grib2Reader`:
1. Verify `open_dataset(..., engine="grib2io")` returns expected dataset shape/vars.
2. Verify `open_chem` and `open_aerosol_aod550` filter propagation.

## 6.3 Integration Smoke (S3 Public)

In `mdt` env, run focused tests for public NOAA buckets with anonymous storage.
Skip gracefully when optional deps absent.

## 6.4 Regression Tests

Confirm non-GRIB2 paths (h5netcdf/netcdf/zarr) are unaffected.

---

## 7. Compatibility and Migration

1. Public reader method signatures remain stable.
2. Legacy options still accepted with deprecation warnings.
3. New preferred UX:
   - `engine="grib2io"`
   - `use_icechunk=True/False`
   - `filters={...}`
   - `storage_options={...}`

---

## 8. Risks and Mitigations

1. Risk: subtle breakage in VirtualiZarr-backed GRIB2 callers.
   Mitigation: deprecation warnings + explicit native routing tests.

2. Risk: optional icechunk dependency mismatch in user env.
   Mitigation: clear ImportError propagation and test skips.

3. Risk: warning suppression hides useful diagnostics.
   Mitigation: narrow pattern-based suppression only around known noisy messages.

4. Risk: performance regression for non-GRIB2 engines.
   Mitigation: native dispatch is guarded strictly by `engine == "grib2io"`.

---

## 9. Verification Plan (mdt environment)

Required verification sequence:

```bash
pre-commit run --all-files
conda run -n mdt pytest tests/test_grib2.py tests/test_drivers_grib2io_s3.py -v
```

Optional benchmark replay:

```bash
conda run -n mdt python /tmp/monetio_grib2io_bench_noice.py
```

Expected benchmark outcome:
- `ratio_monetio_over_direct` should approach 1.0 after native delegation.

---

## 10. Implementation Boundaries

This design intentionally does not:
1. Remove existing VirtualiZarr helper code for non-GRIB2.
2. Change harmonization semantics.
3. Add new public reader classes or CLI switches.

It only changes dispatch behavior for GRIB2 engine integration.
