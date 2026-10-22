# Tasks: Native grib2io Kerchunk/Icechunk GRIB2 Read Path
## Feature Reference: NOAA-MDL/grib2io PR #246

This task list implements `specs/requirements.md` and `specs/design.md`.
No implementation code should begin until this task list is approved.

---

## 1. Implementation Tasks

## T1. Add Native GRIB2 Dispatch in Driver
- Objective: Route `engine="grib2io"` to direct xarray backend calls.
- Files:
  - `monetio/readers/drivers.py`
- Work:
  - Add private helper `_open_grib2io_native(...)`.
  - In `XarrayDriver.open(...)`, short-circuit when `engine == "grib2io"`.
  - Use `xr.open_dataset` for one file and `xr.open_mfdataset` for many.
  - Preserve lazy behavior with guarded default `chunks={}` only when needed.
- Requirements covered: REQ-F01, REQ-F05, REQ-NF01.
- Acceptance mapping: AC-01, AC-03.

## T2. Preserve and Pass Through Native Backend Parameters
- Objective: Forward all relevant grib2io kwargs unchanged.
- Files:
  - `monetio/readers/drivers.py`
- Work:
  - Ensure pass-through for `filters`, `use_icechunk`, `storage_options`,
    `max_workers`, `network_timeout`, `max_concurrent_requests`, `chunks`.
  - Keep unknown backend kwargs pass-through compatible with xarray.
- Requirements covered: REQ-F02, REQ-F08, REQ-NF02.
- Acceptance mapping: AC-01, AC-06.

## T3. Implement Backward-Compatibility Argument Translation
- Objective: Keep legacy args functional with deprecation warnings.
- Files:
  - `monetio/readers/drivers.py`
- Work:
  - Translate `virtualizarr_backend="icechunk"` to `use_icechunk=True`.
  - Translate `icechunk_repo` to `icechunk_url` when provided.
  - Emit `DeprecationWarning` for legacy arguments.
  - If `engine="grib2io"` and `use_virtualizarr=True`, warn and ignore
    VirtualiZarr path.
- Requirements covered: REQ-F03, REQ-F04, REQ-NF03.
- Acceptance mapping: AC-02.

## T4. Keep Retry and Error Wrapping Semantics
- Objective: Maintain robust transient-failure handling.
- Files:
  - `monetio/readers/drivers.py`
- Work:
  - Wrap native open calls using existing `_call_with_retries`.
  - Preserve top-level `OSError` wrapping and chained causes.
  - Confirm deterministic argument errors are not repeatedly retried.
- Requirements covered: REQ-F09, REQ-F10, REQ-NF05.
- Acceptance mapping: AC-06.

## T5. Apply Targeted Warning Hygiene
- Objective: Reduce noisy warnings without hiding actionable issues.
- Files:
  - `monetio/readers/drivers.py`
- Work:
  - Add narrow `warnings.catch_warnings()` around native open path.
  - Filter only known noisy optional-dependency warnings.
  - Add/adjust tests verifying warning behavior boundaries.
- Requirements covered: REQ-NF04.
- Acceptance mapping: AC-06.

## T6. Confirm GRIB2 Reader Pass-Through Integrity
- Objective: Ensure reader wrappers still route correctly after dispatch change.
- Files:
  - `monetio/readers/grib2.py`
  - `monetio/readers/base.py`
  - `monetio/readers/ncep_pds.py`
  - `monetio/readers/gfs.py`
  - `monetio/readers/nam.py`
  - `monetio/readers/rap.py`
  - `monetio/readers/rrfs.py`
- Work:
  - Validate no signature changes needed.
  - Ensure existing defaults and kwargs continue to flow into driver.
  - Update docstrings/comments only where clarification is necessary.
- Requirements covered: REQ-F06, REQ-F07, REQ-F08.
- Acceptance mapping: AC-01, AC-05.

---

## 2. Test Tasks

## T7. Add Driver Dispatch Unit Tests
- Objective: Verify dispatch and call signatures.
- Files:
  - `tests/test_drivers_grib2io_s3.py`
  - optionally `tests/test_duplicate_handling.py` (if dispatch assertions belong there)
- Work:
  - Monkeypatch xarray open calls and assert:
    - single-file uses `open_dataset`
    - multi-file uses `open_mfdataset`
    - VirtualiZarr helpers are not called for `engine="grib2io"`
  - Assert pass-through kwargs are unchanged.
- Requirements covered: REQ-F01, REQ-F02, REQ-F05.
- Acceptance mapping: AC-01, AC-03.

## T8. Add Compatibility/Deprecation Tests
- Objective: Lock in legacy behavior compatibility.
- Files:
  - `tests/test_grib2.py`
  - `tests/test_drivers_grib2io_s3.py`
- Work:
  - Assert warnings for `virtualizarr_backend` and `icechunk_repo`.
  - Assert legacy arguments still produce successful open path.
- Requirements covered: REQ-F03, REQ-F04.
- Acceptance mapping: AC-02.

## T9. Add Native Filters + Icechunk Toggle Tests
- Objective: Validate user-facing grib2io controls.
- Files:
  - `tests/test_gfs.py` or existing GEFS-specific test file
  - `tests/test_grib2.py`
- Work:
  - Verify `filters` from helper APIs propagate correctly.
  - Verify `use_icechunk=True` and `False` both route natively.
- Requirements covered: REQ-F02, REQ-F08.
- Acceptance mapping: AC-01, AC-04.

## T10. Regression Tests for Non-GRIB2 Engines
- Objective: Ensure no behavioral regressions outside GRIB2.
- Files:
  - existing relevant tests already covering netCDF/zarr paths
- Work:
  - Run/select smoke subset that exercises non-GRIB2 readers and drivers.
- Requirements covered: REQ-NF02.
- Acceptance mapping: AC-05.

---

## 3. Validation Tasks (mdt Conda Environment)

## T11. Quality Gates
- Objective: Ensure formatting/lint/type quality remains clean.
- Commands:
  - `pre-commit run --all-files`

## T12. Focused GRIB2 Test Execution
- Objective: Validate GRIB2-native route and compatibility.
- Commands:
  - `conda run -n mdt pytest tests/test_grib2.py tests/test_drivers_grib2io_s3.py -v`

## T13. Optional Benchmark Replay
- Objective: Compare MONETIO wrapper overhead against direct xarray.
- Commands:
  - `conda run -n mdt python /tmp/monetio_grib2io_bench_noice.py`
- Success signal:
  - `ratio_monetio_over_direct` trends near 1.0.

Requirements covered: REQ-NF06.
Acceptance mapping: AC-07.

---

## 4. Execution Order and Dependencies

1. Complete T1-T5 in `drivers.py` before reader/test updates.
2. Complete T6 to confirm wrappers need no API changes.
3. Implement tests T7-T10.
4. Run validation T11-T13 in `mdt` environment.
5. Iterate on failures until all acceptance criteria pass.

---

## 5. Definition of Done

All of the following must be true:
1. `engine="grib2io"` always uses native backend path in MONETIO.
2. Legacy args work with deprecation warnings.
3. Key backend kwargs pass through unchanged.
4. Retry/error semantics are preserved.
5. New/updated tests pass in `mdt` environment.
6. No regressions observed in selected non-GRIB2 tests.
