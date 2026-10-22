# GRIB2 VirtualiZarr Reading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable VirtualiZarr-backed lazy reading for GRIB2 files in MONETIO, fully integrating with `grib2io.kerchunk.ReferenceGenerator`.

**Architecture:** We will update `XarrayDriver.open` inside `monetio/readers/drivers.py` to intercept GRIB2 requests when `use_virtualizarr=True`. We'll build a virtual dataset (VDS) via the grib2io ReferenceGenerator and VirtualiZarr's `open_virtual_dataset`, register a `LocalStore` for loading the manifest file, handle caching and Icechunk storage, and materialize the dataset lazily using the Zarr reference filesystem.

**Tech Stack:** Python 3.12, monetio, grib2io, virtualizarr, obstore, obspec_utils, fsspec, xarray, pytest.

## Global Constraints
*   **REQ-01:** Support `use_virtualizarr=True` with GRIB2 files using `grib2io.kerchunk.ReferenceGenerator` to generate metadata references.
*   **REQ-02:** Support passing `filters` (a dictionary) and `max_workers` (an integer) to the GRIB2 ReferenceGenerator.
*   **REQ-03:** Maintain standard cache behavior via `virtualizarr_file` if provided. If the file exists, load references from it; if not, generate them and write the result back.
*   **REQ-04:** Ensure compatibility with `use_icechunk=True` by writing the virtual dataset (`vds`) directly to the specified Icechunk storage.
*   **REQ-05:** Preserve backwards compatibility by emitting a `DeprecationWarning` when `use_virtualizarr=True` is supplied alongside `engine="grib2io"`, but correctly execute the VirtualiZarr GRIB2 path rather than ignoring it or routing to native/eager paths.

---

### Task 1: Update XarrayDriver Routing Logic

**Files:**
- Modify: `monetio/readers/drivers.py:530-555`

**Interfaces:**
- Consumes: `use_virtualizarr` and `engine` in `xr_kwargs`
- Produces: Bypasses the native GRIB2 short-circuit block when `use_virtualizarr=True`.

- [ ] **Step 1: Locate native GRIB2 routing block**
  Review lines 530-555 in `monetio/readers/drivers.py`. The native GRIB2 path currently intercepts ALL requests with `xr_kwargs.get("engine") == "grib2io"`.

- [ ] **Step 2: Modify the condition**
  Update the GRIB2 engine check to only intercept requests when `use_virtualizarr` is `False`.
  ```python
  if xr_kwargs.get("engine") == "grib2io" and not use_virtualizarr:
  ```

- [ ] **Step 3: Modify the Deprecation Warning**
  Update the warning when `engine="grib2io"` is used with `use_virtualizarr=True` inside the `if use_virtualizarr:` block (not in the native path block).
  We will raise a `DeprecationWarning` notifying the user that `engine="grib2io"` with `use_virtualizarr=True` is redirected to the VirtualiZarr pipeline.
  ```python
  if parser_name == "grib2" and xr_kwargs.get("engine") == "grib2io":
      warnings.warn(
          "For engine='grib2io', use_virtualizarr is redirected to the VirtualiZarr GRIB2 pipeline.",
          DeprecationWarning,
          stacklevel=2,
      )
  ```

---

### Task 2: Implement the VirtualiZarr GRIB2 Pipeline

**Files:**
- Modify: `monetio/readers/drivers.py` (replace old GRIB2 short-circuit block inside `use_virtualizarr`)

**Interfaces:**
- Consumes: GRIB2 files, `virtualizarr_file`, `use_icechunk`, `icechunk_url`, `filters`, `max_workers`
- Produces: An opened lazy `xr.Dataset` using VirtualiZarr references.

- [ ] **Step 1: Replace old GRIB2 short-circuit logic**
  Replace the block under `if parser_name == "grib2":` inside the `if use_virtualizarr:` section of `XarrayDriver.open` with the new pipeline logic:
  ```python
            if parser_name == "grib2":
                try:
                    import grib2io
                    from grib2io.kerchunk import ReferenceGenerator
                except ImportError:
                    raise ImportError("grib2io is required for GRIB2 VirtualiZarr reading.")

                try:
                    import ujson
                    import zarr
                    from virtualizarr import open_virtual_dataset
                    from virtualizarr.parsers import KerchunkJSONParser
                except ImportError:
                    raise ImportError(
                        "VirtualiZarr support for GRIB2 requires virtualizarr, obstore, obspec_utils, ujson, and zarr."
                    )

                # --- Kerchunk cache: load existing refs if available ---
                refs = None
                if (
                    not use_icechunk
                    and virtualizarr_file is not None
                    and os.path.exists(virtualizarr_file)
                ):
                    try:
                        with open(virtualizarr_file) as f_ref:
                            refs = ujson.load(f_ref)
                    except Exception as e:
                        warnings.warn(f"Failed to load virtualizarr_file {virtualizarr_file}: {e}")
                        refs = None

                if refs is None:
                    # Pop GRIB2-specific parameters to avoid reaching xarray
                    filters = xr_kwargs.pop("filters", None)
                    max_workers = xr_kwargs.pop("max_workers", None)
                    storage_options = dict(xr_kwargs.get("storage_options", {}))

                    # 1. Generate the reference manifest using grib2io
                    gen = ReferenceGenerator(
                        file_paths=file_list,
                        filters=filters,
                        storage_options=storage_options,
                        max_workers=max_workers,
                    )
                    manifest = gen.generate()

                    # 2. Write manifest to virtualizarr_file or temporary JSON
                    manifest_path_str = None
                    if virtualizarr_file is not None:
                        manifest_path_str = virtualizarr_file
                    else:
                        import tempfile
                        fd, temp_path_str = tempfile.mkstemp(suffix=".json", prefix="grib2_manifest_")
                        os.close(fd)
                        manifest_path_str = temp_path_str

                    try:
                        with open(manifest_path_str, "w") as f:
                            ujson.dump(manifest, f)

                        # 3. Resolve store registry and register LocalStore
                        registry, _ = _select_store(file_list, storage_options)
                        from obstore.store import LocalStore
                        registry.register("file:///", LocalStore(prefix="/"))

                        # 4. Open virtual dataset
                        manifest_file = pathlib.Path(manifest_path_str).resolve()
                        manifest_url = manifest_file.as_uri()

                        vds = _call_with_retries(
                            open_virtual_dataset,
                            url=manifest_url,
                            registry=registry,
                            parser=KerchunkJSONParser(),
                            loadable_variables=[],
                            attempts=retry_attempts,
                            base_sleep=retry_base_sleep,
                        )

                        # 5. Route to icechunk or kerchunk
                        if use_icechunk:
                            ds = _open_via_icechunk(vds, icechunk_url, virtualizarr_file)
                            if preprocess:
                                ds = preprocess(ds)
                            return ds

                        refs = vds.vz.to_kerchunk()

                        # Write refs to cache if requested
                        if virtualizarr_file is not None:
                            try:
                                with open(virtualizarr_file, "w") as f_ref:
                                    ujson.dump(refs, f_ref)
                            except Exception as e:
                                warnings.warn(f"Failed to save virtualizarr_file {virtualizarr_file}: {e}")

                    finally:
                        if virtualizarr_file is None and manifest_path_str is not None:
                            try:
                                os.remove(manifest_path_str)
                            except Exception:
                                pass

                # --- Materialize Dataset via reference mapper ---
                remote_protocol = "file"
                remote_options = {}
                if file_list[0].startswith("s3://"):
                    remote_protocol = "s3"
                    remote_options = dict(xr_kwargs.get("storage_options", {}))
                    if "anon" not in remote_options:
                        remote_options["anon"] = True
                elif file_list[0].startswith("http"):
                    remote_protocol = "http"

                mapper = fsspec.get_mapper(
                    "reference://",
                    fo=refs,
                    remote_protocol=remote_protocol,
                    remote_options=remote_options,
                )

                # Clean up open_mfdataset-only / virtualizarr kwargs
                mfdataset_keys = [
                    "combine",
                    "concat_dim",
                    "parallel",
                    "compat",
                    "data_vars",
                    "coords",
                    "ids",
                    "infer_order",
                    "join",
                ]
                for key in mfdataset_keys:
                    xr_kwargs.pop(key, None)

                xr_kwargs.pop("engine", None)
                xr_kwargs.pop("use_virtualizarr", None)
                xr_kwargs.pop("virtualizarr_backend", None)
                xr_kwargs.pop("virtualizarr_file", None)
                xr_kwargs.pop("virtualizarr_parser", None)
                xr_kwargs.pop("icechunk_url", None)
                xr_kwargs.pop("icechunk_repo", None)

                return _call_with_retries(
                    xr.open_dataset,
                    mapper,
                    attempts=retry_attempts,
                    base_sleep=retry_base_sleep,
                    engine="zarr",
                    consolidated=False,
                    **xr_kwargs,
                )
  ```

---

### Task 3: Implement Pipeline Unit Tests & Warnings Verification

**Files:**
- Modify: `tests/test_drivers_grib2io_s3.py`

**Interfaces:**
- Consumes: Mocks of `ReferenceGenerator`, `open_virtual_dataset`, `KerchunkJSONParser`
- Produces: Passing test suite with high coverage for GRIB2 VirtualiZarr behavior.

- [ ] **Step 1: Update the previous warnings test**
  Modify `test_grib2_virtual_flag_warns_and_uses_native_path` (rename to `test_grib2_virtual_flag_warns_and_routes_to_virtualizarr`) to assert that it raises the `DeprecationWarning` indicating redirection to the VirtualiZarr pipeline, and mock the VirtualiZarr call so that it passes successfully.

- [ ] **Step 2: Add pipeline execution unit test**
  Add `test_grib2_virtualizarr_pipeline_execution` verifying that `ReferenceGenerator.generate()`, registry configuration, `open_virtual_dataset()`, and final `xr.open_dataset()` are called with correct arguments.

- [ ] **Step 3: Add cached references unit test**
  Add `test_grib2_virtualizarr_pipeline_with_cached_refs` verifying that if `virtualizarr_file` already exists, cached references are read directly and `ReferenceGenerator.generate()` is bypassed.

- [ ] **Step 4: Add Icechunk integration unit test**
  Add `test_grib2_virtualizarr_with_icechunk` verifying that `use_icechunk=True` routes the virtual dataset `vds` successfully to `_open_via_icechunk`.

- [ ] **Step 5: Run pytest and verify everything passes**
  Command: `.venv/bin/pytest tests/test_drivers_grib2io_s3.py`
