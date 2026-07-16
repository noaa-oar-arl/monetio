# Design Document: GRIB2 Reading with VirtualiZarr

**Date:** 2026-07-01  
**Feature:** GRIB2 Reading with VirtualiZarr Integration  
**Status:** Approved  

## 1. Objective
Enable high-performance, cloud-native metadata virtualization for GRIB2 files in MONETIO by integrating with `grib2io.kerchunk.ReferenceGenerator` and VirtualiZarr. This replaces the previous GRIB2 short-circuit logic with an actual VirtualiZarr-backed virtual dataset pipeline, allowing efficient, lazy, zero-copy access to both local and remote (S3) GRIB2 datasets.

## 2. Requirements & Acceptance Criteria
*   **REQ-01:** Support `use_virtualizarr=True` with GRIB2 files using `grib2io.kerchunk.ReferenceGenerator` to generate metadata references.
*   **REQ-02:** Support passing `filters` (a dictionary) and `max_workers` (an integer) to the GRIB2 ReferenceGenerator.
*   **REQ-03:** Maintain standard cache behavior via `virtualizarr_file` if provided. If the file exists, load references from it; if not, generate them and write the result back.
*   **REQ-04:** Ensure compatibility with `use_icechunk=True` by writing the virtual dataset (`vds`) directly to the specified Icechunk storage.
*   **REQ-05:** Preserve backwards compatibility by emitting a `DeprecationWarning` when `use_virtualizarr=True` is supplied alongside `engine="grib2io"`, but correctly execute the VirtualiZarr GRIB2 path rather than ignoring it or routing to native/eager paths.
*   **AC-01:** Passing `use_virtualizarr=True` with `engine="grib2io"` correctly builds the reference manifest and opens the dataset lazily.
*   **AC-02:** Existing tests in `tests/test_grib2.py` and `tests/test_drivers_grib2io_s3.py` continue to pass (with modified or added tests where necessary).

## 3. Detailed Architecture and Flow

```
                      +-----------------------------+
                      |   XarrayDriver.open(...)    |
                      +--------------+--------------+
                                     |
                       use_virtualizarr == True?
                                     |
                                    Yes
                                     |
                            Is Parser GRIB2?
                                     |
                                    Yes
                                     |
                      +--------------v--------------+
                      | Check cached references file|
                      +--------------+--------------+
                                     |
                              Cache Exists?
                               /           \
                             No            Yes
                             /               \
              +-------------v------------+  +v-------------------------+
              | Build manifest using     |  | Load cached references   |
              | ReferenceGenerator       |  | from JSON file           |
              +-------------+------------+  +------------+-------------+
                            |                            |
                     Write manifest to                   |
                     temp JSON file                      |
                            |                            |
              +-------------v------------+               |
              | Initialize Registry &    |               |
              | register LocalStore      |               |
              +-------------+------------+               |
                            |                            |
              +-------------v------------+               |
              | open_virtual_dataset     |               |
              | to produce VDS           |               |
              +-------------+------------+               |
                            |                            |
                      use_icechunk?                      |
                       /         \                       |
                     Yes          No                     |
                     /             \                     |
     +--------------v---+  +--------v---------+          |
     | _open_via_icechunk|  | Export refs to   |          |
     | and return       |  | Kerchunk dict    |          |
     +------------------+  +--------+---------+          |
                                    |                    |
                                    |                    |
                                    +---------+----------+
                                              |
                                     Construct fsspec
                                    reference mapper
                                              |
                                              v
                                    +--------------------+
                                    |  xr.open_dataset   |
                                    |  (engine="zarr")   |
                                    +--------------------+
```

### 3.1 Step-by-Step Execution Sequence

1.  **Intercepting GRIB2 VirtualiZarr Requests:**
    *   In `XarrayDriver.open`, if `use_virtualizarr=True`, check if the parser name resolves to `"grib2"` (based on file suffix or `engine="grib2io"`).
    *   If `engine="grib2io"`, emit a `DeprecationWarning` notifying the user that standard `engine="grib2io"` with `use_virtualizarr=True` has been redirected to the VirtualiZarr pipeline, and continue execution.
2.  **Determining Cached References:**
    *   If `not use_icechunk` and `virtualizarr_file` is specified and exists, load the references dictionary via `ujson.load()`.
3.  **Generating the Manifest:**
    *   If references are not cached:
        *   Import `ReferenceGenerator` from `grib2io.kerchunk`.
        *   Instantiate `ReferenceGenerator(file_paths=file_list, filters=filters, storage_options=storage_options, max_workers=max_workers)`.
        *   Invoke `gen.generate()` to obtain the raw manifest dictionary.
4.  **Writing to File and Building VDS:**
    *   Save the manifest dict to `virtualizarr_file` if provided, or otherwise write it to a temporary JSON file.
    *   Construct an `ObjectStoreRegistry` using `_select_store(file_list, storage_options)`.
    *   Ensure `"file:///"` is registered in the registry as `LocalStore(prefix="/")` so that VirtualiZarr can resolve and read the manifest file path.
    *   Open the virtual dataset with:
        ```python
        vds = open_virtual_dataset(
            url=manifest_url,
            registry=registry,
            parser=KerchunkJSONParser(),
            loadable_variables=[],
        )
        ```
5.  **Handling Icechunk Storage vs. Standard Zarr Mapper:**
    *   If `use_icechunk=True`, delegate to `_open_via_icechunk(vds, icechunk_url, virtualizarr_file)`.
    *   If `use_icechunk=False`, serialize the `vds` back to a Kerchunk references dictionary: `refs = vds.vz.to_kerchunk()`.
6.  **Xarray Materialization:**
    *   Clean up the GRIB2/VirtualiZarr-specific arguments from `xr_kwargs`.
    *   Construct the reference filesystem mapper: `fsspec.get_mapper("reference://", fo=refs, ...)`.
    *   Open and return the lazy dataset via `xr.open_dataset(mapper, engine="zarr", consolidated=False, **xr_kwargs)`.
7.  **Surgical Cleanup:**
    *   In a `finally` block, remove the temporary manifest file if we created one.

## 4. Testing Strategy
*   We will add new tests to `tests/test_drivers_grib2io_s3.py` to check the VirtualiZarr GRIB2 path under various configurations:
    *   `test_grib2_virtualizarr_pipeline_execution`: Mocks the external libraries and asserts that `ReferenceGenerator`, `open_virtual_dataset`, and `xr.open_dataset` are called in sequence with correct arguments.
    *   `test_grib2_virtualizarr_pipeline_with_cached_refs`: Verifies that cached references are loaded and the generator/VDS steps are skipped.
    *   `test_grib2_virtualizarr_with_icechunk`: Verifies that `use_icechunk=True` routes the virtual dataset correctly to `_open_via_icechunk`.
