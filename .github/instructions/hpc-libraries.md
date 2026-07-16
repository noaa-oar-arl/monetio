---
description: NWS Office of Modeling and Development - HPC Scientific Libraries (ESMF, PIO, NetCDF, Zarr)
applyTo: "**/*.{f90,F90,f,for,ftn,cpp,hpp,c,h,cc,hh,cxx,hxx,py}"
---

# Scientific Library Standards: ESMF, PIO, NetCDF, and Zarr

When generating code that interacts with Earth System data, strictly adhere to the following library-specific APIs, official documentation guidelines, and NOAA NWS operational standards.

## 1. Earth System Modeling Framework (ESMF)
* **Official Documentation Reference:** [ESMF Reference Manual](https://earthsystemmodeling.org/docs/release/latest/ESMF_refdoc/)
* **Return Codes:** EVERY call to an ESMF API must include and check the return code (e.g., `rc=rc`). Immediately catch and handle non-zero return codes using `ESMF_LogWrite` and issue an EE2-compliant `FATAL ERROR:`.
* **Data Hierarchies:** Respect the ESMF hierarchy: `ESMF_State` contains `ESMF_Field`s, which are built on `ESMF_Grid`s or `ESMF_Mesh`es. Do not bypass the State to pass raw arrays between coupled components.
* **Time Management:** Always use `ESMF_Time` and `ESMF_TimeInterval` for model clocks. Do not use native language datetime calculations or raw integer tick counters.

## 2. ParallelIO (PIO)
* **Official Documentation Reference:** [ParallelIO (PIO) C/Fortran API Documentation](https://ncar.github.io/ParallelIO/)
* **Initialization:** Ensure the PIO subsystem is correctly initialized with the communicator (`PIO_init`) before any read/write operations occur.
* **Decomposition:** Explicitly define the I/O decomposition (`PIO_initdecomp`) mapping the compute tasks to the I/O tasks.
* **Writing Data:** Use `PIO_write_darray` for distributed arrays. Never gather full 3D model arrays onto the root rank for serial writing; this will cause OOM (Out of Memory) crashes on operational nodes.

## 3. NetCDF (Parallel / HDF5 Backend)
* **Official Documentation Reference:** [NetCDF-C Users Guide](https://docs.unidata.ucar.edu/netcdf-c/current/) & [NetCDF-Fortran Library User's Guide](https://docs.unidata.ucar.edu/netcdf-fortran/current/)
* **Format:** Default to the NetCDF4/HDF5 backend (`NC_NETCDF4`).
* **Parallel Access:** When opening files in an MPI environment, strictly use the parallel API (`nc_open_par` or `nf90_open` with the `MPI_Comm` and `MPI_Info` arguments).
* **Collective vs. Independent I/O:** Explicitly set the parallel access mode (`nc_var_par_access`). Default to collective I/O (`NC_COLLECTIVE`) for large array writes to utilize MPI aggregators efficiently.
* **Compression:** Enable deflation (`nc_def_var_deflate`) for operational output files to save `$DATA` and `$COMOUT` disk space, ensuring chunking (`nc_def_var_chunking`) is optimized for the typical read-pattern.

## 4. Zarr (Cloud & Distributed Storage)
* **Official Documentation Reference:** [Zarr Storage Specification](https://zarr.readthedocs.io/)
* **Parallel Write Safety:** When writing or appending Xarray datasets to a Zarr store via Dask, always use `compute=False` to build a lazy task graph first, or use explicit locks if multiple ranks are updating the same indices concurrently.
* **Optimized Chunking:** Align Zarr chunks with downstream query and analysis patterns. Never use chunk sizes that are too small (creates excessive metadata files on disk/$DATA) or too large (causes memory bottlenecks). Aim for chunks between 100MB and 500MB in uncompressed size.
* **Consolidated Metadata:** Always append `.to_zarr(..., consolidated=True)` when finalizing a Zarr dataset. This ensures metadata is stored in a single JSON key, preventing costly, serial file-system crawls across object storage or parallel filesystems during reads.

## 5. NCEPLIBS (w3emc, bacio, sp, g2c)
* **Official Documentation Reference:** [NCEPLIBS GitHub Organization Docs](https://github.com/NOAA-EMC/NCEPLIBS)
* **BACIO (Binary I/O):** Use NCEP `bacio` routines for bitwise byte swapping and asynchronous block I/O tasks when dealing with legacy raw observational streams.
* **W3EMC (Time/Grid Utilities):** Rely on `w3emc` subroutines for operational time conversion, GRIB mapping, and standard spectral filtering tasks rather than reinventing custom algorithms.
