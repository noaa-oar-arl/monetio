"""Generalized GRIB2 Reader using grib2io"""

import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, register_reader
from .sat_utils import update_history


@register_reader("grib2")
class Grib2Reader(GriddedReader):
    """
    Generalized Reader for GRIB2 files using the grib2io engine.
    """

    def open_dataset(
        self,
        files: str | list[str],
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        engine: str = "grib2io",
        filters: dict | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads GRIB2 files using xarray and grib2io.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path, list of paths, or glob pattern.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr to create a virtual Zarr dataset, by default False.
        virtualizarr_file : str or None, optional
            Path to save/load the VirtualiZarr reference JSON file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use (e.g. 'hdf5', 'netcdf3', 'zarr', 'grib2').
        virtualizarr_backend : str, optional
            Backend for VirtualiZarr references ("kerchunk" or "icechunk"), by default "kerchunk".
        icechunk_repo : str or None, optional
            Path to the Icechunk repository, by default None.
        use_icechunk : bool, optional
            Whether to use Icechunk, by default False.
        icechunk_url : str or None, optional
            Path to the Icechunk repository, by default None.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        engine : str, optional
            The xarray engine to use, by default "grib2io".
        filters : dict, optional
            Filters to pass to the engine (if supported), by default None.
        **kwargs : dict
            Additional arguments passed to xarray.open_mfdataset or the driver.

        Returns
        -------
        xr.Dataset
            The processed GRIB2 dataset.
        """
        if "engine" not in kwargs:
            kwargs["engine"] = engine
        if filters is not None:
            kwargs.setdefault("filters", filters)
            if "backend_kwargs" in kwargs and isinstance(kwargs["backend_kwargs"], dict):
                kwargs["backend_kwargs"].setdefault("filters", filters)

        # Apply safe defaults for remote S3 GRIB2 scans.
        file_list = [files] if isinstance(files, str) else list(files)
        is_s3 = any(str(f).startswith("s3://") for f in file_list)
        if is_s3:
            storage_options = dict(kwargs.get("storage_options", {}))
            storage_options.setdefault("anon", True)
            kwargs["storage_options"] = storage_options
            kwargs.setdefault("max_workers", 4)
            kwargs.setdefault("network_timeout", 300)
            kwargs.setdefault("max_concurrent_requests", 2)

        # Use the driver to open files
        # XarrayDriver handles S3, multiple files, etc.
        ds = self.driver.open(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="grib2",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        # Standardize and Harmonize
        ds = self.harmonize(ds)
        ds = _ensure_time_dimension(ds)

        # Update history
        ds = update_history(ds, f"Read GRIB2 data using {engine}.")

        return ds

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Harmonize GRIB2 metadata to monetio standards.

        Parameters
        ----------
        ds : xr.Dataset
            Input GRIB2 dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # 1. Coordinate Renaming (common in GRIB2)
        rename_dict = {
            "latitude": "latitude",
            "longitude": "longitude",
            "lat": "latitude",
            "lon": "longitude",
            "lat_0": "latitude",
            "lon_0": "longitude",
            "time": "time",
            "step": "step",
        }

        actual_rename = {}
        for k, v in rename_dict.items():
            if k in ds.variables or k in ds.dims:
                if v in ds.dims and k != v:
                    continue
                actual_rename[k] = v

        if actual_rename:
            ds = ds.rename(actual_rename)

        # 1b. Normalize GRIB valid_time -> time consistently.
        if "valid_time" in ds.coords or "valid_time" in ds.dims:
            if "time" in ds.coords or "time" in ds.dims or "time" in ds.variables:
                if "valid_time" in ds.variables:
                    ds = ds.drop_vars("valid_time")
            else:
                if "valid_time" in ds.coords and "valid_time" not in ds.dims:
                    valid_time_dims = ds["valid_time"].dims
                    if len(valid_time_dims) == 1 and valid_time_dims[0] in ds.dims:
                        ds = ds.swap_dims({valid_time_dims[0]: "valid_time"})
                ds = ds.rename({"valid_time": "time"})

        # 2. Ensure latitude/longitude are coordinates
        coord_vars = [v for v in ["latitude", "longitude", "time"] if v in ds.variables]
        if coord_vars:
            ds = ds.set_coords(coord_vars)

        # 3. Scientific Hygiene: Strip whitespace from string attributes
        for var in ds.variables:
            for attr, val in ds[var].attrs.items():
                if isinstance(val, str):
                    ds[var].attrs[attr] = val.strip()

        return ds
