"""TOLNet Reader"""

import pandas as pd
import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, register_reader
from .sat_utils import update_history


@register_reader("tolnet")
class TOLNetReader(GriddedReader):
    """
    Reader for TOLNet (Tropospheric Ocean Laboratory Network) lidar data.
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
        **kwargs,
    ) -> xr.Dataset:
        """
        Retrieve and load TOLNet data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s) or URL(s).
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
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The loaded TOLNet dataset.

        Examples
        --------
        >>> reader = TOLNetReader()
        >>> ds = reader.open_dataset(files="TOLNet_*.hdf5")
        """
        user_preprocess = kwargs.pop("preprocess", None)

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        # We use GriddedReader's driver (XarrayDriver) to open files.
        # XarrayDriver.open handles single/multiple files.
        # Since TOLNet files have groups, we use our custom read_method to merge them.
        def _read_single_tolnet(f, **inner_kwargs):
            return read_tolnet(f, **inner_kwargs)

        # Update kwargs to use our lazy reader as the primary method
        kwargs["read_method"] = _read_single_tolnet

        ds = self.driver.open(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="hdf5",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        if user_preprocess:
            ds = user_preprocess(ds)

        # Update history
        ds = update_history(ds, "Read TOLNet data using standardized preprocessing.")
        ds = _ensure_time_dimension(ds)

        return ds

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Apply TOLNet-specific naming conventions and metadata standardization.

        The heavy lifting is done in ``tolnet_preprocess`` which is called
        per-file during ``read_tolnet``.  This method applies any final
        dataset-level harmonization after files have been merged.

        Parameters
        ----------
        ds : xr.Dataset
            Input dataset (already preprocessed per-file).

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        return super().harmonize(ds)


def read_tolnet(fname: str, chunks=None, **kwargs) -> xr.Dataset:
    """
    Read a single TOLNet HDF5 file lazily.

    Parameters
    ----------
    fname : str
        File path or URL.
    chunks : dict, optional
        Chunk sizes for dask-backed lazy loading. Pass ``{}`` for automatic
        chunking or a mapping like ``{"time": 100}`` for explicit sizes.
        When *None* (default) the dataset is loaded eagerly.
    **kwargs : dict
        Additional arguments passed to xr.open_dataset.

    Returns
    -------
    xr.Dataset
        The TOLNet dataset from a single file.
    """
    # Filter kwargs to only those accepted by xr.open_dataset
    xr_keys = [
        "chunks",
        "decode_times",
        "decode_coords",
        "decode_cf",
        "mask_and_scale",
        "backend_kwargs",
    ]
    xr_kwargs = {k: v for k, v in kwargs.items() if k in xr_keys}

    # Forward the explicit chunks parameter (overrides any chunks in kwargs)
    if chunks is not None:
        xr_kwargs["chunks"] = chunks

    engine = kwargs.get("engine", "h5netcdf")

    # 1. Open DATA group
    try:
        ds_data = xr.open_dataset(
            fname, group="DATA", engine=engine, phony_dims="sort", **xr_kwargs
        )
    except Exception as e:
        # Fallback if group doesn't exist
        import warnings

        warnings.warn(f"Could not open group DATA in {fname}: {e}")
        return xr.Dataset()

    # 2. Open INSTRUMENT_ATTRIBUTES group (for attributes only)
    try:
        # We don't need chunks for attributes, and it might even fail if we pass them
        ds_atts = xr.open_dataset(fname, group="INSTRUMENT_ATTRIBUTES", engine=engine)
        # Copy attributes to ds_data
        ds_data.attrs.update(ds_atts.attrs)
    except Exception:
        pass

    # Now apply TOLNet-specific transformations lazily
    ds = tolnet_preprocess(ds_data)

    return ds


def tolnet_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess TOLNet dataset: standardize coordinates, handle time, and
    rename variables to standard conventions.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset (usually from the 'DATA' group).

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Identify and rename dimensions based on variables
    # Expected variables in TOLNet DATA group:
    # ALT: (z)
    # TIME_MID_UT_UNIX: (time) - in milliseconds since Epoch
    # O3MR: (z, time)

    dim_map = {}
    if "ALT" in ds.variables:
        alt_dim = ds["ALT"].dims[0]
        dim_map[alt_dim] = "z"
    if "TIME_MID_UT_UNIX" in ds.variables:
        time_dim = ds["TIME_MID_UT_UNIX"].dims[0]
        dim_map[time_dim] = "time"

    if dim_map:
        ds = ds.rename(dim_map)

    # Ensure coordinates exist as DataArrays before any transformation
    if "z" in ds.dims and "ALT" in ds.variables:
        # We must ensure z is 1D for coordinate assignment to work as a dimension coordinate
        z_vals = ds["ALT"]
        if z_vals.ndim > 1:
            # Pick first available if multi-dim (shouldn't happen for ALT in TOLNet DATA group)
            z_vals = z_vals.isel({d: 0 for d in z_vals.dims if d != "z"}, drop=True)
        # We must ensure z is 1D for coordinate assignment to work as a dimension coordinate.
        # We avoid .compute() to stay lazy.
        ds = ds.assign_coords(z=z_vals.astype(float))
    if "time" in ds.dims and "TIME_MID_UT_UNIX" in ds.variables:
        t_vals = ds["TIME_MID_UT_UNIX"]
        if t_vals.ndim > 1:
            t_vals = t_vals.isel({d: 0 for d in t_vals.dims if d != "time"}, drop=True)
        ds = ds.assign_coords(time=t_vals.astype(float))

    # 2. Handle Vertical Coordinate
    if "altitude" in ds.variables or "ALT" in ds.variables:
        if "ALT" in ds.variables:
            ds = ds.set_coords("ALT").rename({"ALT": "altitude"})
        ds["altitude"].attrs.update({"units": "m", "standard_name": "altitude"})
        if "z" in ds.dims:
            # We must ensure it's a 1D coordinate for merging
            z_vals = ds["altitude"]
            if z_vals.ndim > 1:
                z_vals = z_vals.isel({d: 0 for d in z_vals.dims if d != "z"}, drop=True)
            ds = ds.assign_coords(z=z_vals)

    # 3. Handle Time (Lazy)
    if "TIME_MID_UT_UNIX" in ds.variables:
        # Convert ms to seconds and then to datetime64[ns]
        t_raw = ds["TIME_MID_UT_UNIX"]
        # Use backend-agnostic conversion
        from .sat_utils import apply_lazy_conversion

        def _to_dt(t):
            return pd.to_datetime(t, unit="ms")

        ds["time"] = apply_lazy_conversion(t_raw, _to_dt, "datetime64[ns]")
        ds = ds.set_coords("time")
        if "time" in ds.dims:
            # For merging, time must be a coordinate.
            # But apply_lazy_conversion might return something that needs to be explicitly set.
            t_vals = ds["time"]
            if t_vals.ndim > 1:
                t_vals = t_vals.isel({d: 0 for d in t_vals.dims if d != "time"}, drop=True)
            ds = ds.assign_coords(time=t_vals.astype("datetime64[ns]"))

        if "TIME_MID_UT_UNIX" in ds.variables and "TIME_MID_UT_UNIX" not in ds.dims:
            ds = ds.drop_vars("TIME_MID_UT_UNIX")

    # 4. Handle Spatial Coordinates (Latitude/Longitude from Attributes)
    # TOLNet often stores these as strings like "39.0 N" or "76.5 W"
    try:
        lat_str = ds.attrs.get("Location_Latitude")
        lon_str = ds.attrs.get("Location_Longitude")

        if isinstance(lat_str, bytes | str):
            if isinstance(lat_str, bytes):
                lat_str = lat_str.decode("ascii")
            parts = lat_str.split()
            lat_val = float(parts[0])
            if len(parts) > 1 and parts[1].upper() == "S":
                lat_val *= -1.0
        else:
            lat_val = None

        if isinstance(lon_str, bytes | str):
            if isinstance(lon_str, bytes):
                lon_str = lon_str.decode("ascii")
            parts = lon_str.split()
            lon_val = float(parts[0])
            if len(parts) > 1 and parts[1].upper() == "W":
                lon_val *= -1.0
        else:
            lon_val = None

        if lat_val is not None and lon_val is not None:
            # Create 1x1 2D coordinates to follow satellite/gridded convention
            ds = ds.assign_coords(
                latitude=(("y", "x"), [[lat_val]], {"units": "degrees_north"}),
                longitude=(("y", "x"), [[lon_val]], {"units": "degrees_east"}),
            )
            ds["x"] = [0]
            ds["y"] = [0]
    except Exception:
        pass

    # 5. Mask missing values (-999, -990 are common)
    for var in ds.data_vars:
        ds[var] = ds[var].where(ds[var] > -900)

    # 6. Harmonize variable names (optional but good practice)
    mapping = {
        "O3MR": "ozone_mixing_ratio",
        "O3ND": "ozone_number_density",
        "O3NDUncert": "ozone_number_density_uncertainty",
        "O3MRUncert": "ozone_mixing_ratio_uncertainty",
        "O3NDResol": "ozone_vertical_resolution",
        "Press": "pressure",
        "Temp": "temperature",
        "AirND": "air_number_density",
    }
    rename_vars = {old: new for old, new in mapping.items() if old in ds.variables}
    if rename_vars:
        ds = ds.rename(rename_vars)

    # Update history
    ds = update_history(ds, "Preprocessed TOLNet data.")

    return ds
