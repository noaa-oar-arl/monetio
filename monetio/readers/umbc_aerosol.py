"""UMBC Aerosol Reader (CL51)"""

import warnings

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import apply_lazy_conversion, update_history


@register_reader("umbc_aerosol")
class UMBCAerosolReader(GriddedReader):
    """
    Reader for UMBC Aerosol (CL51 Ceilometer) data.
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
        Reads UMBC Aerosol HDF5 files.

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
            The UMBC Aerosol dataset.

        Examples
        --------
        >>> reader = UMBCAerosolReader()
        >>> ds = reader.open_dataset(files="UMBC_CL51_*.h5")
        """
        # UMBC CL51 HDF5 files have 'DATA' and 'Instrument_Attributes' groups
        groups = ["DATA", "Instrument_Attributes"]

        user_preprocess = kwargs.pop("preprocess", None)

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"
        dsets = []
        all_attrs = {}
        for g in groups:
            g_kwargs = kwargs.copy()
            g_kwargs["group"] = g
            try:
                # We open without the UMBC preprocess at this stage
                ds_g = super().open_dataset(
                    files,
                    use_virtualizarr=use_virtualizarr,
                    virtualizarr_file=virtualizarr_file,
                    virtualizarr_parser="hdf5",
                    virtualizarr_backend=virtualizarr_backend,
                    icechunk_repo=icechunk_repo,
                    use_icechunk=use_icechunk,
                    icechunk_url=icechunk_url,
                    use_dask=use_dask,
                    **g_kwargs,
                )
                dsets.append(ds_g)
                # Manually collect attributes from groups
                all_attrs.update(ds_g.attrs)
            except Exception as e:
                warnings.warn(f"Could not open group {g}: {e}")

        if not dsets:
            raise RuntimeError("No groups could be opened.")

        # Merge groups
        # We use compat='no_conflicts' as coordinates should be identical
        ds = xr.merge(dsets, compat="no_conflicts")
        ds.attrs.update(all_attrs)

        # Now apply UMBC preprocessing to the merged dataset
        ds = umbc_aerosol_preprocess(ds)

        if user_preprocess:
            ds = user_preprocess(ds)

        # Update history
        ds = update_history(ds, "Read UMBC Aerosol data.")

        return ds


def umbc_aerosol_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess UMBC Aerosol dataset: standardize coordinates and handle time.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset from merged 'DATA' and 'Instrument_Attributes' groups.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Handle renaming and dimensions
    if "Altitude_m" in ds.variables:
        ds = ds.rename({"Altitude_m": "altitude"})
        if ds["altitude"].ndim == 1:
            a_dim = ds["altitude"].dims[0]
            if a_dim != "z":
                ds = ds.rename({a_dim: "z"})
            ds = ds.set_coords("altitude")

    if "Profile_bsc" in ds.variables:
        ds = ds.rename({"Profile_bsc": "bsc"})

    if "UnixTime_UTC" in ds.variables:

        def _convert_time(t):
            return pd.to_datetime(t, unit="s").astype("datetime64[ns]")

        time_da = apply_lazy_conversion(ds["UnixTime_UTC"], _convert_time, "datetime64[ns]")
        u_dim = ds["UnixTime_UTC"].dims[0]

        # Assign time as a coordinate and swap dimension
        ds = ds.assign_coords(time=time_da)
        if u_dim != "time":
            ds = ds.swap_dims({u_dim: "time"})

        if "UnixTime_UTC" in ds.data_vars:
            ds = ds.drop_vars("UnixTime_UTC")

    # 2. Handle Coordinates (Latitude/Longitude) from Attributes
    # Extract from merged attributes (from Instrument_Attributes group)
    lat = ds.attrs.get("Location_lat", 0.0)
    lon = ds.attrs.get("Location_lon", 0.0)

    # Handle case where attributes are stored as lists/arrays
    if isinstance(lat, list | np.ndarray) and len(lat) > 0:
        lat = lat[0]
    if isinstance(lon, list | np.ndarray) and len(lon) > 0:
        lon = lon[0]

    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        lat, lon = 0.0, 0.0

    # Ensure x and y dimensions exist for consistency with other lidar readers
    if "x" not in ds.dims:
        ds = ds.expand_dims("x")
        ds["x"] = [0.0]
    if "y" not in ds.dims:
        ds = ds.expand_dims("y")
        ds["y"] = [0.0]

    ds.coords["latitude"] = (("y", "x"), np.array([[lat]]))
    ds.coords["longitude"] = (("y", "x"), np.array([[lon]]))

    ds.coords["latitude"].attrs.update({"units": "degrees_north", "standard_name": "latitude"})
    ds.coords["longitude"].attrs.update({"units": "degrees_east", "standard_name": "longitude"})

    # Update history
    ds = update_history(ds, "Preprocessed UMBC Aerosol data using standardized preprocessing.")

    return ds
