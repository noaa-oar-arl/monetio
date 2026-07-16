"""MOPITT Reader"""

import numpy as np
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import standardize_satellite_coords, tai93_to_datetime, update_history

MOPITT_MISSING = -9999.0


@register_reader("mopitt")
class MOPITTReader(GriddedReader):
    """
    Reader for MOPITT (Measurements Of Pollution In The Troposphere) L3 data.
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
        Reads MOPITT data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s) or URL(s).
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr, by default False.
        virtualizarr_file : str or None, optional
            Path to the VirtualiZarr file, by default None.
        virtualizarr_backend : str, optional
            VirtualiZarr backend, by default "kerchunk".
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
            The MOPITT dataset.
        """
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = mopitt_preprocess

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        ds = super().open_dataset(
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

        # Update history
        ds = update_history(ds, "Read MOPITT L3 data.")

        return ds


def mopitt_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess MOPITT dataset: standardize coordinates, handle time, and
    calculate auxiliary variables (pressure, a priori profiles).

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset from MOPITT L3 file.

    Returns
    -------
    xr.Dataset
        Processed dataset with standard names and derived variables.

    Examples
    --------
    >>> ds = xr.open_dataset("MOP03-20230101-L3V95.hdf", engine="h5netcdf")
    >>> ds = mopitt_preprocess(ds)
    """
    # 1. Expand Mapping
    # MOPITT L3 HDF5 structure: /HDFEOS/GRIDS/MOP03/Data Fields/
    mapping = {
        "HDFEOS/GRIDS/MOP03/Data Fields/Latitude": "latitude",
        "HDFEOS/GRIDS/MOP03/Data Fields/Longitude": "longitude",
        "HDFEOS/GRIDS/MOP03/Data Fields/Pressure": "pressure_levels",
        "HDFEOS/GRIDS/MOP03/Data Fields/Pressure2": "pressure_levels_ak",
        "HDFEOS/GRIDS/MOP03/Data Fields/RetrievedCOTotalColumnDay": "co_column",
        "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOTotalColumnDay": "apriori_co_column",
        "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOSurfaceMixingRatioDay": "apriori_co_surf",
        "HDFEOS/GRIDS/MOP03/Data Fields/SurfacePressureDay": "surface_pressure",
        "HDFEOS/GRIDS/MOP03/Data Fields/TotalColumnAveragingKernelDay": "co_ak_column",
        "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOMixingRatioProfileDay": "apriori_co_profile",
        # Night versions
        "HDFEOS/GRIDS/MOP03/Data Fields/RetrievedCOTotalColumnNight": "co_column_night",
        "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOTotalColumnNight": "apriori_co_column_night",
        "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOSurfaceMixingRatioNight": "apriori_co_surf_night",
        "HDFEOS/GRIDS/MOP03/Data Fields/SurfacePressureNight": "surface_pressure_night",
        "HDFEOS/GRIDS/MOP03/Data Fields/TotalColumnAveragingKernelNight": "co_ak_column_night",
        "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOMixingRatioProfileNight": "apriori_co_profile_night",
    }

    actual_rename = {old: new for old, new in mapping.items() if old in ds.variables}
    if actual_rename:
        ds = ds.rename(actual_rename)
        ds = update_history(ds, f"Renamed variables: {list(actual_rename.values())}")

    # Ensure pressure levels are coordinates
    for p_var in ["pressure_levels", "pressure_levels_ak"]:
        if p_var in ds.variables and p_var not in ds.coords:
            ds = ds.set_coords(p_var)

    # 2. Standardize
    ds = standardize_satellite_coords(ds)

    # 3. Handle time from attributes if missing
    if "time" not in ds.coords:
        # Check for StartTime in attributes
        start_time = ds.attrs.get("StartTime")
        if start_time is not None:
            if isinstance(start_time, list | np.ndarray):
                start_time = start_time[0]
            # MOPITT uses seconds since 1993-01-01
            # Wrap in DataArray to use standardized utility
            time_da = xr.DataArray([float(start_time)], dims=("time",))
            if "time" not in ds.dims:
                ds = ds.expand_dims("time")
            ds = ds.assign_coords(time=tai93_to_datetime(time_da))
            ds = update_history(ds, "Assigned time coordinate from StartTime attribute.")

    # 4. Handle Missing Values (Vectorized)
    with xr.set_options(keep_attrs=True):
        ds = ds.where(ds != MOPITT_MISSING)
    ds = update_history(ds, f"Applied vectorized missing value mask (value: {MOPITT_MISSING}).")

    # 5. Calculate Pressure (Lazy)
    if "co_ak_column" in ds.data_vars and "surface_pressure" in ds.data_vars:
        ds = _add_mopitt_pressure(ds)

    # 6. Combine A Priori (Lazy)
    if (
        "apriori_co_profile" in ds.data_vars
        and "apriori_co_surf" in ds.data_vars
        and "pressure_levels_ak" in ds.coords
    ):
        ds = _combine_mopitt_apriori(ds)

    # Update history
    ds = update_history(ds, "Preprocessed MOPITT L3 data using standardized preprocessing.")

    return ds


def _add_mopitt_pressure(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate 3D pressure array lazily for MOPITT.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Dataset with 'pressure' variable added.

    Examples
    --------
    >>> ds = _add_mopitt_pressure(ds)
    """
    if "pressure_levels_ak" not in ds.coords:
        return ds

    # Ensure pressure levels are in ascending order (100 to 1000 hPa)
    # Xarray's sortby is lazy if the coordinate is already in memory (typical for L3)
    ds_sorted = ds.sortby("pressure_levels_ak", ascending=True)

    alt = ds_sorted.coords["pressure_levels_ak"]
    if "time" in alt.dims:
        alt = alt.isel(time=0, drop=True)
    z_dim = alt.dims[0]
    ak_col = ds_sorted["co_ak_column"]
    ps = ds_sorted["surface_pressure"]

    _, p_3d = xr.broadcast(ak_col, alt)

    # Replace the 1000 hPa level (now at alt.max()) with actual surface pressure
    p_3d = xr.where(alt == alt.max(), ps, p_3d)

    # Center Pressure calculation
    # p_center[TOP] = 87.0 (at 100 hPa, which is alt.min())
    # p_center[z] = p[z] - (p[z] - p[z-1])/2
    # Shift towards surface (higher index in ascending alt) to get previous (lower pressure) level
    # Actually if ascending, p[z] > p[z-1]. So p_prev is level above.
    p_prev = p_3d.shift({z_dim: 1})
    p_center = p_3d - (p_3d - p_prev) / 2.0
    p_center = xr.where(alt == alt.min(), 87.0, p_center)

    # Apply mask: keep layers above surface (where surface_pressure >= level_pressure)
    p_center = p_center.where((alt == alt.min()) | (ps >= p_3d), np.nan)

    ds_sorted["pressure"] = p_center.assign_attrs({"units": "hPa", "long_name": "Center Pressure"})

    return update_history(ds_sorted, "Calculated 3D center pressure lazily.")


def _combine_mopitt_apriori(ds: xr.Dataset) -> xr.Dataset:
    """
    Combine surface and profile a priori values lazily.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Dataset with combined 'apriori_co_profile'.

    Examples
    --------
    >>> ds = _combine_mopitt_apriori(ds)
    """
    prof = ds["apriori_co_profile"]
    surf = ds["apriori_co_surf"]
    alt = ds.coords["pressure_levels_ak"]
    if "time" in alt.dims:
        alt = alt.isel(time=0, drop=True)

    # Combine: replace 1000 hPa level with surface values
    combined = xr.where(alt == alt.max(), surf, prof)

    # Masking based on pressure if available
    if "pressure" in ds.variables:
        combined = combined.where(~ds["pressure"].isnull())

    ds["apriori_co_profile"] = combined.assign_attrs(prof.attrs)

    return update_history(ds, "Combined surface and profile a priori values lazily.")
