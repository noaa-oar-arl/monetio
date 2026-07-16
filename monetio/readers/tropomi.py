"""TROPOMI Reader"""

import warnings

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import (
    apply_qa_mask,
    lazy_index_along_axis,
    standardize_satellite_coords,
    update_history,
)


@register_reader("tropomi")
class TROPOMIReader(GriddedReader):
    """
    Reader for TROPOMI L2 (Sentinel-5P) data.
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
        group: str | list[str] | None = None,
        calculate_pressure: bool = True,
        qa_threshold: float | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads TROPOMI data.

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
        group : str or list of str, optional
            The NetCDF group(s) to open. If a list is provided, groups will be merged.
            If None, common TROPOMI groups will be opened:
            - "PRODUCT"
            - "PRODUCT/SUPPORT_DATA/INPUT_DATA"
            - "PRODUCT/SUPPORT_DATA/GEOLOCATIONS"
            - "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS"
        calculate_pressure : bool, optional
            Whether to calculate pressure levels if necessary variables are found,
            by default True.
        qa_threshold : float, optional
            If provided, mask data where 'qa_value' is less than this threshold.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The TROPOMI dataset.

        Examples
        --------
        Open standard NO2 product:
        >>> reader = TROPOMIReader()
        >>> ds = reader.open_dataset(files="S5P_OFFL_L2_NO2_*.nc", qa_threshold=0.75)
        """
        if group is None:
            groups = [
                "PRODUCT",
                "PRODUCT/SUPPORT_DATA/INPUT_DATA",
                "PRODUCT/SUPPORT_DATA/GEOLOCATIONS",
                "PRODUCT/SUPPORT_DATA/DETAILED_RESULTS",
            ]
        elif isinstance(group, str):
            groups = [group]
        else:
            groups = group

        # To ensure consistent dimensions and that all variables needed for
        # preprocessing (e.g. pressure calculation) are available, we apply
        # preprocessing AFTER merging all groups.
        user_preprocess = kwargs.pop("preprocess", None)

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"
        dsets = []
        for g in groups:
            # We copy kwargs to avoid modifying the original dict in the loop
            g_kwargs = kwargs.copy()
            g_kwargs["group"] = g
            try:
                # Open without TROPOMI preprocess or _ensure_time_dimension
                # (tropomi_preprocess handles time dimension creation itself)
                ds_g = self.driver.open(
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
            except Exception as e:
                warnings.warn(f"Could not open group {g}: {e}")

        if not dsets:
            raise RuntimeError("No groups could be opened.")

        # Merge groups
        # We use compat='no_conflicts' as coordinates should be identical
        ds = xr.merge(dsets, compat="no_conflicts")

        # Now apply TROPOMI preprocessing to the merged dataset
        ds = tropomi_preprocess(
            ds, calculate_pressure=calculate_pressure, qa_threshold=qa_threshold
        )

        if user_preprocess:
            ds = user_preprocess(ds)

        # Update history
        ds = update_history(ds, "Read TROPOMI data.")

        return ds


def tropomi_preprocess(
    ds: xr.Dataset, calculate_pressure: bool = True, qa_threshold: float | None = None
) -> xr.Dataset:
    """
    Preprocess TROPOMI dataset: standardize coordinates, handle time, and
    optionally calculate pressure and apply quality flags.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset from a single file/group (or merged groups).
    calculate_pressure : bool, optional
        Whether to calculate pressure levels, by default True.
    qa_threshold : float, optional
        Quality value threshold for masking. If provided, variables are masked
        where 'qa_value' is less than this threshold.

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> ds = tropomi_preprocess(ds, qa_threshold=0.75)
    """
    # 1. Standardize dimensions and coordinates
    # TROPOMI uses 'scanline' and 'ground_pixel'
    ds = standardize_satellite_coords(ds, lat_name="latitude", lon_name="longitude")

    # 2. Handle Time
    if "time" in ds.coords and "delta_time" in ds.data_vars:
        ref_time = ds.coords["time"]
        delta_time = ds.data_vars["delta_time"]
        if "y" in delta_time.dims:
            scan_time = ref_time + delta_time.astype("timedelta64[ms]")
            ds = ds.drop_vars("time").assign_coords(time=scan_time)

    # 3. Calculate Pressure (Lazy) - must happen before dim rename if it depends on 'y'
    if calculate_pressure:
        ds = _add_pressure_levels(ds)

    # 4. Handle Altitude/Height
    for h_var in ["altitude", "aerosol_mid_height", "height"]:
        if h_var in ds.data_vars and "height_m_mid" not in ds.data_vars:
            h_mid = ds[h_var].copy()
            units = h_mid.attrs.get("units", "m")
            if units == "km":
                h_mid = h_mid * 1000.0
                h_mid.attrs["units"] = "m"
            ds["height_m_mid"] = h_mid.assign_attrs({"long_name": "mid-layer height"})
            break

    # 5. Coordinate and Dimension Harmonization
    if "time" in ds.coords and "time" not in ds.dims:
        if ds.coords["time"].dims == ("y",):
            ds = ds.swap_dims({"y": "time"})

    # Ensure all data variables have 'time' dimension if it exists and they have 'y'
    if "time" in ds.dims:
        for var in ds.data_vars:
            if "y" in ds[var].dims:
                ds[var] = ds[var].rename({"y": "time"})

    # 6. Quality Flagging (Lazy & Vectorized)
    if qa_threshold is not None:
        ds = apply_qa_mask(ds, qa_var="qa_value", threshold=qa_threshold)

    # Update history
    ds = update_history(
        ds,
        "Preprocessed TROPOMI data: standardized coordinates, handled time, "
        "and optionally applied quality flagging and pressure calculation.",
    )

    return ds


def _add_pressure_levels(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate mid-layer and interface pressure levels for TROPOMI lazily.

    Supports NO2/HCHO (TM5), CO/CH4 style pressure definitions, and existing
    pressure variables (O3 Profile, AER_LH).

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset containing variables required for pressure calculation.

    Returns
    -------
    xr.Dataset
        Dataset with 'pres_pa_mid' and optionally 'troppres' added.

    Examples
    --------
    >>> ds = _add_pressure_levels(ds)
    """
    # 1. NO2/HCHO style (TM5 constant a, b)
    if all(v in ds.data_vars for v in ["tm5_constant_a", "tm5_constant_b", "surface_pressure"]):
        a = ds["tm5_constant_a"]
        b = ds["tm5_constant_b"]
        ps = ds["surface_pressure"]

        # Interface pressure: p_int = a + b * ps
        p_int = a + b * ps
        # p_int now has dims (z, vertices, y, x) or similar

        # Mid-layer pressure: (p_bottom + p_top) / 2
        p_mid = p_int.mean(dim="vertices") if "vertices" in p_int.dims else p_int.mean(dim="v")

        ds["pres_pa_mid"] = p_mid.assign_attrs({"units": "Pa", "long_name": "mid-layer pressure"})

        # If tm5_tropopause_layer_index is present, calculate tropopause pressure
        if "tm5_tropopause_layer_index" in ds.data_vars:
            itrop = ds["tm5_tropopause_layer_index"]
            try:
                # Ensure itrop is within bounds and integer
                itrop_valid = itrop.where((itrop >= 0) & (itrop < ds.sizes.get("z", 1)), 0).astype(
                    int
                )

                # Use standardized utility for lazy indexing
                ds["troppres"] = lazy_index_along_axis(p_mid, itrop_valid, dim="z")

                ds["troppres"].attrs.update({"units": "Pa", "long_name": "tropopause pressure"})
            except Exception as e:
                warnings.warn(f"Could not calculate tropopause pressure: {e}")

    # 2. CO/CH4 style (pressure_levels interfaces)
    elif "pressure_levels" in ds.data_vars:
        p_int = ds["pressure_levels"]
        # Find vertical dimension
        v_dim = next((d for d in ["z", "level", "layer", "Levels"] if d in p_int.dims), None)
        if v_dim:
            # Interface pressure is directly provided
            # Mid-layer pressure is average of adjacent interfaces
            p_mid = p_int.rolling({v_dim: 2}).mean().dropna(v_dim)

            # Ensure dimension name is 'z' for the result to match data variables
            if "z" in ds.dims and ds.sizes["z"] == p_mid.sizes[v_dim]:
                p_mid = p_mid.rename({v_dim: "z"})
            elif v_dim != "z" and "z" not in ds.dims:
                p_mid = p_mid.rename({v_dim: "z"})

            ds["pres_pa_mid"] = p_mid.assign_attrs(
                {"units": "Pa", "long_name": "mid-layer pressure"}
            )

    # 3. Existing pressure variables (e.g. O3 Profile, AER_LH)
    for p_var in ["pressure", "aerosol_mid_pressure", "aerosol_pressure"]:
        if p_var in ds.data_vars and "pres_pa_mid" not in ds.data_vars:
            # Create a copy to avoid modifying the original variable's attributes directly
            p_mid = ds[p_var].copy()
            # Handle units (TROPOMI is usually Pa, but let's be safe)
            units = p_mid.attrs.get("units", "Pa")
            if units == "hPa":
                p_mid = p_mid * 100.0
                p_mid.attrs["units"] = "Pa"

            ds["pres_pa_mid"] = p_mid.assign_attrs({"long_name": "mid-layer pressure"})
            break

    return ds
