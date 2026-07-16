"""TEMPO Reader"""

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("tempo")
class TEMPOReader(GriddedReader):
    """
    Reader for TEMPO (Tropospheric Emissions: Monitoring of Pollution) L2 data.
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
        variable_dict: dict | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads TEMPO data.

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
            If None, common TEMPO groups will be opened:
            - "product"
            - "geolocation"
            - "support_data"
            - "qa_statistics"
        variable_dict : dict, optional
            Dictionary mapping variable names to processing options (scale, minimum, maximum).
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The TEMPO dataset.

        Examples
        --------
        Open standard NO2 product:
        >>> reader = TEMPOReader()
        >>> ds = reader.open_dataset(files="TEMPO_NO2_L2_*.nc")
        """
        if group is None:
            groups = ["product", "geolocation", "support_data", "qa_statistics"]
        elif isinstance(group, str):
            groups = [group]
        else:
            groups = group

        user_preprocess = kwargs.pop("preprocess", None)

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"
        dsets = []
        for g in groups:
            g_kwargs = kwargs.copy()
            g_kwargs["group"] = g
            try:
                # Open without the preprocessor at this stage
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
            except Exception:
                # Not all groups may be present in all files
                continue

        if not dsets:
            raise RuntimeError("No TEMPO groups could be opened.")

        # Merge groups
        ds = xr.merge(dsets, compat="no_conflicts")

        # Now apply TEMPO preprocessing to the merged dataset
        ds = tempo_preprocess(ds, variable_dict=variable_dict)

        if user_preprocess:
            ds = user_preprocess(ds)

        # Update history
        ds = update_history(ds, "Read TEMPO L2 data.")

        return ds


def tempo_preprocess(ds: xr.Dataset, variable_dict: dict | None = None) -> xr.Dataset:
    """
    Preprocess TEMPO dataset: standardize coordinates, handle units,
    calculate pressure, and apply variable-specific transformations.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset with merged groups.
    variable_dict : dict, optional
        Dictionary mapping variable names to processing options (scale, minimum, maximum).

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> ds = tempo_preprocess(ds, variable_dict={"no2_column": {"scale": 1e-15}})
    """
    # 1. Map variables from groups to root if they are still nested/prefixed
    # (Though open_dataset already handles merging, some engines might prefix)
    mapping = {
        "geolocation/longitude": "longitude",
        "geolocation/latitude": "latitude",
        "geolocation/time": "time",
        "product/vertical_column_troposphere": "vertical_column_troposphere",
    }
    for old, new in mapping.items():
        if old in ds.variables and new not in ds.variables:
            ds = ds.rename({old: new})

    # 2. Standardize dimensions and coordinates
    # TEMPO uses 'x' and 'y' for across-track and along-track.
    # Vertical dims can be 'swt_level' or 'swt_level_stagg'.
    ds = standardize_satellite_coords(
        ds,
        lat_name="latitude",
        lon_name="longitude",
        z_dim=["swt_level", "swt_level_stagg", "level"],
    )

    # 3. Handle Unit Conversion (Lazy)
    # Convert surface_pressure from hPa to Pa if needed
    if "surface_pressure" in ds.variables:
        ps = ds["surface_pressure"]
        if ps.attrs.get("units") == "hPa":
            # Scale attributes if they exist
            new_attrs = ps.attrs.copy()
            new_attrs["units"] = "Pa"
            for attr in ["valid_min", "valid_max", "Eta_A"]:
                if attr in new_attrs:
                    new_attrs[attr] *= 100.0
            ds["surface_pressure"] = (ps * 100.0).assign_attrs(new_attrs)

    # 4. Calculate Pressure (Lazy)
    if variable_dict and "pressure" in variable_dict:
        ds = _add_pressure(ds)

    # 5. Apply variable-specific processing from variable_dict
    if variable_dict:
        for var, options in variable_dict.items():
            if var in ds.variables:
                # Scale
                if "scale" in options:
                    ds[var] = ds[var] * options["scale"]

                # Clipping
                if "minimum" in options:
                    ds[var] = ds[var].where(ds[var] >= options["minimum"])
                if "maximum" in options:
                    ds[var] = ds[var].where(ds[var] <= options["maximum"])

                # Quality Flagging
                if "quality_flag_max" in options and "main_data_quality_flag" in ds.variables:
                    qf = ds["main_data_quality_flag"]
                    ds[var] = ds[var].where(qf <= options["quality_flag_max"])

    # Update history
    ds = update_history(ds, "Preprocessed TEMPO data.")

    return ds


def _add_pressure(ds: xr.Dataset) -> xr.Dataset:
    """
    Calculate pressure levels lazily for TEMPO using hybrid coefficients.
    p = Eta_A + Eta_B * surface_pressure
    """
    if "surface_pressure" not in ds.variables:
        import warnings

        warnings.warn("Calculating pressure requires surface_pressure. Variable skipped.")
        return ds

    ps = ds["surface_pressure"]
    eta_a = ps.attrs.get("Eta_A")
    eta_b = ps.attrs.get("Eta_B")

    if eta_a is not None and eta_b is not None:
        # Convert attributes to DataArrays for lazy broadcasting along a new 'z' dimension
        # Note: Eta_A/B in TEMPO are usually 1D arrays of level coefficients
        a = xr.DataArray(eta_a, dims="z", attrs={"units": "Pa"})
        b = xr.DataArray(eta_b, dims="z", attrs={"units": "1"})

        # The calculation is now fully lazy and backend-agnostic
        pres = a + b * ps

        ds["pres_pa_mid"] = pres.assign_attrs(
            {
                "units": "Pa",
                "long_name": "pressure",
                "standard_name": "air_pressure",
                "algorithm": "Calculated as Eta_A + Eta_B * surface_pressure",
            }
        )

    return ds
