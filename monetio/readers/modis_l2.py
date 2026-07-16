"""MODIS L2 Swath Reader"""

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import standardize_satellite_coords, tai93_to_datetime, update_history


@register_reader("modis_l2")
class MODISL2Reader(GriddedReader):
    """
    Reader for MODIS L2 swath data.
    """

    def open_dataset(
        self,
        files: str | list[str],
        variable_dict: dict = None,
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
        Reads MODIS L2 swath data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s) or URL(s).
        variable_dict : dict, optional
            Dictionary of variables to read with metadata (scale, minimum, maximum, quality_flag).
            Example: `{'AOD_550': {'scale': 0.001, 'quality_flag': 3}}`
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
            Additional arguments passed to the reader.

        Returns
        -------
        xr.Dataset
            The processed MODIS dataset.

        Examples
        --------
        >>> reader = MODISL2Reader()
        >>> vdict = {'Deep_Blue_Aerosol_Optical_Depth_550_Land': {'scale': 0.001}}
        >>> ds = reader.open_dataset('MYD04_L2.A2023126.1830.061.2023127154555.hdf', variable_dict=vdict)
        """
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = lambda ds: modis_l2_preprocess(ds, variable_dict=variable_dict)

        # If variable_dict is provided, we can try to only load those variables
        # plus the required coordinates (Latitude, Longitude, Scan_Start_Time)
        if variable_dict is not None and "drop_variables" not in kwargs:
            # We don't know all variables in the file without opening it,
            # but we can specify which ones we WANT if the engine supports it.
            # Most xarray engines don't have an 'only_variables' but have 'drop_variables'.
            # For now, we'll load everything and filter in preprocess,
            # or the user can pass drop_variables.
            pass

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

        # Filter to requested variables if variable_dict is provided
        if variable_dict is not None:
            # Keep variables in variable_dict
            vars_to_keep = list(variable_dict.keys())

            # Only filter data_vars, preserve ALL coordinates
            available_vars = [v for v in vars_to_keep if v in ds.data_vars]
            ds = ds[available_vars]

        # Update history
        ds = update_history(ds, "Read MODIS L2 data.")

        return ds


def modis_l2_preprocess(ds: xr.Dataset, variable_dict: dict = None) -> xr.Dataset:
    """
    Preprocess MODIS L2 dataset: standardize coords, apply scaling/clipping, and quality flags.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.
    variable_dict : dict, optional
        Metadata for variables.

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> vdict = {'AOD': {'scale': 0.001, 'quality_flag': 2}, 'Quality_Assurance': {}}
    >>> ds = modis_l2_preprocess(ds, variable_dict=vdict)
    """
    # 1. Standardize dimensions and coordinates
    # MODIS L2 uses 'Cell_Along_Swath' and 'Cell_Across_Swath'
    ds = standardize_satellite_coords(
        ds,
        y_dim=["Cell_Along_Swath", "Rows", "scanline"],
        x_dim=["Cell_Across_Swath", "Columns", "ground_pixel"],
    )

    # 2. Add time coordinate if Scan_Start_Time is present
    if "Scan_Start_Time" in ds.variables and "time" not in ds.coords:
        ds["time"] = tai93_to_datetime(ds["Scan_Start_Time"])
        ds = ds.set_coords("time")

    # 3. Apply variable_dict transformations (scale, minimum, maximum)
    quality_masks = []
    if variable_dict:
        for varname, meta in variable_dict.items():
            if varname in ds.variables:
                if "scale" in meta:
                    ds[varname] = ds[varname] * meta["scale"]
                if "minimum" in meta:
                    ds[varname] = ds[varname].where(ds[varname] >= meta["minimum"])
                if "maximum" in meta:
                    ds[varname] = ds[varname].where(ds[varname] <= meta["maximum"])

                if "quality_flag" in meta:
                    # Collect quality flags for batch masking
                    quality_masks.append((varname, meta["quality_flag"]))

    # 4. Apply Quality Flag masking (Lazy & Vectorized)
    if quality_masks:
        combined_mask = None
        for q_var, q_thresh in quality_masks:
            mask = ds[q_var] >= q_thresh
            if combined_mask is None:
                combined_mask = mask
            else:
                combined_mask = combined_mask & mask

        if combined_mask is not None:
            for varname in ds.data_vars:
                # Mask all data variables by the combined quality flag
                ds[varname] = ds[varname].where(combined_mask)

    ds = update_history(ds, "Preprocessed MODIS L2 data using standardized preprocessing.")

    return ds
