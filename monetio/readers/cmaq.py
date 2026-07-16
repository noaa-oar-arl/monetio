"""CMAQ File Reader"""

from functools import partial
from typing import Any

import xarray as xr

from monetio.grids import grid_from_dataset

from .base import (
    GriddedReader,
    _add_ioapi_latlon,
    _convert_to_ppb,
    _format_units,
    _harmonize_ioapi_dims,
    _harmonize_ioapi_vars,
    _scientific_hygiene,
    add_lazy_diagnostic,
    register_reader,
)
from .cmaq_specs import DIAGNOSTICS
from .sat_utils import update_history


@register_reader("cmaq")
class CMAQReader(GriddedReader):
    """
    Reader for CMAQ (Community Multiscale Air Quality) model output files.
    """

    def open_dataset(
        self,
        files: str | list[str],
        earth_radius: float = 6370000,
        convert_to_ppb: bool = True,
        drop_duplicates: bool = False,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Reads CMAQ netCDF files.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s), URL(s), or glob pattern.
        earth_radius : float, optional
            Earth radius in meters, by default 6370000.
        convert_to_ppb : bool, optional
            Convert gas species from ppmV to ppbV, by default True.
        drop_duplicates : bool, optional
            Drop duplicate time steps within each file, by default False.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr, by default False.
        virtualizarr_file : str or None, optional
            Path to the VirtualiZarr file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use (e.g. 'hdf5').
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
        **kwargs : Any
            Additional arguments passed to xarray.open_mfdataset or the driver.

        Returns
        -------
        xarray.Dataset
            The processed CMAQ dataset.
        """
        # 1. Setup preprocessing
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = partial(
                cmaq_preprocess,
                earth_radius=earth_radius,
                convert_to_ppb=convert_to_ppb,
            )
        # 2. Open the dataset using standard xarray (via XarrayDriver)
        if "combine" not in kwargs:
            kwargs["combine"] = "nested"
        if "concat_dim" not in kwargs:
            # If we rename in preprocess, we should use 'time'
            # But to be safe with existing logic, we let it be 'TSTEP' if not renamed yet,
            # or 'time' if it is.
            # Actually, preprocess runs BEFORE concatenation.
            kwargs["concat_dim"] = "time"

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

        # 3. Finalize
        if drop_duplicates:
            ds = ds.drop_duplicates("time")
            ds = update_history(ds, "Dropped duplicate time steps.")

        ds = self.harmonize(ds)

        # Update history
        ds = update_history(ds, "Read CMAQ data.")

        return ds

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Standardize variable names and metadata.

        Parameters
        ----------
        ds : xr.Dataset
            CMAQ dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # 1. Standardize variable names and drop redundant ones
        ds = _harmonize_ioapi_vars(ds)

        # 2. Clean up attributes
        ds = _scientific_hygiene(ds)

        # Update history
        ds = update_history(ds, "Harmonized CMAQ dataset.")

        return ds


def cmaq_preprocess(
    ds: xr.Dataset,
    *,
    earth_radius: float = 6370000,
    convert_to_ppb: bool = True,
) -> xr.Dataset:
    """
    Preprocess function for a single CMAQ file.

    Parameters
    ----------
    ds : xarray.Dataset
        Input CMAQ dataset.
    earth_radius : float, optional
        Earth radius in meters, by default 6370000.
    convert_to_ppb : bool, optional
        Convert gas species to ppbV, by default True.

    Returns
    -------
    xarray.Dataset
        Processed dataset.

    Examples
    --------
    >>> ds = cmaq_preprocess(ds, convert_to_ppb=True)
    """
    # 1. Add lazy diagnostic variables
    for name, spec in DIAGNOSTICS.items():
        ds = add_lazy_diagnostic(ds, name, spec)

    # 2. Grid and Coordinates
    grid = grid_from_dataset(ds, earth_radius=earth_radius)
    if grid:
        ds = ds.assign_attrs({"proj4_srs": grid})
        ds = _add_ioapi_latlon(ds, grid)

        # Also assign proj4_srs to all data variables for compatibility
        for var in ds.data_vars:
            ds[var].attrs["proj4_srs"] = grid

    # 3. Time
    if "TFLAG" in ds.variables:
        from .base import _get_ioapi_times

        ds = _get_ioapi_times(ds)

    # 4. Units and Formatting
    if convert_to_ppb:
        ds = _convert_to_ppb(ds)
    ds = _format_units(ds)

    # 5. Rename dimensions
    ds = _harmonize_ioapi_dims(ds)

    # 6. Harmonize variables (lowercase and drop redundant)
    ds = _harmonize_ioapi_vars(ds)

    # 7. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed CMAQ data.")

    return ds
