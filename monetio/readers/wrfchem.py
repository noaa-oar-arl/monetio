"""WRF-Chem Reader"""

from functools import partial
from typing import Any

import numpy as np
import xarray as xr

from .base import (
    GriddedReader,
    _convert_to_ppb,
    _convert_ugkg_to_ugm3,
    _scientific_hygiene,
    add_lazy_diagnostic,
    register_reader,
)
from .sat_utils import update_history
from .time_utils import parse_wrf_times
from .wrfchem_specs import DIAGNOSTICS


@register_reader("wrfchem")
class WRFChemReader(GriddedReader):
    """
    Reader for WRF-Chem and RAP-Chem model output files.
    """

    def open_dataset(
        self,
        files: str | list[str],
        convert_to_ppb: bool = True,
        mech: str = "racm_esrl_vcp",
        var_list: list[str] | None = None,
        surf_only: bool = False,
        surf_only_nc: bool = False,
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
        Reads WRF-Chem netCDF files.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path, list of paths, or glob pattern.
        convert_to_ppb : bool, optional
            Convert gas species from ppmV to ppbV, by default True.
        mech : str, optional
            Mechanism for calculating sums, by default "racm_esrl_vcp".
        var_list : list of str, optional
            List of variables to include, by default None.
        surf_only : bool, optional
            Whether to only keep surface data, by default False.
        surf_only_nc : bool, optional
            Whether input data already contains only surface data, by default False.
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
        **kwargs : Any
            Additional arguments passed to the driver.

        Returns
        -------
        xarray.Dataset
            The processed WRF-Chem dataset.

        Examples
        --------
        >>> reader = WRFChemReader()
        >>> ds = reader.open_dataset("wrfout_d01_*")
        """
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = partial(
                wrfchem_preprocess,
                convert_to_ppb=convert_to_ppb,
                mech=mech,
                var_list=var_list,
                surf_only=surf_only,
                surf_only_nc=surf_only_nc,
            )

        if "combine" not in kwargs:
            kwargs["combine"] = "nested"
        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"

        # Explicitly pass virtualization parameters to the driver via base class
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
        ds = update_history(ds, "Read WRF-Chem data.")

        return ds

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Standardize variable names and metadata.

        Parameters
        ----------
        ds : xr.Dataset
            WRF-Chem dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # Coordinate and Dimension Renaming (if not already done in preprocess)
        rename_dict = {
            "Time": "time",
            "south_north": "y",
            "west_east": "x",
            "XLONG": "longitude",
            "XLAT": "latitude",
            "bottom_top": "z",
            "bottom_top_stag": "z_stag",
            "soil_layers_stag": "z_soil",
        }
        actual_rename = {}
        for k, v in rename_dict.items():
            if k in ds.variables or k in ds.dims:
                if v in ds.dims and k != v:
                    continue
                actual_rename[k] = v

        if actual_rename:
            ds = ds.rename(actual_rename)

        # Scientific Hygiene
        ds = _scientific_hygiene(ds)

        # Update history
        ds = update_history(ds, "Harmonized WRF-Chem dataset.")

        return ds


def wrfchem_preprocess(
    ds: xr.Dataset,
    *,
    convert_to_ppb: bool = True,
    mech: str = "racm_esrl_vcp",
    var_list: list[str] | None = None,
    surf_only: bool = False,
    surf_only_nc: bool = False,
) -> xr.Dataset:
    """
    Preprocess function for a single WRF-Chem file.

    Parameters
    ----------
    ds : xarray.Dataset
        Input WRF-Chem dataset.
    convert_to_ppb : bool, optional
        Whether to convert gas species to ppbV, by default True.
    mech : str, optional
        Mechanism for diagnostics, by default "racm_esrl_vcp".
    var_list : list of str, optional
        List of variables to keep, by default None.
    surf_only : bool, optional
        Keep only surface layer, by default False.
    surf_only_nc : bool, optional
        Whether input is already surface-only, by default False.

    Returns
    -------
    xarray.Dataset
        The preprocessed dataset.
    """
    # 1. Coordinate and Dimension Renaming
    rename_dict = {
        "Time": "time",
        "south_north": "y",
        "west_east": "x",
        "XLONG": "longitude",
        "XLAT": "latitude",
        "bottom_top": "z",
        "bottom_top_stag": "z_stag",
        "soil_layers_stag": "z_soil",
    }
    # Only rename what exists
    actual_rename = {}
    for k, v in rename_dict.items():
        if k in ds.variables or k in ds.dims:
            if v in ds.dims and k != v:
                # Target exists, maybe it's already renamed or there's a conflict
                continue
            actual_rename[k] = v

    if actual_rename:
        ds = ds.rename(actual_rename)

    # 2. Lazy Time Parsing
    if "Times" in ds.variables:
        ds = _parse_wrf_times(ds)

    # 3. Unit Conversions (Lazy) - Move before diagnostics to ensure synced units
    if convert_to_ppb:
        ds = _convert_to_ppb(ds)

    # convert "ug/kg-dryair -> ug/m3" if density can be calculated
    ds = _convert_ugkg_to_ugm3(ds)

    # 4. Add lazy diagnostic variables
    for name, spec in DIAGNOSTICS.items():
        ds = add_lazy_diagnostic(ds, name, spec)

    # 5. Subset variables if requested
    if var_list is not None:
        # We must keep coordinates and some essentials
        essentials = ["latitude", "longitude", "time", "z", "z_stag", "z_soil"]
        to_keep = set(var_list) | set(essentials)
        # Add those that were added as diagnostics
        to_keep |= {name for name in DIAGNOSTICS if name in ds.variables}
        available = [v for v in ds.variables if v in to_keep]
        ds = ds[available]

    # 6. Handle Surface Only
    if surf_only and not surf_only_nc and "z" in ds.dims:
        ds = ds.isel(z=[0])

    # 7. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # 8. Transpose to standard order if dims exist
    # Ensure all dimensions are accounted for to avoid ValueError
    target_dims = ["time", "z", "y", "x"]
    dims = [d for d in target_dims if d in ds.dims]
    dims += [d for d in ds.dims if d not in target_dims]
    if dims:
        ds = ds.transpose(*dims)

    # Update history
    ds = update_history(
        ds, "Preprocessed WRF-Chem data (renaming, unit conversion, diagnostics, and hygiene)."
    )

    return ds


def _parse_wrf_times(ds: xr.Dataset) -> xr.Dataset:
    """
    Parse WRF 'Times' character array into a 'time' coordinate lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset with 'Times' variable.

    Returns
    -------
    xarray.Dataset
        Dataset with 'time' coordinate.

    Examples
    --------
    >>> ds = _parse_wrf_times(ds)
    """
    times_var = ds.Times

    # Find the string dimension (usually 'DateStrLen')
    # We expect 'time' to be the other dimension if it's 2D
    string_dims = [d for d in times_var.dims if d != "time"]
    if not string_dims:
        if times_var.ndim == 1:
            string_dim = times_var.dims[0]
        else:
            return ds
    else:
        string_dim = string_dims[-1]

    # Use vectorized parser from time_utils
    parsed_times = xr.apply_ufunc(
        parse_wrf_times,
        times_var,
        input_core_dims=[[string_dim]],
        output_core_dims=[[]],
        vectorize=False,
        dask="parallelized",
        output_dtypes=[np.dtype("datetime64[ns]")],
    )

    # Standardize name
    parsed_times = parsed_times.rename("time")

    # Assign coordinate
    if "time" in ds.dims:
        ds = ds.assign_coords(time=parsed_times)
    else:
        ds["time"] = parsed_times

    # Update history
    ds = update_history(ds, "Optimized time parsing.")

    return ds
