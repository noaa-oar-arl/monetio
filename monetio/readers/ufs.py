"""UFS-AQM Reader"""

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
from .ufs_specs import DIAGNOSTICS


@register_reader("ufs")
class UFSReader(GriddedReader):
    """
    Reader for UFS-AQM model output files.
    """

    def open_dataset(
        self,
        files: str | list[str],
        convert_to_ppb: bool = True,
        mech: str = "cb6r3_ae6_aq",
        var_list: list[str] | None = None,
        fname_pm25: str | list[str] | None = None,
        surf_only: bool = False,
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
        Reads UFS-AQM netCDF files.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path, list of paths, or glob pattern.
        convert_to_ppb : bool, optional
            Convert gas species from ppmV to ppbV, by default True.
        mech : str, optional
            Mechanism name for species sums, by default "cb6r3_ae6_aq".
        var_list : List[str], optional
            List of variables to include, by default None.
        fname_pm25 : Union[str, List[str]], optional
            Optional separate PM2.5 files to merge, by default None.
        surf_only : bool, optional
            Whether to only keep surface data, by default False.
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
        xr.Dataset
            The processed UFS-AQM dataset.

        Examples
        --------
        >>> reader = UFSReader()
        >>> ds = reader.open_dataset("aqm.t12z.dyn.f*.nc", surf_only=True)
        """
        # Prepare kwargs
        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"
        if "combine" not in kwargs:
            kwargs["combine"] = "nested"
        # Open dataset
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

        # Merge PM25 file if present
        if fname_pm25 is not None:
            ds_pm25 = self.driver.open(fname_pm25, virtualizarr_parser="hdf5", **kwargs)
            ds_pm25 = ds_pm25.drop_vars(["lat", "lon", "pfull"], errors="ignore")
            ds_pm25.attrs = {}
            from monetio.util import _try_merge_exact

            ds = _try_merge_exact(ds, ds_pm25, right_name="PM2.5")

        # Standardize
        rename_dict = {
            "grid_yt": "y",
            "grid_xt": "x",
            "pfull": "z",
            "phalf": "z_i",
            "lon": "longitude",
            "lat": "latitude",
            "tmp": "temperature_k",
            "pressfc": "surfpres_pa",
            "dpres": "dp_pa",
            "hgtsfc": "surfalt_m",
            "delz": "dz_m",
        }
        # Only rename what exists
        # To avoid warnings, we use a single rename call for all variables and dimensions.
        actual_rename = {k: v for k, v in rename_dict.items() if k in ds.variables or k in ds.dims}
        if actual_rename:
            import warnings

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=".*does not create an index anymore.*")
                ds = ds.rename(actual_rename)

            # Ensure renamed dimensions that were coordinates maintain their index
            for old, new in actual_rename.items():
                if new in ds.dims and new in ds.coords and new not in ds.indexes:
                    ds = ds.set_index({new: new})

        # Calculations
        if "surfpres_pa" in ds and "ak" in ds and "bk" in ds:
            ds["pres_pa_mid"] = _calc_pressure(ds)

        # Resort z (Lazy)
        if "z" in ds.coords:
            if ds.z.size > 1:
                is_ascending = ds.z[0] < ds.z[-1]
                if is_ascending:
                    ds = ds.isel(z=slice(None, None, -1))
                    if "dz_m" in ds:
                        ds["dz_m"] = ds["dz_m"] * -1.0
        if "z_i" in ds.coords:
            if ds.z_i.size > 1:
                is_ascending = ds.z_i[0] < ds.z_i[-1]
                if is_ascending:
                    ds = ds.isel(z_i=slice(None, None, -1))

        if not surf_only and "dz_m" in ds and "surfalt_m" in ds:
            ds["alt_msl_m_full"] = _calc_hgt(ds)

        if "latitude" in ds.data_vars and "time" in ds["latitude"].dims:
            ds["latitude"] = ds["latitude"].isel(time=0)
        if "longitude" in ds.data_vars and "time" in ds["longitude"].dims:
            ds["longitude"] = ds["longitude"].isel(time=0)

        coords = [c for c in ["latitude", "longitude", "time"] if c in ds.variables]
        ds = ds.set_coords(coords)

        if surf_only and "z" in ds.dims:
            ds = ds.isel(z=0).expand_dims("z", axis=1)

        # Time fix (Avoid eager .indexes)
        if "time" in ds.coords:
            # Check for cftime lazily if possible, or use .indexes only if necessary
            # For now, following standard conventions to avoid eager indexes if we can
            if ds.indexes["time"].__class__.__name__ == "CFTimeIndex":
                ds["time"] = ds.indexes["time"].to_datetimeindex()

        # Unit conversion (Lazy)
        if convert_to_ppb:
            ds = _convert_to_ppb(ds)

        ds = _convert_ugkg_to_ugm3(ds)

        # Add lazy diagnostic variables
        for name, spec in DIAGNOSTICS.items():
            ds = add_lazy_diagnostic(ds, name, spec)

        # Subset if var_list
        if var_list is not None:
            # We must keep coordinates and some essentials
            essentials = [
                "latitude",
                "longitude",
                "time",
                "z",
                "z_i",
                "z_stag",
                "z_soil",
                "pres_pa_mid",
                "temperature_k",
            ]
            to_keep = set(var_list) | set(essentials)
            # Add those that were added as diagnostics
            to_keep |= {name for name in DIAGNOSTICS if name in ds.variables}
            available = [v for v in ds.variables if v in to_keep]
            ds = ds[available]

        # Scientific Hygiene
        ds = _scientific_hygiene(ds)

        # Update history
        ds = update_history(ds, "Read UFS-AQM data.")

        return ds


def _calc_pressure(ds: xr.Dataset) -> xr.DataArray:
    """
    Calculate mid-layer pressure from hybrid coordinates.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset containing ak, bk, and surfpres_pa.

    Returns
    -------
    xr.DataArray
        Pressure at layer mid-points in Pa.

    Examples
    --------
    >>> p_mid = _calc_pressure(ds)
    """
    psfc = ds.surfpres_pa
    ak = ds.ak
    bk = ds.bk

    if ak.size == ds.sizes["z"] + 1:
        p_interfaces_1 = ak[:-1] + psfc * bk[:-1]
        p_interfaces_2 = ak[1:] + psfc * bk[1:]
    else:
        # Fallback
        p_interfaces_1 = ak + psfc * bk
        p_interfaces_2 = p_interfaces_1

    # Log-linear interpolation for mid-points
    p_mid = (p_interfaces_2 - p_interfaces_1) / np.log(p_interfaces_2 / p_interfaces_1)

    # The resulting dimension should be 'z' (midpoints)
    if "z_i" in p_mid.dims:
        p_mid = p_mid.rename({"z_i": "z"})

    p_mid.attrs.update({"units": "Pa", "long_name": "Pressure at layer mid-points"})
    return p_mid


def _calc_hgt(ds: xr.Dataset) -> xr.DataArray:
    """
    Calculate altitude MSL lazily.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset containing dz_m and surfalt_m.

    Returns
    -------
    xr.DataArray
        Altitude above MSL in meters.

    Examples
    --------
    >>> alt = _calc_hgt(ds)
    """
    # Assuming dz_m is positive upwards or we already flipped it
    alt = ds.dz_m.cumsum(dim="z") + ds.surfalt_m
    alt.attrs.update({"units": "m", "long_name": "Altitude above MSL"})
    return alt
