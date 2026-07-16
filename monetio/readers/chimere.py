"""Chimere Reader"""

from functools import partial
from typing import Any

import xarray as xr

from .base import GriddedReader, _ensure_time_dimension, _scientific_hygiene, register_reader
from .sat_utils import update_history


@register_reader("chimere")
class ChimereReader(GriddedReader):
    """
    Reader for Chimere model output files.
    """

    def open_dataset(
        self,
        files: str | list[str],
        var_list: list[str] | None = None,
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
        Reads Chimere netCDF files.

        Parameters
        ----------
        files : str or list of str
            File path, list of paths, or glob pattern.
        var_list : list of str, optional
            List of variable names meant to be kept for the analysis, by default None.
        surf_only : bool, optional
            Whether to only keep surface data (layer 0), by default False.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr to create a virtual Zarr dataset, by default False.
        virtualizarr_file : str or None, optional
            Path to save/load the VirtualiZarr reference JSON file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use (e.g. 'hdf5', 'netcdf3', 'zarr', 'grib2').
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
            The processed Chimere dataset.

        Examples
        --------
        >>> reader = ChimereReader()
        >>> ds = reader.open_dataset("chimere_output.nc")
        >>> ds_lazy = reader.open_dataset("chimere_output.nc", use_dask=True)
        """
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = partial(
                chimere_preprocess,
                var_list=var_list,
                surf_only=surf_only,
            )

        if "combine" not in kwargs:
            kwargs["combine"] = "nested"
        if "concat_dim" not in kwargs:
            kwargs["concat_dim"] = "time"

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

        ds = self.harmonize(ds)
        ds = _ensure_time_dimension(ds)

        # Update history
        ds = update_history(ds, "Read Chimere data.")

        return ds


def chimere_preprocess(
    ds: xr.Dataset, *, var_list: list[str] | None = None, surf_only: bool = False
) -> xr.Dataset:
    """
    Preprocess function for a single Chimere file.

    Parameters
    ----------
    ds : xr.Dataset
        Input Chimere dataset.
    var_list : list of str, optional
        List of variables to keep, by default None.
    surf_only : bool, optional
        Whether to keep only surface data, by default False.

    Returns
    -------
    xr.Dataset
        Processed dataset.

    Examples
    --------
    >>> import xarray as xr
    >>> ds = xr.Dataset({"O3": (("time_counter", "y", "x"), [[1, 2]])})
    >>> ds_clean = chimere_preprocess(ds, var_list=["O3"])
    """
    if var_list is not None:
        drop_vars = set(ds.data_vars) - set(var_list)
        ds = ds.drop_vars(drop_vars, errors="ignore")

    rename_dict = {
        "nav_lat": "latitude",
        "nav_lon": "longitude",
        "time_counter": "time",
        "bottom_top": "z",
    }
    # Only rename if they exist in variables or dims
    rename_dict = {k: v for k, v in rename_dict.items() if k in ds.variables or k in ds.dims}

    if rename_dict:
        ds = ds.rename(rename_dict)

    if surf_only and "z" in ds.dims:
        ds = ds.isel(z=[0])

    # Ensure lat/lon have standard attributes if they exist
    if "latitude" in ds.variables:
        ds["latitude"].attrs.update({"units": "degrees_north", "standard_name": "latitude"})
    if "longitude" in ds.variables:
        ds["longitude"].attrs.update({"units": "degrees_east", "standard_name": "longitude"})

    # Scientific Hygiene handles coordinate assignment (latitude, longitude, time)
    # and attribute cleaning while preserving history.
    ds = _scientific_hygiene(ds)

    # Transpose to standard order if dims exist
    dims = [d for d in ["time", "z", "y", "x"] if d in ds.dims]
    if dims:
        ds = ds.transpose(*dims)

    # Consolidate history update
    ds = update_history(ds, "Preprocessed Chimere data (renaming, subsetting, and hygiene).")

    return ds
