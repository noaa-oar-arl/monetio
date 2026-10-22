"""MPLNET Reader"""

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import update_history


@register_reader("mplnet")
class MPLNETReader(GriddedReader):
    """
    Reader for MPLNET (NASA Micro-Pulse Lidar Network) V3 NetCDF data.
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
        Retrieve and load MPLNET data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path, list of paths, or glob pattern.
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
            Additional arguments passed to xarray.open_mfdataset.

        Returns
        -------
        xr.Dataset
            The loaded MPLNET data.
        """
        # Default to combined dimensions if not specified
        kwargs.setdefault("combine", "by_coords")

        # Use XarrayDriver (via GriddedReader) to open
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
            preprocess=mplnet_preprocess,
            **kwargs,
        )

        return ds


def mplnet_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess MPLNET dataset: standardize coordinates and dimensions.

    Parameters
    ----------
    ds : xr.Dataset
        Input MPLNET dataset.

    Returns
    -------
    xr.Dataset
        Preprocessed dataset.
    """
    # 1. Standardize Time
    # MPLNET often has 'time' as double (days since start of year or similar)
    # but modern V3 should be CF compliant and xarray should handle it.
    # If not, we might need custom logic.

    # 2. Rename Dimensions/Coordinates
    rename_vars = {}
    if "surface_altitude" in ds.variables:
        rename_vars["surface_altitude"] = "elevation"

    if rename_vars:
        ds = ds.rename_vars(rename_vars)

    # 3. Unit Conversions
    # elevation (surface_altitude) is in km in MPLNET V3
    if "elevation" in ds.coords or "elevation" in ds.data_vars:
        if ds["elevation"].attrs.get("units") == "km":
            ds["elevation"] = ds["elevation"] * 1000.0
            ds["elevation"].attrs["units"] = "m"

    # 4. Coordinate handling
    # Ensure latitude and longitude are coordinates
    coord_vars = ["latitude", "longitude", "elevation", "time"]
    actual_coords = [v for v in coord_vars if v in ds.variables]
    if actual_coords:
        ds = ds.set_coords(actual_coords)

    # Update history
    ds = update_history(ds, "Preprocessed MPLNET data.")

    return ds
