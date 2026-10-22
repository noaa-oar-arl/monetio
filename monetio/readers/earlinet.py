"""EARLINET Reader"""

import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import update_history


@register_reader("earlinet")
class EARLINETReader(GriddedReader):
    """
    Reader for EARLINET (European Aerosol Research Lidar Network) NetCDF data.
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
        Retrieve and load EARLINET data.

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
            The loaded EARLINET data.
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
            preprocess=earlinet_preprocess,
            **kwargs,
        )

        return ds


def earlinet_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess EARLINET dataset: standardize coordinates and dimensions.

    Parameters
    ----------
    ds : xr.Dataset
        Input EARLINET dataset.

    Returns
    -------
    xr.Dataset
        Preprocessed dataset.
    """
    # 1. Standardize Time
    # EARLINET NetCDF files are usually CF compliant and xarray handles them.

    # 2. Rename Dimensions/Coordinates
    # altitude is usually a coordinate/dimension already named 'altitude' or 'height'.
    # If it's something else, we can rename it.
    if "height" in ds.dims and "altitude" not in ds.dims:
        ds = ds.rename({"height": "altitude"})

    # 3. Coordinate handling
    # Ensure latitude and longitude are coordinates
    coord_vars = ["latitude", "longitude", "altitude", "time", "wavelength"]
    actual_coords = [v for v in coord_vars if v in ds.variables]
    if actual_coords:
        ds = ds.set_coords(actual_coords)

    # 4. Standard attributes
    if "altitude" in ds.coords:
        ds["altitude"].attrs.update({"units": "m", "standard_name": "altitude", "positive": "up"})

    if "latitude" in ds.coords:
        ds["latitude"].attrs.update({"units": "degrees_north", "standard_name": "latitude"})

    if "longitude" in ds.coords:
        ds["longitude"].attrs.update({"units": "degrees_east", "standard_name": "longitude"})

    # Update history
    ds = update_history(ds, "Preprocessed EARLINET data.")

    return ds
