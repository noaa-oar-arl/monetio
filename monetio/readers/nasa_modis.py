"""NASA MODIS Reader"""

import pandas as pd
import xarray as xr

from .base import GriddedReader, _scientific_hygiene, register_reader
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("nasa_modis")
class NASAMODISReader(GriddedReader):
    """
    Reader for NASA MODIS HDF files.
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
        Reads NASA MODIS swath data.

        Parameters
        ----------
        files : str | list[str]
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
        **kwargs : Any
            Additional arguments passed to the Xarray driver.

        Returns
        -------
        xr.Dataset
            The processed NASA MODIS dataset.

        Examples
        --------
        >>> from monetio.readers.nasa_modis import NASAMODISReader
        >>> reader = NASAMODISReader()
        >>> ds = reader.open_dataset("MOD43A4.A2023001.h10v05.006.2023010123456.hdf")
        """
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = nasa_modis_preprocess

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
        ds = update_history(ds, "Read NASA MODIS data.")

        return ds


def nasa_modis_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess NASA MODIS dataset: standardize coordinates, handle time, and hygiene.

    Parameters
    ----------
    ds : xr.Dataset
        The raw NASA MODIS dataset.

    Returns
    -------
    xr.Dataset
        The preprocessed NASA MODIS dataset.

    Examples
    --------
    >>> ds = nasa_modis_preprocess(ds)
    """
    from ..grids import get_modis_latlon_from_swath_hv, get_sinu_area_def

    # Standardize dimensions
    ds = standardize_satellite_coords(
        ds, y_dim=["YDim:MOD_Grid_BRDF", "y"], x_dim=["XDim:MOD_Grid_BRDF", "x"]
    )
    ds = update_history(ds, "Standardized satellite coordinates.")

    # Extract tile info from attributes
    h = ds.attrs.get("HORIZONTALTILENUMBER")
    v = ds.attrs.get("VERTICALTILENUMBER")

    if h is not None and v is not None:
        ds = get_modis_latlon_from_swath_hv(h, v, ds)
        ds.attrs["area"] = get_sinu_area_def(ds)
        ds = update_history(ds, f"Assigned coordinates for tile h{h}v{v}.")

    # Handle Time
    if "time" not in ds.coords:
        # Try to get time from attributes
        range_start = ds.attrs.get("RANGEBEGINNINGDATE")
        time_start = ds.attrs.get("RANGEBEGINNINGTIME")
        if range_start and time_start:
            # We use xarray-native assignment to maintain laziness if possible,
            # though these are usually scalar attributes.
            dt = pd.to_datetime(f"{range_start} {time_start}")
            ds = ds.assign_coords(time=dt).expand_dims("time")
            ds = update_history(ds, f"Assigned time coordinate from attributes: {dt}.")

    # Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed NASA MODIS data.")

    return ds
