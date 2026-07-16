"""SMAP (Soil Moisture Active Passive) Reader"""

import datetime

import pandas as pd
import xarray as xr

from .base import GriddedReader, _scientific_hygiene, register_reader
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("smap")
class SMAPReader(GriddedReader):
    """
    Reader for NASA SMAP (Soil Moisture Active Passive) data.
    """

    def open_dataset(
        self,
        files: str | list[str] | None = None,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str | None = None,
        product: str = "SPL3SMP",
        version: str = "009",
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads SMAP data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
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
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str], optional
            Dates to retrieve. Used for URL building (placeholder).
        product : str, optional
            SMAP product short name, by default "SPL3SMP".
        version : str, optional
            Product version, by default "009".
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The SMAP dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, product=product, version=version)

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = smap_preprocess

        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

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
        ds = update_history(ds, f"Read SMAP {product} data.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        product: str = "SPL3SMP",
        version: str = "009",
    ) -> list[str]:
        """
        Build URLs for SMAP data from NSIDC (placeholder).

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        product : str, optional
            SMAP product.
        version : str, optional
            SMAP version.

        Returns
        -------
        List[str]
            List of URLs.
        """
        # Remote access to SMAP usually requires Earthdata login and
        # often uses HTTPS or OPeNDAP from NSIDC.
        # This is a placeholder for URL structure.
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        urls = []
        for _ in dates:
            # Example: SPL3SMP.009/2024.01.01/SMAP_L3_SM_P_20240101_R19240_001.h5
            # The RXXXXX and XXX are orbit/version specific and hard to guess without search
            pass

        return urls


def smap_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess SMAP dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # SMAP L3 products are often on the EASE-2 grid.
    # Dimensions: 'lat', 'lon' or 'row', 'col'.

    # 1. Standardize coordinates
    ds = standardize_satellite_coords(
        ds,
        lat_name="latitude",
        lon_name="longitude",
        y_dim=["lat", "row"],
        x_dim=["lon", "col"],
    )

    # 2. Variable renaming (mapping common SMAP variables)
    mapping = {
        "soil_moisture": "soil_moisture",
        "soil_moisture_error": "soil_moisture_error",
        "surface_temp": "surface_temperature",
        "vegetation_water_content": "vwc",
        "freeze_thaw_fraction": "freeze_thaw",
    }
    # Some SMAP variables are nested in groups in the HDF5 file.
    # If the reader flattens them, we rename here.
    rename_dict = {
        old: new for old, new in mapping.items() if old in ds.variables and new not in ds.variables
    }
    if rename_dict:
        ds = ds.rename(rename_dict)

    # 3. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed SMAP data.")

    return ds
