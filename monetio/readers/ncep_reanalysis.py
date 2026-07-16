"""NCEP Reanalysis Reader"""

import datetime

import pandas as pd
import xarray as xr

from .base import GriddedReader, _scientific_hygiene, register_reader
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("ncep_reanalysis")
class NCEPReanalysisReader(GriddedReader):
    """
    Reader for NCEP/NCAR Reanalysis 1 and NCEP/DOE Reanalysis 2 data.
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
        product: str = "reanalysis1",
        variable: str = "air",
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads NCEP Reanalysis data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File path(s) or URL(s). If None, will try to build URLs using dates and product.
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
            Dates to retrieve. Used if files is None.
        product : str, optional
            NCEP Reanalysis product, by default "reanalysis1".
            Options: "reanalysis1", "reanalysis2".
        variable : str, optional
            Variable name to retrieve (e.g., 'air', 'uwnd', 'vwnd'), by default 'air'.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The NCEP Reanalysis dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, product=product, variable=variable)

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = ncep_reanalysis_preprocess
        ds = super().open_dataset(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="netcdf3",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            **kwargs,
        )

        # Update history
        ds = update_history(ds, f"Read NCEP {product} {variable} data.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        product: str = "reanalysis1",
        variable: str = "air",
    ) -> list[str]:
        """
        Build OPeNDAP URLs for NCEP Reanalysis data based on dates and product.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        product : str, optional
            NCEP Reanalysis product.
        variable : str, optional
            Variable name.

        Returns
        -------
        List[str]
            List of OPeNDAP URLs.
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        if product == "reanalysis1":
            base_url = "https://psl.noaa.gov/thredds/dodsC/Datasets/ncep.reanalysis/pressure"
        elif product == "reanalysis2":
            base_url = "https://psl.noaa.gov/thredds/dodsC/Datasets/ncep.reanalysis2/pressure"
        else:
            raise ValueError(f"Unknown product: {product}")

        urls = []
        for year in dates.year.unique():
            url = f"{base_url}/{variable}.{year}.nc"
            urls.append(url)

        return urls


def ncep_reanalysis_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess NCEP Reanalysis dataset: standardize coordinates and metadata.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # 1. Standardize dimensions and coordinates
    # NCEP Reanalysis typically uses 'lat', 'lon', 'time', 'level'.
    ds = standardize_satellite_coords(
        ds,
        lat_name="lat",
        lon_name="lon",
        y_dim=["lat"],
        x_dim=["lon"],
        z_dim=["level", "lev"],
    )

    # NCEP Lon is often 0-360, convert to -180 to 180 if needed
    if "longitude" in ds.coords:
        if (ds.coords["longitude"] > 180).any():
            ds.coords["longitude"] = (ds.coords["longitude"] + 180) % 360 - 180
            ds = ds.sortby("longitude")

    # 2. Expand 1D coords to 2D
    if "latitude" in ds.coords and ds["latitude"].ndim == 1:
        if "longitude" in ds.coords and ds["longitude"].ndim == 1:
            lons, lats = xr.broadcast(ds.longitude, ds.latitude)
            ds = ds.assign_coords(longitude=lons, latitude=lats)

    # 3. Variable renaming
    mapping = {
        "uwnd": "u_wind",
        "vwnd": "v_wind",
        "air": "temperature",
        "shum": "specific_humidity",
        "rhum": "relative_humidity",
        "pr_wtr": "precipitable_water",
        "hpbl": "pbl_height",
        "pres": "pressure",
        "slp": "mean_sea_level_pressure",
    }
    rename_dict = {
        old: new for old, new in mapping.items() if old in ds.variables and new not in ds.variables
    }
    if rename_dict:
        ds = ds.rename(rename_dict)

    # 4. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed NCEP Reanalysis data.")

    return ds
