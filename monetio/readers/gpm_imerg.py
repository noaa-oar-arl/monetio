"""GPM IMERG (Global Precipitation Measurement - IMERG) Reader"""

import datetime

import pandas as pd
import xarray as xr

from .base import GriddedReader, _scientific_hygiene, register_reader
from .sat_utils import standardize_satellite_coords, update_history


@register_reader("gpm_imerg")
class GPMIMERGReader(GriddedReader):
    """
    Reader for GPM IMERG (Global Precipitation Measurement - Integrated Multi-satellitE
    Retrievals for GPM) data.
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
        product: str = "3B-HHR",
        version: str = "07",
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads GPM IMERG data.

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
            IMERG product, by default "3B-HHR" (Half-hourly).
            Options include "3B-HHR", "3B-DAY" (Daily).
        version : str, optional
            IMERG version, by default "07".
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The GPM IMERG dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates, product=product, version=version)

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = gpm_imerg_preprocess

        # GPM IMERG HDF5 files often need the h5netcdf engine
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
        ds = update_history(ds, f"Read GPM IMERG {product} v{version} data.")

        return ds

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        product: str = "3B-HHR",
        version: str = "07",
    ) -> list[str]:
        """
        Build OPeNDAP URLs for GPM IMERG data from NASA GES DISC.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        product : str, optional
            IMERG product.
        version : str, optional
            IMERG version.

        Returns
        -------
        List[str]
            List of OPeNDAP URLs.
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        # Base OPeNDAP URL for GES DISC
        # Example for 3B-HHR: https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L3/GPM_3IMERGHH.07/
        if product == "3B-HHR":
            base_url = f"https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L3/GPM_3IMERGHH.{version}"
        elif product == "3B-DAY":
            base_url = f"https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L3/GPM_3IMERGDF.{version}"
        else:
            raise ValueError(f"URL building for product {product} not implemented.")

        urls = []
        for d in dates:
            if product == "3B-HHR":
                # IMERG half-hourly files are every 30 mins
                # Format: 3B-HHR.MS.MRG.3IMERG.20240101-S000000-E002959.0000.V07B.HDF5
                date_str = d.strftime("%Y%m%d")
                # This is a simplification, exact minute/second might vary or need rounding
                # Usually it's S000000-E002959, S003000-E005959 etc.
                min_start = (d.minute // 30) * 30
                start_time = f"S{d.hour:02d}{min_start:02d}00"
                end_min = min_start + 29
                end_time = f"E{d.hour:02d}{end_min:02d}59"
                # The .0000. might also change depending on the product stream
                url = f"{base_url}/{d.strftime('%Y/%m')}/3B-HHR.MS.MRG.3IMERG.{date_str}-{start_time}-{end_time}.{d.hour * 60 + min_start:04d}.V{version}B.HDF5"
            else:
                # Daily: 3B-DAY.MS.MRG.3IMERG.20240101-S000000-E235959.V07B.nc4
                date_str = d.strftime("%Y%m%d")
                url = f"{base_url}/{d.strftime('%Y')}/3B-DAY.MS.MRG.3IMERG.{date_str}-S000000-E235959.V{version}B.nc4"
            urls.append(url)

        return urls


def gpm_imerg_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess GPM IMERG dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input dataset.

    Returns
    -------
    xr.Dataset
        Processed dataset.
    """
    # GPM IMERG often has groups or nested structures in HDF5.
    # If the dataset was opened with groups, it might need flattening or subsetting.
    # Usually, the variables are in the 'Grid' group.

    # 1. Standardize coordinates
    ds = standardize_satellite_coords(
        ds,
        lat_name="lat",
        lon_name="lon",
        y_dim=["lat", "nlat"],
        x_dim=["lon", "nlon"],
    )

    # 2. Variable renaming
    mapping = {
        "precipitationCal": "precipitation",
        "precipitationUncal": "precipitation_uncal",
        "HQprecipitation": "precipitation_hq",
        "probabilityLiquidPrecipitation": "prob_liquid_precip",
    }
    rename_dict = {
        old: new for old, new in mapping.items() if old in ds.variables and new not in ds.variables
    }
    if rename_dict:
        ds = ds.rename(rename_dict)

    # 3. Handle transpose if needed (GPM is often Lon, Lat)
    if "x" in ds.dims and "y" in ds.dims:
        if ds.precipitation.dims[0] == "x":
            ds = ds.transpose("time", "y", "x", ...)

    # 4. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed GPM IMERG data.")

    return ds
