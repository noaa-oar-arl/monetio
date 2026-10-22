"""NESDIS EPS VIIRS Reader"""

import datetime

import numpy as np
import pandas as pd
import xarray as xr

from .base import GriddedReader, register_reader
from .sat_utils import add_time_coord, standardize_satellite_coords, update_history


@register_reader("nesdis_eps_viirs")
class NESDISEPSVIIRSReader(GriddedReader):
    """
    Reader for NESDIS EPS VIIRS (Enterprise Processing System) AOT data.
    Available on NOAA STAR FTP.
    """

    def open_dataset(
        self,
        files: str | list[str] = None,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str = None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Reads NESDIS EPS VIIRS data.

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
            Dates to retrieve. If files is None, this is used to build URLs.
        **kwargs : dict
            Additional arguments passed to XarrayDriver.open.

        Returns
        -------
        xr.Dataset
            The NESDIS EPS VIIRS dataset.
        """
        if files is None:
            if dates is None:
                raise ValueError("Either 'files' or 'dates' must be provided.")
            files = self.build_urls(dates)

        if "preprocess" not in kwargs:
            kwargs["preprocess"] = nesdis_eps_viirs_preprocess

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
        ds = update_history(ds, "Read NESDIS EPS VIIRS data.")

        return ds

    def build_urls(
        self, dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str
    ) -> list[str]:
        """
        Build FTP URLs for NESDIS EPS VIIRS data based on dates.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.

        Returns
        -------
        List[str]
            List of FTP URLs.
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        server = "ftp.star.nesdis.noaa.gov"
        base_dir = "/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550"

        urls = []
        for d in dates:
            year = d.strftime("%Y")
            yyyymmdd = d.strftime("%Y%m%d")
            # Example: ftp://ftp.star.nesdis.noaa.gov/pub/smcd/VIIRS_Aerosol/npp.viirs.aerosol.data/epsaot550/2023/npp_eaot_ip_gridded_0.25_20230101.high.nc
            url = f"ftp://{server}{base_dir}/{year}/npp_eaot_ip_gridded_0.25_{yyyymmdd}.high.nc"
            urls.append(url)
        return urls


def nesdis_eps_viirs_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess NESDIS EPS VIIRS dataset: assign coordinates and standardize.

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
    ds = standardize_satellite_coords(ds)

    # 2. Identify grid size and generate coordinates if needed
    # EPS files typically have nlat=720, nlon=1440
    if "latitude" not in ds.variables:
        nlat = ds.sizes.get("y", 720)
        nlon = ds.sizes.get("x", 1440)

        lon_min = -179.875
        lon_max = -1.0 * lon_min
        lat_min = -89.875
        lat_max = -1.0 * lat_min
        lons = np.linspace(lon_min, lon_max, nlon)
        # EPS uses descending latitudes (lat_max to lat_min)
        lats = np.linspace(lat_max, lat_min, nlat)

        # Lazy coordinate generation using xr.broadcast
        lon1d = xr.DataArray(lons, dims=("x",), name="longitude")
        lat1d = xr.DataArray(lats, dims=("y",), name="latitude")
        lat2d, lon2d = xr.broadcast(lat1d, lon1d)

        ds = ds.assign_coords(
            latitude=lat2d.assign_attrs({"units": "degrees_north", "standard_name": "latitude"}),
            longitude=lon2d.assign_attrs({"units": "degrees_east", "standard_name": "longitude"}),
        )

    # 3. Handle Time
    if "time" not in ds.coords:
        ds = add_time_coord(ds, time_attr="time_coverage_start")
    if "time" not in ds.coords:
        ds = add_time_coord(ds, time_attr="DATE")

    # 4. Final cleaning and standardization
    if "aot_ip_out" in ds.data_vars:
        ds = ds.rename({"aot_ip_out": "aod_550"})
        # Mask invalid values (e.g., negative)
        ds["aod_550"] = ds["aod_550"].where(ds["aod_550"] >= 0)
        ds["aod_550"].attrs.update(
            {
                "long_name": "Aerosol Optical Thickness at 550nm",
                "units": "1",
                "standard_name": "atmosphere_optical_thickness_due_to_ambient_aerosol",
            }
        )

    return ds
