"""RRFS Reader for AWS Open Data"""

import datetime
from typing import Any

import pandas as pd
import xarray as xr

from .base import _format_units, _scientific_hygiene, register_reader
from .ncep_pds import NCEPPDSReader
from .sat_utils import update_history


@register_reader("rrfs")
class RRFSReader(NCEPPDSReader):
    """
    Reader for RRFS (Rapid Refresh Forecast System) on AWS.
    """

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        product: str = "prslev.3km",
        domain: str = "conus",
        **kwargs: Any,
    ) -> list[str]:
        """
        Build S3 URLs for RRFS data.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime.datetime], datetime.datetime, str]
            Dates to retrieve.
        hour : int, optional
            Forecast cycle hour, by default 0.
        lead_time : Union[int, List[int]], optional
            Forecast lead time(s), by default 0.
        product : str, optional
            Product string, by default "prslev.3km".
        domain : str, optional
            Domain string (e.g., 'conus', 'ak', 'na', 'pr', 'hi'), by default "conus".
        **kwargs : Any
            Additional arguments.

        Returns
        -------
        List[str]
            List of S3 URLs.

        Examples
        --------
        >>> reader = RRFSReader()
        >>> urls = reader.build_urls("2026-03-28", hour=0, lead_time=1)
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        if isinstance(lead_time, int):
            lead_times = [lead_time]
        else:
            lead_times = lead_time

        bucket = "noaa-rrfs-pds"
        urls = []
        for d in dates:
            d_str = d.strftime("%Y%m%d")
            h_str = f"{hour:02d}"
            for lt in lead_times:
                lt_str = f"{lt:03d}"
                # s3://noaa-rrfs-pds/rrfs_a/rrfs.20260328/00/rrfs.t00z.prslev.3km.f000.conus.grib2
                url = f"s3://{bucket}/rrfs_a/rrfs.{d_str}/{h_str}/rrfs.t{h_str}z.{product}.f{lt_str}.{domain}.grib2"
                urls.append(url)
        return urls

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
        hour: int = 0,
        lead_time: int | list[int] = 0,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Reads RRFS data.

        Parameters
        ----------
        files : Union[str, List[str]], optional
            File path(s) or S3 URL(s).
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
        dates : Union[pd.DatetimeIndex, List[datetime.datetime], datetime.datetime, str], optional
            Dates to retrieve. If files is None, this is used to build URLs.
        hour : int, optional
            Forecast cycle hour, by default 0.
        lead_time : Union[int, List[int]], optional
            Forecast lead time(s), by default 0.
        **kwargs : Any
            Additional arguments. Common options for RRFS:
            product : str, optional
                Product string, by default "prslev.3km".
            domain : str, optional
                Domain string, by default "conus".

        Returns
        -------
        xr.Dataset
            The RRFS dataset.
        """
        if "preprocess" not in kwargs:
            kwargs["preprocess"] = rrfs_preprocess

        # product and domain defaults for RRFS
        if "product" not in kwargs:
            kwargs["product"] = "prslev.3km"
        if "domain" not in kwargs:
            kwargs["domain"] = "conus"

        return super().open_dataset(
            files=files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser="grib2",
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            dates=dates,
            hour=hour,
            lead_time=lead_time,
            **kwargs,
        )

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Harmonize RRFS metadata to monetio standards.

        Parameters
        ----------
        ds : xr.Dataset
            Input RRFS dataset.

        Returns
        -------
        xr.Dataset
            Harmonized dataset.
        """
        # First use parent harmonization (NCEP standards)
        ds = super().harmonize(ds)

        # Add RRFS-specific renaming or transformations if needed
        # (Parent handles most NCEP products well)

        return ds


def rrfs_preprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocess function for a single RRFS file.

    Parameters
    ----------
    ds : xarray.Dataset
        Input RRFS dataset.

    Returns
    -------
    xarray.Dataset
        Processed dataset.

    Examples
    --------
    >>> # ds = rrfs_preprocess(ds)
    """
    # 1. Format Units
    ds = _format_units(ds)

    # 2. Scientific Hygiene
    ds = _scientific_hygiene(ds)

    # Update history
    ds = update_history(ds, "Preprocessed RRFS data.")

    return ds
