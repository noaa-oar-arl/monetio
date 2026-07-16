"""GFS, GEFS, and GDAS Readers for AWS Open Data"""

import datetime
from collections.abc import Sequence
from typing import Any

import pandas as pd
import xarray as xr

from .base import register_reader
from .ncep_pds import NCEPPDSReader


@register_reader("gfs")
class GFSReader(NCEPPDSReader):
    """
    Reader for GFS (Global Forecast System) on AWS or NOMADS.
    """

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        product: str = "pgrb2.0p25",
        source: str = "aws",
        **kwargs: Any,
    ) -> list[str]:
        """
        Build URLs for GFS data.
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        if isinstance(lead_time, int):
            lead_times = [lead_time]
        else:
            lead_times = lead_time

        urls = []
        for d in dates:
            d_str = d.strftime("%Y%m%d")
            h_str = f"{hour:02d}"
            for lt in lead_times:
                lt_str = f"{lt:03d}"
                if source.lower() == "aws":
                    bucket = "noaa-gfs-bdp-pds"
                    url = (
                        f"s3://{bucket}/gfs.{d_str}/{h_str}/atmos/gfs.t{h_str}z.{product}.f{lt_str}"
                    )
                else:
                    # https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20250325/00/atmos/gfs.t00z.pgrb2.0p25.f000
                    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{d_str}/{h_str}/atmos/gfs.t{h_str}z.{product}.f{lt_str}"
                urls.append(url)
        return urls


@register_reader("gefs")
class GEFSReader(NCEPPDSReader):
    """
    Reader for GEFS (Global Ensemble Forecast System) on AWS or NOMADS.
    """

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        product: str = "geavg.tHHz.pgrb2a.0p50",
        source: str = "aws",
        **kwargs: Any,
    ) -> list[str]:
        """
        Build URLs for GEFS data.
        Note: product here usually specifies the member and resolution.
        Example: 'geavg.tHHz.pgrb2a.0p50' for ensemble mean 0.5 deg.
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        if isinstance(lead_time, int):
            lead_times = [lead_time]
        else:
            lead_times = lead_time

        urls = []
        h_str = f"{hour:02d}"
        for d in dates:
            d_str = d.strftime("%Y%m%d")
            for lt in lead_times:
                lt_str = f"{lt:03d}"
                # The product string might have 'tHHz' as a placeholder
                prod = product.replace("tHHz", f"t{h_str}z")
                if source.lower() == "aws":
                    bucket = "noaa-gefs-pds"
                    # s3://noaa-gefs-pds/gefs.20250324/00/atmos/pgrb2ap5/geavg.t00z.pgrb2a.0p50.f000
                    res_dir = "pgrb2ap5" if "0p50" in prod else "pgrb2bp5"
                    if product in ("aerosol", "chem", "a2d_0p25"):
                        url = (
                            f"s3://{bucket}/gefs.{d_str}/{h_str}/chem/pgrb2ap25/"
                            f"gefs.chem.t{h_str}z.a2d_0p25.f{lt_str}.grib2"
                        )
                    else:
                        url = f"s3://{bucket}/gefs.{d_str}/{h_str}/atmos/{res_dir}/{prod}.f{lt_str}"
                else:
                    # https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.20250325/00/atmos/pgrb2ap5/geavg.t00z.pgrb2a.0p50.f000
                    res_dir = "pgrb2ap5" if "0p50" in prod else "pgrb2bp5"
                    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.{d_str}/{h_str}/atmos/{res_dir}/{prod}.f{lt_str}"
                urls.append(url)
        return urls

    def open_chem(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        source: str = "aws",
        short_name: str | Sequence[str] | None = None,
        type_of_first_fixed_surface: int | None = None,
        value_of_first_fixed_surface: float | int | None = None,
        use_dask: bool = True,
        use_icechunk: bool = True,
        storage_options: dict[str, Any] | None = None,
        filters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """
        Open GEFS chemistry (single variable or all variables) with grib2io defaults.

        This wraps the common cloud-native workflow so users only provide dates
        and optionally tune backend knobs.
        """
        merged_filters: dict[str, Any] = {}
        if short_name is not None:
            if isinstance(short_name, str):
                merged_filters["shortName"] = short_name
            else:
                merged_filters["shortName"] = list(short_name)
        if type_of_first_fixed_surface is not None:
            merged_filters["typeOfFirstFixedSurface"] = type_of_first_fixed_surface
        if value_of_first_fixed_surface is not None:
            merged_filters["valueOfFirstFixedSurface"] = value_of_first_fixed_surface
        if filters:
            merged_filters.update(filters)

        merged_storage = dict(storage_options or {})
        merged_storage.setdefault("anon", True)

        open_kwargs: dict[str, Any] = {
            "dates": dates,
            "hour": hour,
            "lead_time": lead_time,
            "source": source,
            "product": "aerosol",
            "use_dask": use_dask,
            "use_icechunk": use_icechunk,
            "storage_options": merged_storage,
            **kwargs,
        }
        if merged_filters:
            open_kwargs["filters"] = merged_filters

        return self.open_dataset(**open_kwargs)

    def open_aerosol_aod550(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        source: str = "aws",
        use_dask: bool = True,
        use_icechunk: bool = True,
        storage_options: dict[str, Any] | None = None,
        filters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """Backward-compatible helper for GEFS chemistry AOD550."""

        return self.open_chem(
            dates=dates,
            hour=hour,
            lead_time=lead_time,
            source=source,
            short_name="totAOD550",
            type_of_first_fixed_surface=10,
            use_dask=use_dask,
            use_icechunk=use_icechunk,
            storage_options=storage_options,
            filters=filters,
            **kwargs,
        )


@register_reader("gdas")
class GDASReader(NCEPPDSReader):
    """
    Reader for GDAS (Global Data Assimilation System) on AWS or NOMADS.
    """

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        product: str = "pgrb2.0p25",
        source: str = "aws",
        **kwargs: Any,
    ) -> list[str]:
        """
        Build URLs for GDAS data.
        """
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)

        if isinstance(lead_time, int):
            lead_times = [lead_time]
        else:
            lead_times = lead_time

        urls = []
        for d in dates:
            d_str = d.strftime("%Y%m%d")
            h_str = f"{hour:02d}"
            for lt in lead_times:
                lt_str = f"{lt:03d}"
                if source.lower() == "aws":
                    bucket = "noaa-gfs-bdp-pds"
                    url = f"s3://{bucket}/gdas.{d_str}/{h_str}/atmos/gdas.t{h_str}z.{product}.f{lt_str}"
                else:
                    # https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gdas.20250325/00/atmos/gdas.t00z.pgrb2.0p25.f000
                    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gdas.{d_str}/{h_str}/atmos/gdas.t{h_str}z.{product}.f{lt_str}"
                urls.append(url)
        return urls
