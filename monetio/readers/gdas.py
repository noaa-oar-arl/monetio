"""GDAS atmospheric GRIB2 analyses and short forecasts."""

import datetime
from typing import Any

import pandas as pd

from .base import register_reader
from .ncep_pds import NCEPPDSReader


@register_reader("gdas")
class GDASReader(NCEPPDSReader):
    """Read GDAS GRIB2 files locally or from NOAA AWS/NOMADS archives."""

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        product: str = "pgrb2.0p25",
        source: str = "aws",
        **kwargs: Any,
    ) -> list[str]:
        """Build AWS or NOMADS paths for GDAS analysis/forecast GRIB2 files."""
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.to_datetime(dates)
        if isinstance(lead_time, int):
            lead_times = [lead_time]
        else:
            lead_times = lead_time
        if source.lower() not in {"aws", "nomads"}:
            raise ValueError("source must be 'aws' or 'nomads'")

        urls = []
        for date in dates:
            date_str = date.strftime("%Y%m%d")
            cycle = f"{hour:02d}"
            for lead in lead_times:
                lead_str = f"{lead:03d}"
                path = f"gdas.{date_str}/{cycle}/atmos/gdas.t{cycle}z.{product}.f{lead_str}"
                if source.lower() == "aws":
                    urls.append(f"s3://noaa-gfs-bdp-pds/{path}")
                else:
                    urls.append(f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/{path}")
        return urls
