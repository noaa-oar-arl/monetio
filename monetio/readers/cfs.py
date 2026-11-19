"""NCEP Climate Forecast System Reanalysis and operational analyses."""

import datetime

import pandas as pd

from .base import register_reader
from .ncep_pds import NCEPPDSReader


class _CFSReader(NCEPPDSReader):
    """Shared GRIB2 reader for NCEP CFS analysis products."""

    archive = ""
    filename = ""
    product_name = ""

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int | list[int] = 0,
        **kwargs,
    ) -> list[str]:
        """Build NCEI archive paths for six-hourly pressure-level analyses."""
        if isinstance(dates, str | datetime.datetime | pd.Timestamp):
            dates = pd.DatetimeIndex([pd.to_datetime(dates)])
        else:
            dates = pd.DatetimeIndex(pd.to_datetime(dates))
        hours = [hour] if isinstance(hour, int) else list(hour)
        invalid = sorted(set(hours).difference({0, 6, 12, 18}))
        if invalid:
            raise ValueError(f"hour must contain only 0, 6, 12, or 18; got {invalid}")

        urls = []
        for date in dates.normalize().unique():
            day = pd.Timestamp(date)
            for cycle_hour in hours:
                stamp = day + pd.Timedelta(hours=cycle_hour)
                urls.append(
                    f"{self.archive}/{day:%Y}/{day:%Y%m}/{day:%Y%m%d}/"
                    f"{self.filename.format(stamp=stamp)}"
                )
        return urls


@register_reader("cfsr")
class CFSRReader(_CFSReader):
    """Read NCEP Climate Forecast System Reanalysis (CFSR) pressure analyses."""

    archive = "https://www.ncei.noaa.gov/oa/prod-cfs-reanalysis/6-hourly-by-pressure-level"
    filename = "pgbh{stamp:%H}.gdas.{stamp:%Y%m%d%H}.grb2"
    product_name = "CFSR"


@register_reader("cfsv2")
class CFSv2Reader(_CFSReader):
    """Read NCEP Climate Forecast System version 2 operational analyses."""

    archive = "https://www.ncei.noaa.gov/oa/prod-cfs-operational-analysis/6-hourly-by-pressure"
    filename = "cdas1.t{stamp:%H}z.pgrbh00.grib2"
    product_name = "CFSv2"
