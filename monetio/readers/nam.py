"""NAM (North American Mesoscale) Reader"""

import datetime
from typing import Any

import pandas as pd

from .base import register_reader
from .ncep_pds import NCEPPDSReader


@register_reader("nam")
class NAMReader(NCEPPDSReader):
    """
    Reader for NAM (North American Mesoscale) on AWS or NOMADS.
    """

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        product: str = "conusnest.hiresf",
        source: str = "aws",
        **kwargs: Any,
    ) -> list[str]:
        """
        Build URLs for NAM data.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        hour : int, optional
            Forecast cycle hour (0, 6, 12, 18). Default is 0.
        lead_time : Union[int, List[int]], optional
            Forecast lead time(s). Default is 0.
        product : str, optional
            Product string. Examples: 'conusnest.hiresf', 'awip32'.
        source : str, optional
            Data source: 'aws' (default) or 'nomads'.
        **kwargs : Any
            Additional arguments.

        Returns
        -------
        List[str]
            List of URLs.
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
                lt_str = f"{lt:02d}"
                if source.lower() == "aws":
                    bucket = "noaa-nam-pds"
                    # s3://noaa-nam-pds/nam.20250325/nam.t00z.conusnest.hiresf00.tm00.grib2
                    url = f"s3://{bucket}/nam.{d_str}/nam.t{h_str}z.{product}{lt_str}.tm00.grib2"
                else:
                    # https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.20250325/nam.t00z.conusnest.hiresf00.tm00.grib2
                    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.{d_str}/nam.t{h_str}z.{product}{lt_str}.tm00.grib2"
                urls.append(url)
        return urls
