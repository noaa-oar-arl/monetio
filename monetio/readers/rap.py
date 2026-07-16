"""RAP (Rapid Refresh) Reader"""

import datetime
from typing import Any

import pandas as pd

from .base import register_reader
from .ncep_pds import NCEPPDSReader


@register_reader("rap")
class RAPReader(NCEPPDSReader):
    """
    Reader for RAP (Rapid Refresh) on AWS or NOMADS.
    """

    def build_urls(
        self,
        dates: pd.DatetimeIndex | list[datetime.datetime] | datetime.datetime | str,
        hour: int = 0,
        lead_time: int | list[int] = 0,
        product: str = "awp130pgrb",
        source: str = "aws",
        **kwargs: Any,
    ) -> list[str]:
        """
        Build URLs for RAP data.

        Parameters
        ----------
        dates : Union[pd.DatetimeIndex, List[datetime], datetime, str]
            Dates to retrieve.
        hour : int, optional
            Forecast cycle hour (0-23). Default is 0.
        lead_time : Union[int, List[int]], optional
            Forecast lead time(s). Default is 0.
        product : str, optional
            Product string. Common options: 'awp130pgrb', 'awp252pgrb'.
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
                    bucket = "noaa-rap-pds"
                    # s3://noaa-rap-pds/rap.20250325/rap.t00z.awp130pgrbf00.grib2
                    url = f"s3://{bucket}/rap.{d_str}/rap.t{h_str}z.{product}f{lt_str}.grib2"
                else:
                    # https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.20250325/rap.t00z.awp130pgrbf00.grib2
                    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.{d_str}/rap.t{h_str}z.{product}f{lt_str}.grib2"
                urls.append(url)
        return urls
