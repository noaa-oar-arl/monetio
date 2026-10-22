"""Offline checks for reader selectors that bound file discovery."""

import pandas as pd
import pytest

from monetio.readers.gfs import GDASReader, GEFSReader, GFSReader
from monetio.readers.hrrr import HRRRReader
from monetio.readers.nam import NAMReader
from monetio.readers.rap import RAPReader
from monetio.readers.rrfs import RRFSReader
from monetio.readers.skynet import SKYNETReader
from monetio.readers.solrad import SOLRADReader
from monetio.readers.surfrad import SURFRADReader


@pytest.mark.parametrize(
    "reader,sites,expected",
    [
        (
            SURFRADReader(),
            ["bon"],
            [
                "https://gml.noaa.gov/aftp/data/radiation/surfrad/Bondville_IL/2024/bon24001.dat",
                "https://gml.noaa.gov/aftp/data/radiation/surfrad/Bondville_IL/2025/bon25001.dat",
            ],
        ),
        (
            SOLRADReader(),
            ["abq"],
            [
                "https://gml.noaa.gov/aftp/data/radiation/solrad/abq/2024/abq24001.dat",
                "https://gml.noaa.gov/aftp/data/radiation/solrad/abq/2025/abq25001.dat",
            ],
        ),
    ],
    ids=["surfrad", "solrad"],
)
def test_radiation_date_site_selectors_bound_daily_files(reader, sites, expected):
    dates = pd.to_datetime(["2024-01-01", "2025-01-01"])
    assert reader.build_urls(dates, sites) == expected


def test_skynet_date_site_product_selectors_bound_daily_files():
    urls = SKYNETReader().build_urls(
        dates=["2023-01-01", "2023-01-02", "2024-01-01"],
        siteid="TEST_SITE",
        product="AOT",
    )
    assert urls == [
        "https://www.skynet-isdc.org/data/L2/AOT/TEST_SITE/2023/TEST_SITE_20230101.AOT",
        "https://www.skynet-isdc.org/data/L2/AOT/TEST_SITE/2023/TEST_SITE_20230102.AOT",
        "https://www.skynet-isdc.org/data/L2/AOT/TEST_SITE/2024/TEST_SITE_20240101.AOT",
    ]


@pytest.mark.parametrize(
    "reader,expected_parts",
    [
        (GFSReader(), ["gfs.20250324", "/06/", ".f000", ".f003"]),
        (GEFSReader(), ["gefs.20250324", "/06/", ".f000", ".f003"]),
        (GDASReader(), ["gdas.20250324", "/06/", ".f000", ".f003"]),
        (HRRRReader(), ["hrrr.20250324", "t06z", "f00", "f03"]),
        (NAMReader(), ["nam.20250324", "t06z", "f00", "f03"]),
        (RAPReader(), ["rap.20250324", "t06z", "f00", "f03"]),
        (RRFSReader(), ["rrfs.20250324", "t06z", "f000", "f003"]),
    ],
    ids=["gfs", "gefs", "gdas", "hrrr", "nam", "rap", "rrfs"],
)
def test_ncep_forecast_selectors_bound_cycle_and_lead_urls(reader, expected_parts):
    urls = reader.build_urls(dates="2025-03-24", hour=6, lead_time=[0, 3])
    assert len(urls) == 2
    assert all(expected_parts[0] in url for url in urls)
    assert all(expected_parts[1] in url for url in urls)
    assert any(expected_parts[2] in url for url in urls)
    assert any(expected_parts[3] in url for url in urls)
    assert len(set(urls)) == 2
