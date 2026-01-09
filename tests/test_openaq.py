import sys
from urllib.error import HTTPError

import pandas as pd
import pytest

from monetio import openaq

if sys.version_info < (3, 7):
    pytest.skip("requires Python 3.7+", allow_module_level=True)

# openaq._URL_CAP_RANDOM_SAMPLE = True
openaq._URL_CAP = 4

# First date in the archive, just one file
# Browse the archive at https://openaq-fetches.s3.amazonaws.com/index.html
FIRST_DAY = pd.date_range(start="2013-11-26", end="2013-11-27", freq="H")[:-1]

permission_error = pytest.mark.xfail(reason="private", raises=PermissionError, strict=True)

forbidden_error = pytest.mark.xfail(reason="forbidden", raises=HTTPError, strict=True)  # 403


@permission_error
def test_openaq_first_date():
    dates = FIRST_DAY
    df = openaq.add_data(dates)
    assert not df.empty
    assert df.siteid.nunique() == 1
    assert (df.country == "CN").all() and ((df.time_local - df.time) == pd.Timedelta(hours=8)).all()

    assert df.latitude.isnull().sum() == 0
    assert df.longitude.isnull().sum() == 0

    assert df.dtypes["averagingPeriod"] == "timedelta64[ns]"
    assert df.averagingPeriod.eq(pd.Timedelta("1H")).all()

    assert df.pm25_ugm3.gt(0).all()


@permission_error
def test_openaq_long_fmt():
    dates = FIRST_DAY
    df = openaq.add_data(dates, wide_fmt=False)

    assert not df.empty

    assert {"parameter", "value", "unit"} < set(df.columns)
    assert "pm25_ugm3" not in df.columns
    assert "pm25" in df.parameter.values


@forbidden_error
@pytest.mark.parametrize(
    "url",
    [
        "https://openaq-fetches.s3.amazonaws.com/realtime/2019-08-01/1564644065.ndjson",  # 1 MB
        "https://openaq-fetches.s3.amazonaws.com/realtime/2023-09-04/1693798742_realtime_1c4e466d-c461-4c8d-b604-1e81cf2df73a.ndjson",  # 10 MB"
    ],
)
def test_read(url):
    df = openaq.read_json(url)
    df2 = openaq.read_json2(url)

    assert len(df) > 0

    if "2019-08-01" in url:
        assert len(df2) < len(df), "some that didn't have coords were skipped"
        assert df.latitude.isnull().sum() > 0
    else:
        assert len(df2) == len(df)

    assert df.dtypes["averagingPeriod"] == "timedelta64[ns]"
    assert not df.averagingPeriod.isnull().all()
    assert df.averagingPeriod.dropna().gt(pd.Timedelta(0)).all()


@permission_error
def test_openaq_2023():
    # Period from Jordan's NRT example (#130)
    # There are many files in this period (~ 100?)
    # Disable cap setting to test whole set of files
    # NOTE: possible to get empty df with the random URL selection
    df = openaq.add_data(["2023-09-04", "2023-09-04 23:00"], n_procs=2)

    assert len(df) > 0

    assert (df.time.astype(str) + df.siteid).nunique() == len(df)

    assert df.dtypes["averagingPeriod"] == "timedelta64[ns]"
    assert not df.averagingPeriod.isnull().all()
    assert df.averagingPeriod.dropna().gt(pd.Timedelta(0)).all()

    assert df.pm25_ugm3.dropna().gt(0).all()
    assert df.o3_ppm.dropna().gt(0).all()


def test_parameter_coverage():
    # From https://openaq.org/developers/help/ ("What pollutants are available on OpenAQ?")
    # these are the parameters to account for:
    params = [
        "pm1",
        "pm25",
        "pm4",
        "pm10",
        "bc",
        "o3",
        "co",
        "no2",
        "no",
        "nox",
        "so2",
        "ch4",
        "co2",
    ]
    assert len(params) == 13
    assert sorted(openaq.OPENAQ.NON_MOLEC_PARAMS + list(openaq.OPENAQ.PPM_TO_UGM3)) == sorted(
        params
    )
