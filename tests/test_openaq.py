import sys

import pandas as pd
import pytest

from monetio import openaq


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python 3.7+")
def test_openaq():
    # First date in the archive, just one file
    # Browse the archive at https://openaq-fetches.s3.amazonaws.com/index.html
    dates = pd.date_range(start="2013-11-26", end="2013-11-27", freq="H")[:-1]
    df = openaq.add_data(dates)
    assert not df.empty
    assert df.siteid.nunique() == 1
    assert (df.country == "CN").all() and ((df.time_local - df.time) == pd.Timedelta(hours=8)).all()
    assert df.latitude.isnull().sum() == 0
    assert df.longitude.isnull().sum() == 0
    assert df.dtypes["averagingPeriod"] == "timedelta64[ns]"
    assert df.averagingPeriod.eq(pd.Timedelta("1H")).all()


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


def test_openaq_2023():
    # Period from Jordan's NRT example (#130)
    df = openaq.add_data(["2023-09-04", "2023-09-04 23:00"], n_procs=2)  # many files
    assert len(df) > 0
    assert df.dtypes["averagingPeriod"] == "timedelta64[ns]"
    assert not df.averagingPeriod.isnull().all()
    assert df.averagingPeriod.dropna().gt(pd.Timedelta(0)).all()


# df = openaq.read_json(
#     # "https://openaq-fetches.s3.amazonaws.com/realtime/2019-08-01/1564644065.ndjson"  # 1 MB
#     "https://openaq-fetches.s3.amazonaws.com/realtime/2023-09-04/1693798742_realtime_1c4e466d-c461-4c8d-b604-1e81cf2df73a.ndjson"  # 10 MB
# )

# o = openaq.OPENAQ()
# days_avail = o._get_available_days(pd.date_range("2019-08-01", "2019-08-03"))
# files = o._get_files_in_day(pd.to_datetime("2019-08-01"))

# from dask.diagnostics import ProgressBar

# ProgressBar().register()

# # df = openaq.add_data(["2016-08-01", "2016-08-01 23:00"], n_procs=1)  # one file
# df = openaq.add_data(["2023-09-04", "2023-09-04 23:00"], n_procs=4)  # many files

# df = openaq.read_json2(
#     # "https://openaq-fetches.s3.amazonaws.com/realtime/2019-08-01/1564644065.ndjson"  # 1 MB
#     # "https://openaq-fetches.s3.amazonaws.com/realtime/2023-09-04/1693798742_realtime_1c4e466d-c461-4c8d-b604-1e81cf2df73a.ndjson"  # 10 MB
# )
