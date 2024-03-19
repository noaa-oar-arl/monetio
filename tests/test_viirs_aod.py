import sys

import pandas as pd
import pytest

from monetio.sat.nesdis_viirs_aod_aws_gridded import open_dataset, open_mfdataset

if sys.version_info < (3, 7):
    pytest.skip("s3fs requires Python 3.7+", allow_module_level=True)


@pytest.mark.parametrize("sat", ["SNPP", "NOAA20"])
@pytest.mark.parametrize("res", [0.05, 0.1, 0.25])
def test_open_dataset_daily(sat, res):
    date = "2020-01-01"  # a date when we have both SNPP and NOAA-20 data available
    s_res = f"{res:.3f}"

    ds = open_dataset(date, sat, s_res)
    assert set(ds.dims) == {"time", "lat", "lon"}
    assert ds.sizes["time"] == 1
    assert ds.sizes["lat"] == int(180 / res)
    assert ds.sizes["lon"] == int(360 / res)
    assert ds.attrs["satellite_name"] == ("NPP" if sat == "SNPP" else "NOAA 20")
    assert ds.attrs["spatial_resolution"].strip().startswith(str(res))
    assert (ds.time == pd.DatetimeIndex([date])).all()


def test_open_dataset_bad_input():
    with pytest.raises(ValueError, match="Invalid input"):
        open_dataset("2020-01-01", satellite="GOES-16")

    with pytest.raises(ValueError, match="Invalid input"):
        open_dataset("2020-01-01", satellite="both")

    with pytest.raises(ValueError, match="Invalid input"):
        open_dataset("2020-01-01", data_resolution=100)

    with pytest.raises(ValueError, match="Invalid input"):
        open_dataset("2020-01-01", averaging_time="asdf")


def test_open_dataset_no_data():
    with pytest.raises(ValueError, match="File does not exist on AWS:"):
        open_dataset("1900-01-01")


def test_open_mfdataset_bad_input():
    cases = [
        {"satellite": "GOES-16"},
        {"satellite": "both"},
        {"data_resolution": 100},
        {"averaging_time": "asdf"},
    ]
    for case in cases:
        with pytest.raises(ValueError, match="Invalid input"):
            open_mfdataset(["2020-01-01"], **case)


def test_open_mfdataset_daily():
    ds = open_mfdataset(["2020-01-01", "2020-01-02"], satellite="SNPP", data_resolution=0.25)
    assert set(ds.dims) == {"time", "lat", "lon"}
    assert ds.sizes["time"] == 2
    assert ds.attrs["spatial_resolution"].strip().startswith("0.25")
    assert (ds.time == pd.DatetimeIndex(["2020-01-01", "2020-01-02"])).all()


def test_open_mfdataset_monthly():
    with pytest.raises(ValueError, match="not the same length"):
        open_mfdataset(["2020-01-01", "2020-01-02"], averaging_time="monthly")

    months = pd.date_range(start="2020-01-01", freq="MS", periods=2)
    ds = open_mfdataset(months, averaging_time="monthly")
    assert ds.sizes["time"] == 2


def test_open_mfdataset_daily_warning():
    dates = ["2012-01-18", "2012-01-19"]  # 2012-01-19 is the first available date

    # Warn and skip by default
    with pytest.warns(match="File does not exist on AWS:"):
        ds = open_mfdataset(
            dates,
            satellite="SNPP",
            data_resolution=0.25,
            averaging_time="daily",
        )
    assert ds.sizes["time"] == 1
    assert (ds.time == pd.DatetimeIndex([dates[1]])).all()

    # Error optionally
    with pytest.raises(ValueError, match="File does not exist on AWS:"):
        ds = open_mfdataset(
            dates,
            satellite="SNPP",
            data_resolution=0.25,
            averaging_time="daily",
            error_missing=True,
        )


def test_open_mfdataset_monthly_warning():
    dates = ["2011-12-01", "2012-01-01"]

    # Warn and skip by default
    with pytest.warns(match="File does not exist on AWS:"):
        ds = open_mfdataset(
            dates,
            satellite="SNPP",
            data_resolution=0.25,
            averaging_time="monthly",
        )
    assert ds.sizes["time"] == 1
    assert (ds.time == pd.DatetimeIndex([dates[1]])).all()

    # Error optionally
    with pytest.raises(ValueError, match="File does not exist on AWS:"):
        ds = open_mfdataset(
            dates,
            satellite="SNPP",
            data_resolution=0.25,
            averaging_time="monthly",
            error_missing=True,
        )


def test_open_mfdataset_no_data():
    with (
        pytest.raises(ValueError, match="Files not available"),
        pytest.warns(match="File does not exist on AWS:"),
    ):
        open_mfdataset(["1900-01-01"])
