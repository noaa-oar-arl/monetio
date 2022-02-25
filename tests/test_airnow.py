import pandas as pd

from monetio import airnow


def _check_df(df):
    assert df.index.size >= 1
    assert not any(col in df.columns for col in ["index", "level_0"])


def test_build_urls():
    dates = pd.date_range("2021/01/01", "2021/01/05", freq="H")

    # Hourly
    urls, fnames = airnow.build_urls(dates)
    assert urls.size == fnames.size == dates.size == 4 * 24 + 1
    assert fnames[0] == dates[0].strftime(r"HourlyData_%Y%m%d%H.dat")

    # Daily
    urls, fnames = airnow.build_urls(dates, daily=True)
    assert urls.size == fnames.size == 5
    assert (fnames == "daily_data.dat").all()


def test_add_data_hourly():
    dates = pd.date_range("2021/07/01", periods=3, freq="H")

    # Wide format (default)
    df = airnow.add_data(dates)
    _check_df(df)
    assert all(col in df.columns for col in ["OZONE", "OZONE_unit"])

    # Non-wide
    df = airnow.add_data(dates, wide_fmt=False)
    _check_df(df)
    assert all(col in df.columns for col in ["variable", "units", "obs"])


def test_add_data_daily():
    dates = pd.date_range("2021/07/01", "2021/07/03")  # 3 days

    # Wide format (default)
    df = airnow.add_data(dates, daily=True)
    _check_df(df)
    assert all(
        col in df.columns for col in ["OZONE-1HR", "OZONE-8HR", "OZONE-1HR_unit", "OZONE-8HR_unit"]
    )
    assert df.time.unique().size == 3

    # Non-wide
    df = airnow.add_data(dates, daily=True, wide_fmt=False)
    _check_df(df)
    assert all(col in df.columns for col in ["variable", "units", "obs"])
    assert df.time.unique().size == 3
