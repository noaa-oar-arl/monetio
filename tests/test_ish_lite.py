import pandas as pd
import pytest

from monetio import ish_lite


def test_ish_read_history():
    dates = pd.date_range("2020-09-01", "2020-09-02")
    ish = ish_lite.ISH()
    ish.dates = dates
    ish.read_ish_history()

    df = ish.history

    assert len(df) > 0
    assert {"latitude", "longitude", "begin", "end"} < set(df.columns)
    for col in ["begin", "end"]:
        assert df[col].dtype == "datetime64[ns]"
        assert (df[col].dt.hour == 0).all()

    assert df.station_id.nunique() == len(df), "unique ID for station"
    assert (df.usaf.value_counts() == 2).sum() == 2
    assert (df.wban.value_counts() == 2).sum() == 5
    assert (df.usaf == "999999").sum() > 100
    assert (df.wban == "99999").sum() > 10_000


def test_ish_lite_one_site():
    dates = pd.date_range("2020-09-01", "2020-09-02")
    site = "72224400358"  # "College Park AP"

    df = ish_lite.add_data(dates, site=site)

    assert (df.nunique()[["usaf", "wban"]] == 1).all(), "one site"
    assert (df.usaf + df.wban).iloc[0] == site, "correct site"
    assert (df.time.diff().dropna() == pd.Timedelta("1H")).all(), "hourly data"
    assert len(df) == 25, "includes hour 0 on second day"

    assert {
        "usaf",
        "wban",
        "latitude",
        "longitude",
        "country",
        "state",
    } < set(df.columns), "useful site metadata"
    assert {
        "time",
        "temp",
        "dew_pt_temp",
        "press",
        "wdir",
        "ws",
        "sky_condition",
        "precip_1hr",
        "precip_6hr",
    } < set(df.columns), "data columns"
    assert (df.temp < 100).all(), "temp in degC"


def test_ish_lite_resample():
    dates = pd.date_range("2020-09-01", "2020-09-02")
    site = "72224400358"  # "College Park AP"
    freq = "3H"

    df = ish_lite.add_data(dates, site=site, resample=True, window=freq)

    assert (df.time.diff().dropna() == pd.Timedelta(freq)).all()
    assert len(df) == 8 + 1


@pytest.mark.parametrize("meta", ["country", "state", "site"])
def test_ish_lite_invalid_subset(meta):
    dates = pd.date_range("2020-09-01", "2020-09-02")
    with pytest.raises(ValueError, match="^No data URLs found"):
        _ = ish_lite.add_data(dates, **{meta: "asdf"})


def test_ish_lite_error_on_multiple_subset_options():
    dates = pd.date_range("2020-09-01", "2020-09-02")
    with pytest.raises(ValueError, match="^Only one of "):
        ish_lite.add_data(dates, site="72224400358", state="MD")
