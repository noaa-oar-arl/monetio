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
    df = ish_lite.add_data(dates, site="72224400358")  # "College Park AP"
    assert len(df) == 25, "includes hour 0 on second day"
    assert {"usaf", "wban", "latitude", "longitude", "state"} < set(
        df.columns
    ), "useful site metadata"


# TODO: invalid site


def test_ish_lite_error_on_multiple_subset_options():
    dates = pd.date_range("2020-09-01", "2020-09-02")
    with pytest.raises(ValueError, match="^Only one of "):
        ish_lite.add_data(dates, site="72224400358", state="MD")
