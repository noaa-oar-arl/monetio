import pandas as pd

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


def test_ish_lite_one_site():
    dates = pd.date_range("2020-09-01", "2020-09-02")
    df = ish_lite.add_data(dates, site="72224400358")  # "College Park AP"
    assert len(df) == 25, "includes hour 0 on second day"
    assert {"usaf", "wban", "latitude", "longitude", "state"} < set(
        df.columns
    ), "useful site metadata"


# TODO: invalid site
