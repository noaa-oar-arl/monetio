import pandas as pd
import pytest

from monetio import aqs


def test_aqs():
    # For MM data proc example
    dates = pd.date_range(start="2019-08-01", end="2019-08-31", freq="H")
    # Note: will retrieve full year
    network = "NCORE"  # CSN NCORE CASTNET
    with pytest.warns(UserWarning, match="Short names not available for these variables"):
        df = aqs.add_data(dates, param=["PM10SPEC"], network=network, wide_fmt=False, daily=True)
    assert (df.variable == "").sum() == 0


def test_aqs_daily_wide():
    dates = pd.date_range(start="2019-08-01", end="2019-08-31", freq="D")
    _ = aqs.add_data(
        dates,
        param=["O3", "PM2.5"],
        network="IMPROVE",
        wide_fmt=True,
        daily=True,
    )
