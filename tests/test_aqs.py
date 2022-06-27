import pandas as pd

from monetio import aqs


def test_aqs():
    # For MM data proc example
    dates = pd.date_range(start="2019-08-01", end="2019-08-31", freq="H")
    network = "NCORE"  # CSN NCORE CASTNET
    aqs.add_data(dates, param=["PM10SPEC", "SPEC"], network=network, wide_fmt=False, daily=True)
