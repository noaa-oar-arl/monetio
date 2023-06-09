import pandas as pd

from monetio import openaq


def test_openaq():
    # First date in the archive, just one file
    # Browse the archive at https://openaq-fetches.s3.amazonaws.com/index.html
    dates = pd.date_range(start="2013-11-26", end="2013-11-27", freq="H")[:-1]
    df = openaq.add_data(dates)
    assert not df.empty
    assert df.siteid.nunique() == 1
    assert (df.country == "CN").all() and ((df.time_local - df.time) == pd.Timedelta(hours=8)).all()
