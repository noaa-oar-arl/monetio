import sys

import pandas as pd
import pytest

from monetio import openaq


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires Python 3.7+")
def test_openaq():
    # First date in the archive, just one file
    # Browse the archive at https://openaq-fetches.s3.amazonaws.com/index.html
    dates = pd.date_range(start="2013-11-26", end="2013-11-27", freq="H")[:-1]
    try:
        df = openaq.add_data(dates)
    except PermissionError:
        pytest.skip("private")
    assert not df.empty
    assert df.siteid.nunique() == 1
    assert (df.country == "CN").all() and ((df.time_local - df.time) == pd.Timedelta(hours=8)).all()
