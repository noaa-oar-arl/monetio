import ssl

import pandas as pd
import pytest

from monetio import aqs

ssl_version = ssl.OPENSSL_VERSION_INFO


@pytest.mark.xfail(ssl_version > (1,), strict=True, reason="Doesn't work with newer OpenSSL")
def test_aqs():
    # For MM data proc example
    dates = pd.date_range(start="2019-08-01", end="2019-08-31", freq="H")
    # Note: will retrieve full year
    network = "NCORE"  # CSN NCORE CASTNET
    with pytest.warns(UserWarning, match="Short names not available for these variables"):
        df = aqs.add_data(dates, param=["PM10SPEC"], network=network, wide_fmt=False, daily=True)
    assert (df.variable == "").sum() == 0
