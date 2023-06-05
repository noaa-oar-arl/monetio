import ssl
import warnings

import pandas as pd
import pytest

from monetio import aqs

try:
    ssl_version = tuple(int(x) for x in ssl.OPENSSL_VERSION.split()[1].split("."))
except Exception:
    warnings.warn(
        "Could not determine OpenSSL version, assuming recent. "
        f"ssl.OPENSSL_VERSION: {ssl.OPENSSL_VERSION!r}",
        category=UserWarning,
    )
    ssl_version = (100,)


@pytest.mark.xfail(ssl_version > (1,), strict=True, reason="Doesn't work with newer OpenSSL")
def test_aqs():
    # For MM data proc example
    dates = pd.date_range(start="2019-08-01", end="2019-08-31", freq="H")
    # Note: will retrieve full year
    network = "NCORE"  # CSN NCORE CASTNET
    with pytest.warns(UserWarning, match="Short names not available for these variables"):
        df = aqs.add_data(dates, param=["PM10SPEC"], network=network, wide_fmt=False, daily=True)
    assert (df.variable == "").sum() == 0
