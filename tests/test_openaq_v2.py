import pandas as pd
import pytest

import monetio.obs.openaq_v2 as openaq

SITES_NEAR_NCWCP = [
    # AirGradient monitor
    1236068,
    # PurpleAir sensors
    1118827,
    357301,
    273440,
    271155,
]


def test_get_parameters():
    params = openaq.get_parameters()
    assert 50 <= len(params) <= 500
    assert params.id.nunique() == len(params)
    assert params.name.nunique() < len(params), "dupes for different units etc."
    assert "pm25" in params.name.values
    assert "o3" in params.name.values


def test_get_locations():
    sites = openaq.get_locations(npages=2, limit=100)
    assert len(sites) <= 200
    assert sites.siteid.nunique() == len(sites)
    assert sites.dtypes["firstUpdated"] == "datetime64[ns]"
    assert sites.dtypes["lastUpdated"] == "datetime64[ns]"
    assert sites.dtypes["latitude"] == "float64"
    assert sites.dtypes["longitude"] == "float64"
    assert sites["latitude"].isnull().sum() == 0
    assert sites["longitude"].isnull().sum() == 0


def test_get_data_near_ncwcp_sites():
    sites = SITES_NEAR_NCWCP
    dates = pd.date_range("2023-08-01", "2023-08-01 01:00", freq="1H")
    df = openaq.add_data(dates, sites=sites)
    assert len(df) > 0
    assert "pm25" in df.parameter.values
    assert df.latitude.round().eq(39).all()
    assert df.longitude.round().eq(-77).all()
    assert (sorted(df.time.unique()) == dates).all()
    assert set(df.siteid) == {str(site) for site in sites}


def test_get_data_near_ncwcp_search_radius():
    latlon = 38.9721, -76.9248
    dates = pd.date_range("2023-08-01", "2023-08-01 01:00", freq="1H")
    df = openaq.add_data(dates, search_radius={latlon: 5_000})
    assert len(df) > 0
    assert "pm25" in df.parameter.values
    assert df.latitude.round().eq(39).all()
    assert df.longitude.round().eq(-77).all()
    assert (sorted(df.time.unique()) == dates).all()


def test_get_data_wide_error():
    with pytest.raises(NotImplementedError, match="wide format not implemented"):
        openaq.add_data(["2023-08-01", "2023-08-02"], wide_fmt=True)
