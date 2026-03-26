import os
from contextlib import contextmanager

import pandas as pd
import pytest
import requests

import monetio.obs.openaq_v2 as openaq

if (
    os.environ.get("CI", "false").lower() not in {"false", "0"}
    and os.environ.get("OPENAQ_API_KEY", "") == ""
):
    # PRs from forks don't get the secret
    pytest.skip("no API key", allow_module_level=True)

LATLON_NCWCP = 38.9721, -76.9248
SITES_NEAR_NCWCP = [
    # AirGradient monitor
    1236068,
    1719392,
    # # PurpleAir sensors
    # 1118827,
    # 357301,
    # 273440,
    # 271155,
    # NASA GSFC
    2978434,
    # Beltsville (AirNow)
    3832,
    843,
]

xfail_httperror = pytest.mark.xfail(
    raises=requests.HTTPError,
    reason="API v2 is gone",
    strict=True,
)


@contextmanager
def check_error_code():
    should_raise = 410
    try:
        yield
    except requests.HTTPError as e:
        assert e.response.status_code == should_raise
        raise
    except Exception as e:
        raise AssertionError(f"expected HTTP Error {should_raise}") from e


@xfail_httperror
def test_get_parameters():
    with check_error_code():
        params = openaq.get_parameters()
    assert 50 <= len(params) <= 500
    assert params.id.nunique() == len(params)
    assert params.name.nunique() < len(params), "dupes for different units etc."
    assert "pm25" in params.name.values
    assert "o3" in params.name.values


@xfail_httperror
def test_get_locations():
    with check_error_code():
        sites = openaq.get_locations(npages=2, limit=100)
    assert len(sites) <= 200
    assert sites.siteid.nunique() == len(sites)
    assert sites.dtypes["firstUpdated"] == "datetime64[ns]"
    assert sites.dtypes["lastUpdated"] == "datetime64[ns]"
    assert sites.dtypes["latitude"] == "float64"
    assert sites.dtypes["longitude"] == "float64"
    assert sites["latitude"].isnull().sum() == 0
    assert sites["longitude"].isnull().sum() == 0


@xfail_httperror
def test_get_data_near_ncwcp_sites():
    sites = SITES_NEAR_NCWCP
    dates = pd.date_range("2023-08-01", "2023-08-01 01:00", freq="1h")
    with check_error_code():
        df = openaq.add_data(dates, sites=sites)
    assert len(df) > 0
    assert "pm25" in df.parameter.values
    assert df.latitude.round().eq(39).all()
    assert df.longitude.round().eq(-77).all()
    assert (sorted(df.time.unique()) == dates).all()
    assert set(df.siteid) <= {str(site) for site in sites}
    assert not df.value.isna().all() and not df.value.lt(0).any()


@xfail_httperror
def test_get_data_near_ncwcp_sites_wide():
    sites = SITES_NEAR_NCWCP
    dates = pd.date_range("2023-08-01", "2023-08-01 01:00", freq="1h")

    # with pytest.warns(UserWarning, match=r"dropping '.*' from index for wide fmt \(all null\)"):
    with check_error_code():
        df = openaq.add_data(dates, sites=sites, wide_fmt=True)
    assert len(df) > 0
    assert {"pm25_ugm3", "o3_ppm"} <= set(df.columns)
    assert not {"parameter", "value", "unit"} <= set(df.columns)


@xfail_httperror
def test_get_data_near_ncwcp_search_radius():
    latlon = LATLON_NCWCP
    dates = pd.date_range("2023-08-01", "2023-08-01 01:00", freq="1h")
    with check_error_code():
        df = openaq.add_data(dates, search_radius={latlon: 10_000}, threads=2)
    assert len(df) > 0
    assert "pm25" in df.parameter.values
    assert df.latitude.round().eq(39).all()
    assert df.longitude.round().eq(-77).all()
    assert (sorted(df.time.unique()) == dates).all()
    assert not df.sensor_type.eq("low-cost sensor").all()
    assert df.entity.eq("Governmental Organization").all()


@xfail_httperror
def test_get_data_near_ncwcp_sensor_type():
    latlon = LATLON_NCWCP
    dates = pd.date_range("2023-08-01", "2023-08-01 03:00", freq="1h")
    with check_error_code():
        df = openaq.add_data(dates, sensor_type="low-cost sensor", search_radius={latlon: 25_000})
    assert len(df) > 0
    assert df.sensor_type.eq("low-cost sensor").all()


@xfail_httperror
def test_get_data_single_dt_single_site():
    site = 843
    dates = "2023-08-01"
    with check_error_code():
        df = openaq.add_data(dates, parameters="o3", sites=site)
    assert len(df) == 1


@xfail_httperror
@pytest.mark.parametrize(
    "entity",
    [
        "research",
        "community",
        ["research", "community"],
    ],
)
def test_get_data_near_ncwcp_entity(entity):
    latlon = LATLON_NCWCP
    dates = pd.date_range("2023-08-01", "2023-08-01 01:00", freq="1h")
    with check_error_code():
        df = openaq.add_data(dates, entity=entity, search_radius={latlon: 25_000})
    assert df.empty


@pytest.mark.parametrize(
    "radius",
    [
        0,
        -1,
        25001,
    ],
)
def test_get_data_bad_radius(radius):
    with pytest.raises(ValueError, match="invalid radius"):
        openaq.add_data(["2023-08-01", "2023-08-02"], search_radius={LATLON_NCWCP: radius})
