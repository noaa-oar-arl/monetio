import os

import pandas as pd
import pytest

import monetio.obs.openaq_v3 as openaq

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


def columns_all_snake_case(df):
    return all(df.columns.str.fullmatch(r"[a-z_]+"))


def test_get_parameters():
    params = openaq.get_parameters()
    assert columns_all_snake_case(params)
    assert 20 <= len(params) <= 100
    assert params.id.nunique() == len(params)
    assert params.name.nunique() < len(params), "dupes for different units etc."
    assert "pm25" in params.name.values
    assert "o3" in params.name.values


def test_get_locations():
    sites = openaq.get_locations()
    assert columns_all_snake_case(sites)
    assert 10_000 <= len(sites) < 50_000
    assert sites.siteid.nunique() == len(sites)
    assert sites.dtypes["first_time"] == "datetime64[ns]"
    assert sites.dtypes["last_time"] == "datetime64[ns]"
    assert sites.dtypes["latitude"] == "float64"
    assert sites.dtypes["longitude"] == "float64"
    assert sites["latitude"].isnull().sum() == 0
    assert sites["longitude"].isnull().sum() == 0

    # Check that we didn't end up with unexpected non-scalar columns
    for col in sites.columns:
        is_scalar = sites[col].apply(lambda x: pd.api.types.is_scalar(x)).all()
        assert is_scalar or col in {"parameters", "parameter_ids", "sensor_ids"}

    # Check that pm25 and o3 are in the unique parameters
    unique_params = set(sites["parameters"].sum())
    assert {"pm25", "o3"} <= unique_params


def test_get_sensors():
    df = openaq.get_sensors("3832")
    assert columns_all_snake_case(df)
    assert df.parameter.tolist() == ["so2", "o3"]


@pytest.mark.parametrize(
    "product",
    [
        "raw",
        "hourly",
        "daily",
    ],
)
def test_add_data_sensor_ids(product):
    df = openaq.get_sensors("2978434")
    sensor_ids = df["id"].tolist()
    assert len(sensor_ids) == 1
    df = openaq.add_data(
        ["2024-08-01", "2024-08-08"],
        raw=product == "raw",
        hourly=product == "hourly",
        daily=product == "daily",
        query_time_split="1D",
        sensor_ids=sensor_ids,
        threads=2,
    )
    assert columns_all_snake_case(df)
    assert len(df) > 0
    assert df.sensor_id.nunique() == 1

    tmin, tmax = df.time.min(), df.time.max()
    if product == "raw":
        assert tmin >= pd.Timestamp("2024-08-01")
        assert tmax <= pd.Timestamp("2024-08-08")
    elif product in {"hourly", "daily"}:
        assert tmin == pd.Timestamp("2024-08-01")
        assert tmax == pd.Timestamp("2024-08-08")
    else:
        raise AssertionError


@pytest.mark.parametrize(
    "product",
    [
        "raw",
        "hourly",
        "daily",
    ],
)
def test_add_data_sensor_limit(product):
    df = openaq.add_data(
        ["2019-08-01", "2019-08-02"],
        parameters="pm25",
        raw=product == "raw",
        hourly=product == "hourly",
        daily=product == "daily",
        query_time_split=None,
        sensor_limit=10,
        threads=2,
    )
    assert columns_all_snake_case(df)
    assert len(df) > 0
    assert df.sensor_id.nunique() <= 10

    tmin, tmax = df.time.min(), df.time.max()
    if product == "raw":
        assert tmin >= pd.Timestamp("2019-08-01")
        assert tmax <= pd.Timestamp("2019-08-02")
    elif product in {"hourly", "daily"}:
        assert tmin == pd.Timestamp("2019-08-01")
        assert tmax == pd.Timestamp("2019-08-02")
    else:
        raise AssertionError

    df_wide = openaq._to_wide_fmt(df)
    assert df_wide.pm25_ugm3.mean() == pytest.approx(
        df.query("parameter == 'pm25'").value.mean(),
        rel=1e-15,
        abs=0,
    )


def test_get_data_single_dt_single_site():
    site = "843"
    dates = "2023-08-01"
    df = openaq.add_data(dates, parameters="o3", sites=site)
    assert len(df) == 1
    assert df.time.iloc[0] == pd.Timestamp("2023-08-01")
