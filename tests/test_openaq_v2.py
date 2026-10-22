from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.openaq_v2 import OpenAQV2Reader


@pytest.fixture
def mock_openaq_data():
    """Provides a sample OpenAQ API response."""
    return {
        "meta": {"found": 1},
        "results": [
            {
                "locationId": 1234,
                "location": "Test Site",
                "parameter": "o3",
                "value": 0.05,
                "date": {"utc": "2023-01-01T00:00:00Z", "local": "2023-01-01T00:00:00+00:00"},
                "unit": "ppm",
                "coordinates": {"latitude": 40.0, "longitude": -100.0},
                "country": "US",
                "city": "Test City",
                "isMobile": False,
                "entity": "government",
                "sensorType": "reference grade",
            }
        ],
    }


@patch("monetio.readers.openaq_v2.requests.get")
def test_openaq_v2_eager_lazy_consistency(mock_get, mock_openaq_data):
    """Verifies consistency between eager and lazy backends for OpenAQV2Reader."""
    # Mock the API response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_openaq_data
    mock_get.return_value = mock_response

    reader = OpenAQV2Reader()
    dates = pd.to_datetime(["2023-01-01"])

    # 1. Eager Load (NumPy/Pandas)
    ds_eager = reader.open_dataset(dates=dates, parameters=["o3"], lazy=False, as_xarray=True)

    # Reset mock for next call
    mock_get.reset_mock()

    # 2. Lazy Load (Dask)
    ds_lazy = reader.open_dataset(dates=dates, parameters=["o3"], lazy=True, as_xarray=True)

    # Assertions
    assert isinstance(ds_eager, xr.Dataset)
    assert isinstance(ds_lazy, xr.Dataset)

    # Check that lazy data is indeed dask-backed for core observations
    assert hasattr(ds_lazy["o3_ppm"].data, "dask")

    # Compute lazy result and compare
    ds_lazy_computed = ds_lazy.compute()

    # Compare important variables
    vars_to_compare = ["o3_ppm", "latitude", "longitude", "time"]
    for v in vars_to_compare:
        xr.testing.assert_allclose(ds_eager[v], ds_lazy_computed[v])

    # The public use_dask alias must select the custom delayed-fetch path too.
    ds_use_dask = reader.open_dataset(dates=dates, parameters=["o3"], use_dask=True, as_xarray=True)
    assert hasattr(ds_use_dask["o3_ppm"].data, "dask")
    ds_use_dask_computed = ds_use_dask.compute()
    for v in vars_to_compare:
        xr.testing.assert_allclose(ds_eager[v], ds_use_dask_computed[v])

    table_eager = reader.open_dataset(
        dates=dates, parameters=["o3"], lazy=False, as_xarray=False, wide_fmt=False
    )
    table_use_dask = reader.open_dataset(
        dates=dates, parameters=["o3"], use_dask=True, as_xarray=False, wide_fmt=False
    )
    assert isinstance(table_use_dask, dd.DataFrame)
    pd.testing.assert_frame_equal(table_eager, table_use_dask.compute())

    # Verify variable naming consistency
    assert "o3_ppm" in ds_eager.data_vars
    assert "o3_ppm" in ds_lazy_computed.data_vars
    assert "o3_ppm" in ds_use_dask_computed.data_vars


@patch("monetio.readers.openaq_v2.requests.get")
def test_openaq_v2_empty_response(mock_get):
    """Verifies that the reader handles empty API responses gracefully."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"meta": {"found": 0}, "results": []}
    mock_get.return_value = mock_response

    reader = OpenAQV2Reader()
    dates = pd.to_datetime(["2023-01-01"])

    ds = reader.open_dataset(dates=dates, parameters=["o3"], as_xarray=True)
    assert isinstance(ds, xr.Dataset)
    if "node" in ds.dims:
        assert ds.sizes["node"] == 0


def test_openaq_v2_fetch_query_applies_site_country_parameter_and_dates(monkeypatch):
    from monetio.readers import openaq_v2

    seen = []

    def fake_consume(endpoint, *, params, **kwargs):
        seen.append((endpoint, params))
        return []

    monkeypatch.setattr(openaq_v2, "_consume", fake_consume)
    OpenAQV2Reader()._fetch_data(
        dates=pd.DatetimeIndex(["2023-01-01"]),
        parameters=["o3"],
        country="US",
        sites=[1234],
    )
    assert len(seen) == 1
    endpoint, params = seen[0]
    assert endpoint == openaq_v2._ENDPOINTS["measurements"]
    assert params["parameter"] == "o3"
    assert params["country"] == "US"
    assert params["location_id"] == [1234]
    assert params["date_from"] == pd.Timestamp("2022-12-31 23:59:59")
    assert params["date_to"] == pd.Timestamp("2023-01-01")
