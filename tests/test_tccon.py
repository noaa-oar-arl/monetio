"""
Test TCCON Reader
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.tccon import TCCONReader


def create_mock_tccon_dataset():
    """Create a mock TCCON GGG2020 dataset."""
    time = pd.date_range("2023-01-01", periods=10, freq="h")

    ds = xr.Dataset(
        data_vars={
            "xco2": (("time",), np.random.rand(10)),
            "lat_deg": (("time",), np.ones(10) * 34.1),
            "long_deg": (("time",), np.ones(10) * -118.1),
        },
        coords={
            "time": time,
        },
        attrs={
            "site_name": "pasadena01",
        },
    )
    return ds


def test_tccon_reader_logic(monkeypatch):
    """Test TCCON reader preprocessing logic."""
    mock_ds = create_mock_tccon_dataset()

    def mock_open_mfdataset(*args, **kwargs):
        return mock_ds

    monkeypatch.setattr("xarray.open_mfdataset", mock_open_mfdataset)

    reader = TCCONReader()
    ds = reader.open_dataset(files="dummy.nc")

    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "time" in ds.coords
    assert ds.siteid == "pasadena01"
    assert ds.latitude.values[0] == pytest.approx(34.1)
