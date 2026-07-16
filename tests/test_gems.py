"""
Test GEMS Reader
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.gems import GEMSReader


def create_mock_gems_dataset(is_dask=False):
    """Create a mock GEMS L2 NetCDF dataset."""
    nscans = 10
    npixels = 20

    ds = xr.Dataset(
        data_vars={
            "ColumnAmountNO2": (("nscans", "npixels"), np.random.rand(nscans, npixels)),
            "Latitude": (
                ("nscans", "npixels"),
                np.linspace(30, 40, nscans)[:, None] * np.ones((1, npixels)),
            ),
            "Longitude": (
                ("nscans", "npixels"),
                np.ones((nscans, 1)) * np.linspace(120, 130, npixels)[None, :],
            ),
            "Time": (("nscans",), pd.date_range("2023-01-01", periods=nscans, freq="h")),
        },
        attrs={
            "history": "Created mock GEMS data",
        },
    )

    if is_dask:
        ds = ds.chunk({"nscans": 5, "npixels": 10})

    return ds


@pytest.mark.parametrize("lazy", [False, True])
def test_gems_reader_logic(lazy, monkeypatch):
    """Test GEMS reader preprocessing logic."""
    mock_ds = create_mock_gems_dataset(is_dask=lazy)

    # Mock XarrayDriver.open to return our mock dataset
    def mock_open(*args, **kwargs):
        # Handle group if present
        return mock_ds

    monkeypatch.setattr("monetio.readers.drivers.XarrayDriver.open", mock_open)

    reader = GEMSReader()
    # We call with a dummy file path
    ds = reader.open_dataset(files="dummy.nc")

    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "time" in ds.coords
    assert ds.dims == {"y": 10, "x": 20} or ds.dims == {
        "time": 10,
        "x": 20,
    }  # Depending on swap_dims which we didn't implement yet but sat_utils might

    if lazy:
        assert ds.ColumnAmountNO2.chunks is not None
    else:
        assert ds.ColumnAmountNO2.chunks is None

    assert "history" in ds.attrs
    assert "GEMS" in ds.attrs["history"]
