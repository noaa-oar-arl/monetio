"""
Test Sentinel-4 Reader
"""

import numpy as np
import pytest
import xarray as xr

from monetio.readers.sentinel4 import Sentinel4Reader


def create_mock_sentinel4_dataset(is_dask=False):
    """Create a mock Sentinel-4 L2 dataset."""
    scanline = 10
    ground_pixel = 20

    ds = xr.Dataset(
        data_vars={
            "qa_value": (
                ("scanline", "ground_pixel"),
                np.linspace(0, 1, scanline * ground_pixel).reshape(scanline, ground_pixel),
            ),
            "nitrogendioxide_tropospheric_column": (
                ("scanline", "ground_pixel"),
                np.random.rand(scanline, ground_pixel),
            ),
            "latitude": (
                ("scanline", "ground_pixel"),
                np.linspace(40, 50, scanline)[:, None] * np.ones((1, ground_pixel)),
            ),
            "longitude": (
                ("scanline", "ground_pixel"),
                np.ones((scanline, 1)) * np.linspace(-10, 10, ground_pixel)[None, :],
            ),
        }
    )

    if is_dask:
        ds = ds.chunk({"scanline": 5, "ground_pixel": 10})

    return ds


@pytest.mark.parametrize("lazy", [False, True])
def test_sentinel4_reader_logic(lazy, monkeypatch):
    """Test Sentinel-4 reader preprocessing logic."""
    mock_ds = create_mock_sentinel4_dataset(is_dask=lazy)

    def mock_open(*args, **kwargs):
        return mock_ds

    monkeypatch.setattr("monetio.readers.drivers.XarrayDriver.open", mock_open)

    reader = Sentinel4Reader()
    # Test with qa_threshold
    ds = reader.open_dataset(files="dummy.nc", qa_threshold=0.5)

    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert ds.dims == {"y": 10, "x": 20}

    # Check masking
    # qa_value should be preserved (unmasked)
    assert not np.isnan(ds.qa_value.values).any()
    # data variable should be masked
    assert np.isnan(ds.nitrogendioxide_tropospheric_column.values).any()

    if lazy:
        assert ds.nitrogendioxide_tropospheric_column.chunks is not None
    else:
        assert ds.nitrogendioxide_tropospheric_column.chunks is None
