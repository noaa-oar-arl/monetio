"""
Test EarthCARE Reader
"""

import numpy as np
import pytest
import xarray as xr

from monetio.readers.earthcare import EarthCAREReader


def create_mock_earthcare_dataset(is_dask=False):
    """Create a mock EarthCARE L2 aerosol profile dataset."""
    profile = 10
    n_range = 30

    ds = xr.Dataset(
        data_vars={
            "aerosol_extinction": (("profile", "n_range"), np.random.rand(profile, n_range)),
            "latitude": (("profile",), np.linspace(-90, 90, profile)),
            "longitude": (("profile",), np.linspace(-180, 180, profile)),
            "UTC_time": (("profile",), np.linspace(0, 6000, profile)),
        }
    )

    if is_dask:
        ds = ds.chunk({"profile": 5, "n_range": 15})

    return ds


@pytest.mark.parametrize("lazy", [False, True])
def test_earthcare_reader_logic(lazy, monkeypatch):
    """Test EarthCARE reader preprocessing logic."""
    mock_ds = create_mock_earthcare_dataset(is_dask=lazy)

    def mock_open(*args, **kwargs):
        return mock_ds

    monkeypatch.setattr("monetio.readers.drivers.XarrayDriver.open", mock_open)

    reader = EarthCAREReader()
    ds = reader.open_dataset(files="dummy.nc")

    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "time" in ds.coords
    assert ds.dims == {"y": 10, "z": 30}

    if lazy:
        assert ds.aerosol_extinction.chunks is not None
