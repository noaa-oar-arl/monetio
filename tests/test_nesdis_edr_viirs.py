import gzip

import numpy as np
import pytest
import xarray as xr

from monetio.readers.nesdis_edr_viirs import NESDISEDRVIIRSReader


@pytest.fixture
def mock_edr_binary(tmp_path):
    """Create a mock gzipped EDR binary file."""
    # High res: 1800 x 3600
    nlat, nlon = 1800, 3600
    # 2 layers
    data = np.random.rand(2, nlat, nlon).astype("<f4")
    # Set some values to -999.9 for masking test
    data[0, 0, 0] = -999.9

    fname = tmp_path / "npp_aot550_edr_gridded_0.10_20230101.high.bin.gz"
    with gzip.open(fname, "wb") as f:
        f.write(data.tobytes())

    return str(fname), data[0, :, :]


def test_nesdis_edr_viirs_eager(mock_edr_binary):
    fname, expected_data = mock_edr_binary
    reader = NESDISEDRVIIRSReader()

    ds = reader.open_dataset(files=fname, lazy=False)

    assert isinstance(ds, xr.Dataset)
    assert "aod_550" in ds.data_vars
    assert ds.aod_550.shape == (1, 1800, 3600)
    assert not hasattr(ds.aod_550.data, "dask")

    # Check masking
    assert np.isnan(ds.aod_550.values[0, 0, 0])
    # Check data (excluding NaN)
    mask = ~np.isnan(ds.aod_550.values[0])
    np.testing.assert_allclose(ds.aod_550.values[0][mask], expected_data[mask])

    # Check coords
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "time" in ds.coords
    assert ds.time.values[0] == np.datetime64("2023-01-01")


def test_nesdis_edr_viirs_lazy(mock_edr_binary):
    fname, expected_data = mock_edr_binary
    reader = NESDISEDRVIIRSReader()

    ds = reader.open_dataset(files=fname, lazy=True)

    assert isinstance(ds, xr.Dataset)
    assert "aod_550" in ds.data_vars
    assert ds.aod_550.shape == (1, 1800, 3600)
    assert hasattr(ds.aod_550.data, "dask")

    # Check that no compute has happened yet for data
    # (Checking a value will trigger compute, so we do it last)

    ds_computed = ds.compute()
    assert np.isnan(ds_computed.aod_550.values[0, 0, 0])
    mask = ~np.isnan(ds_computed.aod_550.values[0])
    np.testing.assert_allclose(ds_computed.aod_550.values[0][mask], expected_data[mask])


def test_nesdis_edr_viirs_consistency(mock_edr_binary):
    fname, _ = mock_edr_binary
    reader = NESDISEDRVIIRSReader()

    ds_eager = reader.open_dataset(files=fname, lazy=False)
    ds_lazy = reader.open_dataset(files=fname, lazy=True).compute()

    xr.testing.assert_allclose(ds_eager, ds_lazy)


def test_build_urls():
    reader = NESDISEDRVIIRSReader()
    urls = reader.build_urls("2023-01-01", resolution="high")
    assert len(urls) == 1
    assert "ftp://ftp.star.nesdis.noaa.gov" in urls[0]
    assert "20230101.high.bin.gz" in urls[0]

    urls_low = reader.build_urls("2023-01-01", resolution="low")
    assert "0.25_20230101.high.bin.gz" in urls_low[0]
