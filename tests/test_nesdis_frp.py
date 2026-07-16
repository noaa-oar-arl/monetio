import numpy as np
import pytest
import xarray as xr

from monetio.readers.nesdis_frp import NESDISFRPReader


@pytest.fixture
def mock_frp_binary(tmp_path):
    """Create a mock FRP binary file."""
    res = "C384"
    r = int(res[1:])
    # tile 1
    data = np.random.rand(r, r).astype("f4")

    fname = tmp_path / "meanFRP.20230101.FV3.C384Grid.tile1.bin"
    # We will just write a raw binary file for the fallback path
    with open(fname, "wb") as f:
        f.write(data.flatten(order="F").tobytes())

    return str(fname), data


def test_nesdis_frp_eager(mock_frp_binary):
    fname, expected_data = mock_frp_binary
    reader = NESDISFRPReader()

    ds = reader.open_dataset(files=fname, lazy=False, ftype="meanFRP")

    assert isinstance(ds, xr.Dataset)
    assert "meanFRP" in ds.data_vars
    # For a single file, it might be (time, x, y) if not concatenated.
    # Check shape:
    print(f"DEBUG: {ds.meanFRP.dims} {ds.meanFRP.shape}")
    if "tile" in ds.dims:
        assert ds.meanFRP.shape == (1, 1, 384, 384)
        np.testing.assert_allclose(ds.meanFRP.values[0, 0], expected_data)
    else:
        assert ds.meanFRP.shape == (1, 384, 384)
        np.testing.assert_allclose(ds.meanFRP.values[0], expected_data)

    assert "tile" in ds.coords
    # If it's a scalar coord
    if ds.tile.ndim == 0:
        assert ds.tile.values == 1
    else:
        assert ds.tile.values[0] == 1
    assert "time" in ds.coords
    assert ds.time.values[0] == np.datetime64("2023-01-01")


def test_nesdis_frp_lazy(mock_frp_binary):
    fname, expected_data = mock_frp_binary
    reader = NESDISFRPReader()

    ds = reader.open_dataset(files=fname, lazy=True, ftype="meanFRP")

    assert isinstance(ds, xr.Dataset)
    assert "meanFRP" in ds.data_vars
    assert hasattr(ds.meanFRP.data, "dask")

    ds_computed = ds.compute()
    if "tile" in ds_computed.dims:
        np.testing.assert_allclose(ds_computed.meanFRP.values[0, 0], expected_data)
    else:
        np.testing.assert_allclose(ds_computed.meanFRP.values[0], expected_data)


def test_nesdis_frp_consistency(mock_frp_binary):
    fname, _ = mock_frp_binary
    reader = NESDISFRPReader()

    ds_eager = reader.open_dataset(files=fname, lazy=False, ftype="meanFRP")
    ds_lazy = reader.open_dataset(files=fname, lazy=True, ftype="meanFRP").compute()

    xr.testing.assert_allclose(ds_eager, ds_lazy)


def test_frp_build_urls():
    reader = NESDISFRPReader()
    urls = reader.build_urls("2023-01-01", ftype="meanFRP")
    assert len(urls) == 6
    assert "meanFRP.20230101.FV3C384Grid.tile1.bin" in urls[0]
