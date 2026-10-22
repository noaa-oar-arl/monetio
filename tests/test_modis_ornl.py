from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from monetio.readers.errors import OptionUnsupportedError
from monetio.readers.modis_ornl import MODISORNLReader, _get_latlon, _make_xarray_dataset


def test_modis_ornl_latlon_lazy():
    """Verify that _get_latlon is lazy and matches expected values."""
    xll, yll = 0, 0
    cell_width = 1000
    nx, ny = 10, 8

    # 1. Generate coordinates
    lon, lat = _get_latlon(xll, yll, cell_width, nx, ny)

    # Check dimensions
    assert lon.dims == ("y", "x")
    assert lat.dims == ("y", "x")
    assert lon.shape == (8, 10)

    # Check values (one point)
    # Sinusoidal projection at (0,0) should be (0,0)
    assert not np.isnan(lon.values).all()
    assert not np.isnan(lat.values).all()

    # Verify it can be chunked
    lon_lazy = lon.chunk({"x": 5, "y": 4})
    assert hasattr(lon_lazy.data, "dask")


def test_modis_ornl_rejects_use_dask_before_remote_retrieval(monkeypatch):
    reader = MODISORNLReader()

    def unexpected_retrieval(*args, **kwargs):
        raise AssertionError("use_dask must be rejected before the SOAP request")

    monkeypatch.setattr("monetio.readers.modis_ornl._get_single_retrieval", unexpected_retrieval)
    with pytest.raises(OptionUnsupportedError, match="use_dask"):
        reader.open_dataset(date="2020-01-01", use_dask=True)


def test_modis_ornl_rejects_chunks_before_remote_retrieval(monkeypatch):
    reader = MODISORNLReader()

    def unexpected_retrieval(*args, **kwargs):
        raise AssertionError("chunks must be rejected before the SOAP request")

    monkeypatch.setattr("monetio.readers.modis_ornl._get_single_retrieval", unexpected_retrieval)
    with pytest.raises(OptionUnsupportedError, match="modis_ornl.*chunks"):
        reader.open_dataset(date="2020-01-01", chunks={"time": 1})


def test_modis_ornl_reader_eager_output_without_network(monkeypatch):
    from monetio.readers import modis_ornl

    metadata = {
        "band": "Lai_500m",
        "date_int": 2020001,
        "xllcorner": 0,
        "yllcorner": 0,
        "cellsize": 1000,
        "ncols": 3,
        "nrows": 2,
        "units": "m",
        "product": "MOD15A2H",
        "server": "test_server",
    }
    monkeypatch.setattr(modis_ornl, "HAS_SUDS", True)
    monkeypatch.setattr(
        modis_ornl,
        "_get_single_retrieval",
        lambda *args, **kwargs: _make_xarray_dataset(np.arange(6).reshape(2, 3), metadata),
    )

    ds = MODISORNLReader().open_dataset(date="2020-01-01", product="MOD15A2H")
    assert isinstance(ds, xr.Dataset)
    assert isinstance(ds["Lai_500m"].data, np.ndarray)
    np.testing.assert_array_equal(ds["Lai_500m"].values, np.arange(6).reshape(1, 2, 3))


def test_modis_ornl_forwards_remote_spatial_and_band_selection(monkeypatch):
    from monetio.readers import modis_ornl

    metadata = {
        "band": "Lai_500m",
        "date_int": 2020001,
        "xllcorner": 0,
        "yllcorner": 0,
        "cellsize": 1000,
        "ncols": 3,
        "nrows": 2,
        "units": "m",
        "product": "MOD15A2H",
        "server": "test_server",
    }
    captured = {}

    def retrieve(date, **kwargs):
        captured.update(kwargs)
        return modis_ornl._make_xarray_dataset(np.arange(6).reshape(2, 3), metadata)

    monkeypatch.setattr(modis_ornl, "HAS_SUDS", True)
    monkeypatch.setattr(modis_ornl, "_get_single_retrieval", retrieve)
    ds = MODISORNLReader().open_dataset(
        date="2020-01-01",
        product="MOD15A2H",
        band="Lai_500m",
        latitude=40.0,
        longitude=-105.0,
        kmAboveBelow=25,
        kmLeftRight=30,
    )

    assert captured == {
        "product": "MOD15A2H",
        "band": "Lai_500m",
        "quality_control": None,
        "lat": 40.0,
        "lon": -105.0,
        "kmAboveBelow": 25,
        "kmLeftRight": 30,
    }
    assert "Lai_500m" in ds


def test_make_xarray_dataset_eager_lazy():
    """Verify _make_xarray_dataset works with eager and lazy data."""
    metadata = {
        "band": "test_band",
        "date_int": 2020001,
        "xllcorner": 0,
        "yllcorner": 0,
        "cellsize": 1000,
        "ncols": 10,
        "nrows": 8,
        "units": "m",
        "product": "MOD15A2H",
        "server": "test_server",
    }
    grid_data = np.random.rand(8, 10)

    # Eager
    ds_eager = _make_xarray_dataset(grid_data, metadata)
    assert isinstance(ds_eager["test_band"].data, np.ndarray)
    assert ds_eager.test_band.shape == (1, 8, 10)
    assert "history" in ds_eager.attrs

    # Lazy
    try:
        import dask.array as da

        grid_lazy = da.from_array(grid_data, chunks=(4, 5))
        ds_lazy = _make_xarray_dataset(grid_lazy, metadata)
        assert hasattr(ds_lazy["test_band"].data, "dask")

        # Verify results match
        xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    except ImportError:
        pytest.skip("Dask not installed")


@pytest.mark.skipif(
    not pytest.importorskip("monetio.readers.modis_ornl").HAS_SUDS, reason="suds not installed"
)
@patch("monetio.readers.modis_ornl.Client", create=True)
def test_modis_ornl_reader_mock(mock_client):
    """Test MODISORNLReader with mocked SOAP service."""
    # Setup mock
    instance = mock_client.return_value
    instance.service.getdates.return_value = ["A2020001"]

    mock_subset = MagicMock()
    mock_subset.nrows = 8
    mock_subset.ncols = 10
    mock_subset.cellsize = 1000
    mock_subset.scale = 0.1
    mock_subset.units = "m"
    mock_subset.yllcorner = 0
    mock_subset.xllcorner = 0
    # Data is comma-separated string, first 5 elements are metadata we skip
    mock_subset.subset = ["0,0,0,0,0," + ",".join(["1.0"] * 80)]

    instance.service.getsubset.return_value = mock_subset

    reader = MODISORNLReader()
    ds = reader.open_dataset(date="2020-01-01", product="MOD15A2H", band="Lai_500m")

    assert isinstance(ds, xr.Dataset)
    assert "Lai_500m" in ds.data_vars
    assert ds.Lai_500m.shape == (1, 8, 10)
    # Check scaling (1.0 * 0.1 = 0.1)
    assert np.allclose(ds.Lai_500m.values, 0.1)
    assert "history" in ds.attrs
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
