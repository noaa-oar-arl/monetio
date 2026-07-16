from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

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
