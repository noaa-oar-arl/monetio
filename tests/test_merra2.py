import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.merra2 import MERRA2Reader


@pytest.fixture
def mock_merra2_dataset():
    """Create a mock MERRA2 dataset for testing."""
    lat = np.linspace(-90, 90, 10)
    lon = np.linspace(-180, 180, 20)
    lev = np.arange(1, 73)
    time = pd.date_range("2023-01-01", periods=1)

    ds = xr.Dataset(
        coords={
            "lat": (["lat"], lat),
            "lon": (["lon"], lon),
            "lev": (["lev"], lev),
            "time": (["time"], time),
        }
    )

    # Add ak, bk coefficients (standard for MERRA-2)
    ds["ak"] = (["lev"], np.linspace(0, 1000, 72))
    ds["bk"] = (["lev"], np.linspace(1, 0, 72))

    # Add surface pressure and temperature
    shape_sfc = (len(time), len(lat), len(lon))
    ds["PS"] = (["time", "lat", "lon"], np.full(shape_sfc, 101325.0))
    ds["T"] = (
        ["time", "lev", "lat", "lon"],
        np.random.rand(len(time), len(lev), len(lat), len(lon)) + 273.15,
    )

    # Set attributes
    ds.lat.attrs = {"units": "degrees_north", "long_name": "latitude"}
    ds.lon.attrs = {"units": "degrees_east", "long_name": "longitude"}
    ds.lev.attrs = {"units": "level", "long_name": "vertical_level"}
    ds.PS.attrs = {"units": "Pa", "long_name": "surface_pressure"}
    ds.T.attrs = {"units": "K", "long_name": "temperature"}

    return ds


def test_merra2_reader_basic(mock_merra2_dataset, tmp_path):
    """Test MERRA2Reader with a mock dataset."""
    test_file = tmp_path / "test_merra2.nc"
    mock_merra2_dataset.to_netcdf(test_file)

    reader = MERRA2Reader()
    ds = reader.open_dataset(files=str(test_file))

    # Check standardization
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert ds.latitude.ndim == 2
    assert ds.longitude.ndim == 2
    assert "y" in ds.dims
    assert "x" in ds.dims
    assert "z" in ds.dims

    # Check variable renaming
    assert "surface_pressure" in ds.variables
    assert "temperature" in ds.variables

    # Check pressure calculation
    assert "pres_pa_mid" in ds.variables
    assert ds.pres_pa_mid.attrs["units"] == "Pa"

    # Verify calculation: p = ak + bk * ps
    # For a specific point
    expected_p = (
        mock_merra2_dataset.ak.values[0]
        + mock_merra2_dataset.bk.values[0] * mock_merra2_dataset.PS.values[0, 0, 0]
    )
    # In processed ds, z dimension is levelled (usually 1 is top, 72 is bottom in MERRA2, but check sorting)
    # standardize_satellite_coords might not flip z automatically unless specified.
    # Our reader just uses z_dim=["lev", ...].

    # We can check the values directly
    calculated_p = ds.pres_pa_mid.isel(time=0, z=0, y=0, x=0).values
    # In mock, ak[0]=0, bk[0]=1, PS=101325 => expected_p = 101325
    assert np.allclose(calculated_p, expected_p)


def test_merra2_lazy_loading(mock_merra2_dataset, tmp_path):
    """Test MERRA2Reader with lazy loading (Dask)."""
    test_file = tmp_path / "test_merra2_lazy.nc"
    mock_merra2_dataset.to_netcdf(test_file)

    reader = MERRA2Reader()
    # XarrayDriver should use dask if chunks are provided or lazy=True
    ds = reader.open_dataset(files=str(test_file), chunks={"time": 1})

    assert ds.pres_pa_mid.chunks is not None
    assert "history" in ds.attrs
    assert "Preprocessed MERRA-2 data using standardized preprocessing." in ds.attrs["history"]


def test_merra2_build_urls():
    """Test URL building logic for MERRA-2."""
    reader = MERRA2Reader()
    urls = reader.build_urls("2024-01-01", product="inst1_2d_asm_Nx")
    assert len(urls) == 1
    assert "M2I1NXASM.5.12.4" in urls[0]
    assert "MERRA2_400.inst1_2d_asm_Nx.20240101.nc4" in urls[0]

    urls_old = reader.build_urls("1990-01-01", product="inst1_2d_asm_Nx")
    assert "MERRA2_100" in urls_old[0]

    with pytest.raises(ValueError, match="Unknown product"):
        reader.build_urls("2024-01-01", product="invalid_product")


def test_merra2_open_dataset_with_dates(monkeypatch):
    """Test open_dataset with dates instead of files."""
    reader = MERRA2Reader()

    def mock_open(self, files, **kwargs):
        return xr.Dataset(attrs={"files_opened": files})

    # Mock the XarrayDriver.open (or GriddedReader.open_dataset indirectly)
    # Actually it's easier to mock the super().open_dataset or the build_urls
    monkeypatch.setattr("monetio.readers.base.GriddedReader.open_dataset", mock_open)

    ds = reader.open_dataset(dates="2024-01-01", product="inst1_2d_asm_Nx")
    assert "files_opened" in ds.attrs
    assert "MERRA2_400.inst1_2d_asm_Nx.20240101.nc4" in ds.attrs["files_opened"][0]
