import pandas as pd
import pytest
import xarray as xr

from monetio.readers.skynet import SKYNETReader


@pytest.fixture
def mock_skynet_file(tmp_path):
    fn = tmp_path / "test_site_20230101.AOT"
    content = """# site: TEST_SITE
# latitude: 35.0
# longitude: 135.0
# elevation: 10.0
date time aot_500nm aot_675nm ae
2023-01-01 12:00:00 0.1 0.05 1.5
2023-01-01 13:00:00 0.2 0.10 1.5
"""
    fn.write_text(content)
    return str(fn)


def test_read_skynet_eager(mock_skynet_file):
    reader = SKYNETReader()
    df = reader.open_dataset(files=mock_skynet_file, as_xarray=False, lazy=False)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "aod_500nm" in df.columns
    assert "aod_675nm" in df.columns
    assert "angstrom_exponent" in df.columns
    assert df["latitude"].iloc[0] == 35.0
    assert df["longitude"].iloc[0] == 135.0
    assert df["siteid"].iloc[0] == "TEST_SITE"


def test_read_skynet_xarray(mock_skynet_file):
    reader = SKYNETReader()
    ds = reader.open_dataset(files=mock_skynet_file, as_xarray=True, lazy=False)

    assert isinstance(ds, xr.Dataset)
    assert "aod_500nm" in ds.data_vars
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "siteid" in ds.coords
    # If expand2d=True (default), it might have collapsed to 1 site if it thinks it is a single point
    # but PointReader.to_xarray with expand2d=True usually gives (time, node) or (time, siteid)
    # Let's check the dimensions
    if "time" in ds.dims and "node" in ds.dims:
        assert ds.sizes["time"] * ds.sizes["node"] >= 2
    else:
        assert ds.sizes["node"] >= 2


def test_read_skynet_lazy(mock_skynet_file):
    pytest.importorskip("dask")
    reader = SKYNETReader()
    ds = reader.open_dataset(files=mock_skynet_file, as_xarray=True, lazy=True)

    assert isinstance(ds, xr.Dataset)
    # Check if data is lazy (dask-backed)
    assert hasattr(ds["aod_500nm"].data, "dask")

    ds_computed = ds.compute()
    if "time" in ds_computed.dims and "node" in ds_computed.dims:
        assert ds_computed.sizes["time"] * ds_computed.sizes["node"] >= 2
    else:
        assert ds_computed.sizes["node"] >= 2


def test_build_urls():
    reader = SKYNETReader()
    urls = reader.build_urls(dates="2023-01-01", siteid="TEST_SITE", product="AOT")
    assert len(urls) == 1
    assert "2023/TEST_SITE_20230101.AOT" in urls[0]
