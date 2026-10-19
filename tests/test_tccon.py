"""
Test TCCON Reader
"""

import dask.dataframe as dd
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


@pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
def test_tccon_reader_honors_dask_time_chunks(tmp_path, as_xarray):
    path = tmp_path / "tccon-chunks.nc"
    source = create_mock_tccon_dataset()
    source.to_netcdf(path, engine="h5netcdf")

    result = TCCONReader().open_dataset(
        str(path), use_dask=True, chunks={"time": 2}, as_xarray=as_xarray, expand2d=False
    )
    if as_xarray:
        try:
            assert hasattr(result.xco2.data, "dask")
            assert result.xco2.chunks == ((2, 2, 2, 2, 2),)
        finally:
            result.close()
    else:
        assert isinstance(result, dd.DataFrame)
        assert result.npartitions == 5
