import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.earlinet import EARLINETReader


def test_earlinet_reader_basic(tmp_path):
    # Create a mock EARLINET NetCDF file
    fn = tmp_path / "ipr_001_20240101.nc"

    # Mock data
    time_vals = pd.to_datetime(["2024-01-01 12:00:00", "2024-01-01 13:00:00"])
    alt_vals = np.linspace(500, 10000, 200)  # m

    ds_mock = xr.Dataset(
        data_vars={
            "backscatter": (("wavelength", "time", "altitude"), np.random.rand(1, 2, 200)),
            "latitude": ((), 45.82),
            "longitude": ((), 8.617),
        },
        coords={
            "time": (("time",), time_vals),
            "altitude": (("altitude",), alt_vals),
            "wavelength": (("wavelength",), [1064.0]),
        },
        attrs={
            "title": "EARLINET Data",
            "station_ID": "ipr",
        },
    )
    ds_mock.latitude.attrs["units"] = "degrees_north"
    ds_mock.longitude.attrs["units"] = "degrees_east"
    ds_mock.altitude.attrs["units"] = "m"
    ds_mock.backscatter.attrs["units"] = "m-1*sr-1"

    ds_mock.to_netcdf(fn)

    reader = EARLINETReader()

    # Test Eager
    ds = reader.open_dataset(files=str(fn), lazy=False)
    assert isinstance(ds, xr.Dataset)
    assert "backscatter" in ds.data_vars
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "wavelength" in ds.coords
    assert ds.latitude.attrs["units"] == "degrees_north"
    assert ds.longitude.attrs["units"] == "degrees_east"
    assert "history" in ds.attrs
    assert "Preprocessed EARLINET data" in ds.attrs["history"]

    # Test Lazy
    ds_lazy = reader.open_dataset(files=str(fn), lazy=True)
    assert isinstance(ds_lazy, xr.Dataset)
    assert ds_lazy.backscatter.chunks is not None
    assert "latitude" in ds_lazy.coords
    assert "longitude" in ds_lazy.coords


def test_earlinet_load_universal(tmp_path):
    import monetio

    # Create a mock EARLINET NetCDF file
    fn = tmp_path / "ipr_001_20240101_2.nc"
    ds_mock = xr.Dataset(
        data_vars={
            "backscatter": (("wavelength", "time", "altitude"), np.random.rand(1, 1, 10)),
            "latitude": ((), 45.82),
            "longitude": ((), 8.617),
        },
        coords={
            "time": (("time",), [pd.Timestamp("2024-01-01")]),
            "altitude": (("altitude",), np.arange(10)),
            "wavelength": (("wavelength",), [1064.0]),
        },
    )
    ds_mock.to_netcdf(fn)

    ds = monetio.load("earlinet", files=str(fn))
    assert isinstance(ds, xr.Dataset)
    assert "backscatter" in ds.data_vars
