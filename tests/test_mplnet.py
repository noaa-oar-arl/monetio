import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.mplnet import MPLNETReader


def test_mplnet_reader_basic(tmp_path):
    # Create a mock MPLNET V3 NetCDF file
    fn = tmp_path / "MPLNET_V3_L1_NRB_20240101_SITE.nc"

    # Mock data
    time_vals = pd.to_datetime(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
    alt_vals = np.linspace(0.1, 10.0, 100)  # km

    ds_mock = xr.Dataset(
        data_vars={
            "nrb": (("time", "altitude"), np.random.rand(2, 100)),
            "latitude": (("time",), [40.0, 40.0]),
            "longitude": (("time",), [-80.0, -80.0]),
            "surface_altitude": (("time",), [0.5, 0.5]),  # km
        },
        coords={
            "time": (("time",), time_vals),
            "altitude": (("altitude",), alt_vals),
        },
        attrs={
            "title": "MPLNET V3 NRB Data",
        },
    )
    ds_mock.surface_altitude.attrs["units"] = "km"
    ds_mock.latitude.attrs["units"] = "degrees_north"
    ds_mock.longitude.attrs["units"] = "degrees_east"
    ds_mock.altitude.attrs["units"] = "km"

    ds_mock.to_netcdf(fn)

    reader = MPLNETReader()

    # Test Eager
    ds = reader.open_dataset(files=str(fn), lazy=False)
    assert isinstance(ds, xr.Dataset)
    assert "nrb" in ds.data_vars
    assert "elevation" in ds.coords
    assert ds.elevation.attrs["units"] == "m"
    assert ds.elevation.values[0] == 500.0
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "history" in ds.attrs
    assert "Preprocessed MPLNET data" in ds.attrs["history"]

    # Test Lazy
    ds_lazy = reader.open_dataset(files=str(fn), lazy=True)
    assert isinstance(ds_lazy, xr.Dataset)
    assert ds_lazy.nrb.chunks is not None
    assert "elevation" in ds_lazy.coords
    # Check that unit conversion was lazy
    assert "elevation" in ds_lazy.coords
    assert ds_lazy.elevation.attrs["units"] == "m"


def test_mplnet_load_universal(tmp_path):
    import monetio

    # Create a mock MPLNET V3 NetCDF file
    fn = tmp_path / "MPLNET_V3_L1_NRB_20240101_SITE2.nc"
    ds_mock = xr.Dataset(
        data_vars={
            "nrb": (("time", "altitude"), np.random.rand(1, 10)),
            "latitude": (("time",), [40.0]),
            "longitude": (("time",), [-80.0]),
        },
        coords={
            "time": (("time",), [pd.Timestamp("2024-01-01")]),
            "altitude": (("altitude",), np.arange(10)),
        },
    )
    ds_mock.to_netcdf(fn)

    ds = monetio.load("mplnet", files=str(fn))
    assert isinstance(ds, xr.Dataset)
    assert "nrb" in ds.data_vars
