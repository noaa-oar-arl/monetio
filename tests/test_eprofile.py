import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.eprofile import EPROFILEReader


def test_eprofile_reader_basic(tmp_path):
    # Create a mock E-PROFILE NetCDF file
    fn = tmp_path / "EPROFILE_L1_20240101_SITE.nc"

    # Mock data
    time_vals = pd.to_datetime(["2024-01-01 00:00:00", "2024-01-01 00:01:00"])
    range_vals = np.linspace(10, 5000, 500)  # meters

    ds_mock = xr.Dataset(
        data_vars={
            "beta": (("time", "range"), np.random.rand(2, 500)),
            "station_latitude": (("time",), [40.0, 40.0]),
            "station_longitude": (("time",), [-80.0, -80.0]),
            "station_altitude": (("time",), [100.0, 100.0]),  # meters
        },
        coords={
            "time": (("time",), time_vals),
            "range": (("range",), range_vals),
        },
        attrs={
            "title": "E-PROFILE ALC Data",
        },
    )
    ds_mock.station_altitude.attrs["units"] = "m"
    ds_mock.station_latitude.attrs["units"] = "degrees_north"
    ds_mock.station_longitude.attrs["units"] = "degrees_east"
    ds_mock.range.attrs["units"] = "m"
    ds_mock.beta.attrs["units"] = "m-1 sr-1"

    ds_mock.to_netcdf(fn)

    reader = EPROFILEReader()

    # Test Eager
    ds = reader.open_dataset(files=str(fn), lazy=False)
    assert isinstance(ds, xr.Dataset)
    assert "attenuated_backscatter" in ds.data_vars
    assert "elevation" in ds.coords
    assert ds.elevation.attrs["units"] == "m"
    assert ds.elevation.values[0] == 100.0
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "altitude" in ds.coords
    # altitude = elevation (100) + range (10..5000)
    assert ds.altitude.values[0, 0] == 110.0
    assert "history" in ds.attrs
    assert "Preprocessed E-PROFILE data" in ds.attrs["history"]

    # Test Lazy
    ds_lazy = reader.open_dataset(files=str(fn), lazy=True)
    assert isinstance(ds_lazy, xr.Dataset)
    assert ds_lazy.attenuated_backscatter.chunks is not None
    assert "elevation" in ds_lazy.coords
    assert ds_lazy.elevation.attrs["units"] == "m"
    assert "altitude" in ds_lazy.coords


def test_eprofile_load_redirection(tmp_path):
    import monetio

    # Create a mock E-PROFILE NetCDF file
    fn = tmp_path / "EPROFILE_L1_SITE2.nc"
    ds_mock = xr.Dataset(
        data_vars={
            "beta": (("time", "range"), np.random.rand(1, 10)),
            "station_latitude": (("time",), [40.0]),
            "station_longitude": (("time",), [-80.0]),
        },
        coords={
            "time": (("time",), [pd.Timestamp("2024-01-01")]),
            "range": (("range",), np.arange(10)),
        },
    )
    ds_mock.to_netcdf(fn)

    # Test via monetio.load
    ds = monetio.load("eprofile", files=str(fn))
    assert isinstance(ds, xr.Dataset)
    assert "attenuated_backscatter" in ds.data_vars

    # Test via monetio.obs.eprofile.add_data
    from monetio.obs.eprofile import add_data

    ds_obs = add_data(files=str(fn))
    assert isinstance(ds_obs, xr.Dataset)
    assert "attenuated_backscatter" in ds_obs.data_vars


def test_eprofile_not_implemented():
    reader = EPROFILEReader()
    with pytest.raises(NotImplementedError):
        reader.open_dataset(files=None, dates="2024-01-01")
