import numpy as np
import pytest
import xarray as xr

from monetio.models._cesm_fv_mm import _calc_pressure, open_mfdataset


@pytest.fixture(scope="module")
def test_file_path(tmp_path_factory):
    """Write a small CAM-Chem grid with hybrid pressure coefficients."""
    path = tmp_path_factory.mktemp("cesm_fv") / "cam.nc"
    dims = ("time", "lev", "lat", "lon")
    shape = (1, 2, 2, 3)
    with xr.Dataset(
        {
            "NO2": (dims, np.full(shape, 2e-9), {"units": "mol/mol"}),
            "T": (dims, np.full(shape, 280.0), {"units": "K"}),
            "Z3": (dims, np.broadcast_to(np.array([3000.0, 500.0])[None, :, None, None], shape)),
            "PS": (("time", "lat", "lon"), np.full((1, 2, 3), 90_000.0)),
            "PHIS": (("lat", "lon"), np.zeros((2, 3))),
            "P0": 100_000.0,
            "hyam": ("lev", [0.2, 0.1]),
            "hybm": ("lev", [0.5, 0.85]),
        },
        coords={"time": [0], "lev": [500, 900], "lat": [35.0, 36.0], "lon": [260.0, 261.0, 262.0]},
    ) as ds:
        ds.to_netcdf(path, engine="netcdf4")
    return path


def _check_dimensions(ds):
    assert set(ds.dims) >= {"time", "x", "y"}


def _check_latitude_and_longitude(ds):
    assert "lat" not in ds.variables
    assert "lon" not in ds.variables
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    # Check for 2D coords (xarray standard in monetio)
    assert ds.latitude.ndim == 2
    assert ds.longitude.ndim == 2


def test_open_mfdataset(test_file_path):
    file_path = str(test_file_path)
    var_list = ["NO2"]
    ds = open_mfdataset(file_path, var_list=var_list, engine="netcdf4")
    _check_dimensions(ds)
    _check_latitude_and_longitude(ds)
    assert "time" in ds.coords
    assert ds["NO2"].attrs["units"] == "ppbv"


def test_open_mfdataset_surf_only_false(test_file_path):
    file_path = str(test_file_path)
    var_list = ["NO2"]
    ds = open_mfdataset(file_path, var_list=var_list, surf_only=False, engine="netcdf4")
    _check_dimensions(ds)
    _check_latitude_and_longitude(ds)
    assert "pres_pa_mid" in ds.variables
    assert ds["pres_pa_mid"].attrs["units"] == "Pa"


def test_calc_pressure(test_file_path):
    file_path = str(test_file_path)
    with xr.open_dataset(file_path, engine="netcdf4") as ds:
        pressure = _calc_pressure(ds)
    assert pressure.dims == ("time", "lev", "lat", "lon")
    assert pressure.attrs["units"] == "Pa"
    np.testing.assert_allclose(pressure.isel(time=0, lat=0, lon=0), [65_000.0, 86_500.0])
