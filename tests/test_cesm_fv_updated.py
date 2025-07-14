import shutil
import warnings
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from filelock import FileLock

from monetio.models._cesm_fv_mm import _calc_pressure, _calc_pressure_i, open_mfdataset

HERE = Path(__file__).parent


def retrieve_test_file():
    fn = "f.e22.FCnudged.f09_f09_mg17.cst_emis.cam.h1.2018-12-25-43200.nc"

    # Download to tests/data if not already present
    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for CESM-FV test")
        import requests

        r = requests.get(
            "https://csl.noaa.gov/groups/csl4/modeldata/melodies-monet/data/"
            + f"example_model_data/cesmfv_example/{fn}",
            stream=True,
        )
        r.raise_for_status()
        with open(p, "wb") as f:
            f.write(r.content)

    return p


@pytest.fixture(scope="module")
def test_file_path(tmp_path_factory, worker_id=None):
    worker_id = "master"

    if worker_id == "master":
        # Not executing with multiple workers;
        # let pytest's fixture caching do its job
        return retrieve_test_file()

    # Get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # Copy to the shared test location
    p_test = root_tmp_dir / "cesm_fv_test.nc"
    with FileLock(p_test.as_posix() + ".lock"):
        if p_test.is_file():
            return p_test
        else:
            p = retrieve_test_file()
            shutil.copy(p, p_test)
            return p_test


def _check_dimensions(ds):
    assert set(ds.dims) == {"time", "x", "y", "z"}


def _check_latitude_and_longitude(ds):
    assert "lat" not in ds.variables
    assert "lon" not in ds.variables
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert np.all(ds.latitude.values[0, :] == ds.latitude.values[0, 0])
    assert np.all(ds.longitude.values[:, 0] == ds.longitude.values[0, 0])
    assert np.all(ds.latitude.values >= -90) and np.all(
        ds.latitude.values <= 90
    ), "Latitude values are out of range. "
    assert np.all(ds.longitude.values >= -180) and np.all(
        ds.longitude.values <= 180
    ), "Longitude values are out of range. "


def _check_time(ds):
    assert "time" in ds.coords, "Time coordinate is missing. "
    assert len(ds.time) > 0, "Time dimension is empty. "


def _check_species_variables(ds):
    species_list = ["O3", "NO2"]
    for sp in species_list:
        if sp in ds.variables:
            assert sp in ds.variables, f"{sp} variable is missing. "
            assert tuple(ds[sp].dims) == (
                "time",
                "z",
                "y",
                "x",
            ), f"Dimensions for {sp} are incorrect. "
            assert ds[sp].attrs["units"] == "ppbv", f"Units for {sp} are incorrect. "


def _check_vertical_levels(ds):
    assert np.all(np.diff(ds["z"].values) > 0), "Vertical levels are not flipped correctly. "


def _check_pressure_vars(ds):
    assert "pres_pa_mid" in ds.variables, "pres_pa_mid variable is missing. "
    assert tuple(ds["pres_pa_mid"].dims) == (
        "time",
        "z",
        "y",
        "x",
    ), "Dimensions for pres_pa_mid are incorrect. "
    assert ds["pres_pa_mid"].attrs["units"] == "Pa", "Units for pres_pa_mid are incorrect. "


def _check_temperature(ds):
    assert "temperature_k" in ds.variables, "temperature_k variable is missing. "
    assert tuple(ds["temperature_k"].dims) == (
        "time",
        "z",
        "y",
        "x",
    ), "Dimensions for temperature_k are incorrect. "
    assert ds["temperature_k"].attrs["units"] == "K", "Units for temperature_k are incorrect. "


def _check_altitude(ds):
    assert "alt_msl_m_mid" in ds.variables, "alt_msl_m_mid variable is missing. "
    assert tuple(ds["alt_msl_m_mid"].dims) == (
        "time",
        "z",
        "y",
        "x",
    ), "Dimensions for alt_msl_m_mid are incorrect. "
    assert ds["alt_msl_m_mid"].attrs["units"] == "m", "Units for alt_msl_m_mid are incorrect. "


def test_open_mfdataset(test_file_path):
    file_path = str(test_file_path)
    var_list = ["NO2"]
    ds = open_mfdataset(file_path, var_list=var_list)
    _check_dimensions(ds)
    _check_latitude_and_longitude(ds)
    _check_time(ds)
    _check_species_variables(ds)
    _check_vertical_levels(ds)


def test_open_mfdataset_surf_only_false(test_file_path):
    file_path = str(test_file_path)
    var_list = ["NO2"]
    ds = open_mfdataset(file_path, var_list=var_list, surf_only=False)
    _check_dimensions(ds)
    _check_latitude_and_longitude(ds)
    _check_time(ds)
    _check_species_variables(ds)
    _check_vertical_levels(ds)
    _check_pressure_vars(ds)
    _check_temperature(ds)
    _check_altitude(ds)


def test_hybrid_vars(test_file_path):
    file_path = str(test_file_path)
    ds = xr.open_mfdataset(file_path)
    assert "hyam" in ds.variables, "hyam variable is missing. "
    assert tuple(ds["hyam"].dims) == ("lev",), "Dimensions for hyam are incorrect. "
    assert "hybm" in ds.variables, "hybm variable is missing. "
    assert tuple(ds["hybm"].dims) == ("lev",), "Dimensions for hybm are incorrect. "
    assert "hyai" in ds.variables, "hyai variable is missing. "
    assert tuple(ds["hyai"].dims) == ("ilev",), "Dimensions for hyai are incorrect. "
    assert "hybi" in ds.variables, "hybi variable is missing. "
    assert tuple(ds["hybi"].dims) == ("ilev",), "Dimensions for hybi are incorrect. "


def test_calc_pressure(test_file_path):
    file_path = str(test_file_path)
    ds = xr.open_mfdataset(file_path)
    pressure = _calc_pressure(ds)
    assert tuple(pressure.dims) == (
        "time",
        "lev",
        "lat",
        "lon",
    ), "Dimensions for pressure are incorrect. "
    assert pressure.attrs["units"] == "Pa", "Units for pressure are incorrect. "


def test_calc_pressure_i(test_file_path):
    file_path = str(test_file_path)
    ds = xr.open_mfdataset(file_path)
    pressure_i = _calc_pressure_i(ds)
    assert tuple(pressure_i.dims) == (
        "time",
        "ilev",
        "lat",
        "lon",
    ), "Dimensions for pressure are incorrect. "
    assert pressure_i.attrs["units"] == "Pa", "Units for pressure are incorrect. "
