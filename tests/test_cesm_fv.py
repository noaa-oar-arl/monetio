import warnings
from pathlib import Path

import pytest
import xarray as xr

from monetio.models._cesm_fv_mm import _calc_pressure, open_mfdataset

HERE = Path(__file__).parent

cesm_xdist = pytest.mark.xdist_group(name="retrieve-files")


def retrieve_test_file(updated=True):
    if updated:
        fn = "f.e22.FCnudged.f09_f09_mg17.cst_emis.cam.h1.2018-12-25-43200.nc"
    else:
        fn = "CAM_chem_merra2_FCSD_1deg_QFED_world_201909-01-09_small_sfc.nc"

    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for CESM-FV test")
        import requests

        try:
            r = requests.get(
                "https://csl.noaa.gov/groups/csl4/modeldata/melodies-monet/data/"
                + f"example_model_data/cesmfv_example/{fn}",
                stream=True,
                timeout=30,
            )
            r.raise_for_status()
            with open(p, "wb") as f:
                f.write(r.content)
        except Exception as e:
            pytest.skip(f"Failed to download {fn}: {e}")

    return p


@pytest.fixture(scope="module")
def test_file_path(tmp_path_factory, worker_id):
    p = retrieve_test_file(updated=True)
    return p


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


@cesm_xdist
@pytest.mark.network
def test_open_mfdataset(test_file_path):
    file_path = str(test_file_path)
    var_list = ["NO2"]
    ds = open_mfdataset(file_path, var_list=var_list, engine="netcdf4")
    _check_dimensions(ds)
    _check_latitude_and_longitude(ds)
    assert "time" in ds.coords
    if "NO2" in ds.variables:
        assert ds["NO2"].attrs["units"] == "ppbv"


@cesm_xdist
@pytest.mark.network
def test_open_mfdataset_surf_only_false(test_file_path):
    file_path = str(test_file_path)
    var_list = ["NO2"]
    ds = open_mfdataset(file_path, var_list=var_list, surf_only=False, engine="netcdf4")
    _check_dimensions(ds)
    _check_latitude_and_longitude(ds)
    assert "pres_pa_mid" in ds.variables
    assert ds["pres_pa_mid"].attrs["units"] == "Pa"


@cesm_xdist
@pytest.mark.network
def test_calc_pressure(test_file_path):
    file_path = str(test_file_path)
    ds = xr.open_mfdataset(file_path, engine="netcdf4")
    pressure = _calc_pressure(ds)
    assert "time" in pressure.dims
    assert pressure.attrs["units"] == "Pa"
