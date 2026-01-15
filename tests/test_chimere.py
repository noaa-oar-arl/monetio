from pathlib import Path

import pytest
import xarray as xr

from monetio import chimere

DATA = Path(__file__).parent / "data"
VAR_LIST = ["temp", "PM25", "PM10", "OX", "NO2", "NO3", "CH4"]

TEST_FP = str(DATA / "chimere_test.nc")


@pytest.fixture(scope="module")
def chimere_test_file():
    test_file = Path(TEST_FP).resolve()
    if test_file.exists() and test_file.is_file():
        return test_file
    raise FileNotFoundError(f"File {TEST_FP} not found. Download first.")


def _test_ds(xrds, var_list: list[str] = []):
    assert isinstance(xrds, xr.Dataset)
    assert all(coord in xrds.coords for coord in ["longitude", "latitude"])
    assert all(dim in list(xrds.dims.keys()) for dim in ["time", "z", "x", "y"])
    assert all(var in list(xrds.data_vars.keys()) for var in var_list)


def test_openmfdataset_chimere(chimere_test_file):
    var_list = VAR_LIST
    ds = chimere.open_mfdataset(chimere_test_file, var_list=var_list, surf_only=True)
    _test_ds(ds, var_list=var_list)
