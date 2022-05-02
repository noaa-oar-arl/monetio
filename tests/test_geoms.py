from pathlib import Path

import pandas as pd
import pytest
import xarray as xr

from monetio import geoms

HERE = Path(__file__).parent

TEST_FP = (HERE / "data/tolnet-hdf4-test-data.hdf").absolute().as_posix()
TEST_FP_H4TONCCF = (HERE / "data/tolnet-hdf4-test-data_h4tonccf_nc4.nc").absolute().as_posix()


def test_open():
    ds = geoms.open_dataset(TEST_FP)
    assert "o3_mixing_ratio_volume_derived" in ds.variables
    assert tuple(ds["o3_mixing_ratio_volume_derived"].dims) == ("time", "altitude")
    assert ds.dims == {"time": 28, "altitude": 496}


def test_open_no_rename_vars():
    ds = geoms.open_dataset(TEST_FP, rename_all=False)
    assert "O3.MIXING.RATIO.VOLUME_DERIVED" in ds.variables
    assert tuple(ds["O3.MIXING.RATIO.VOLUME_DERIVED"].dims) == ("time", "altitude")
    assert ds.dims == {"time": 28, "altitude": 496}


def test_open_no_squeeze():
    ds = geoms.open_dataset(TEST_FP, squeeze=False)
    assert ds.dims == {
        "latitude": 1,
        "longitude": 1,
        "altitude_instrument": 1,
        "time": 28,
        "altitude": 496,
    }


def test_mjd2k():
    f0 = 0.0
    t0 = pd.Timestamp("2000-01-01 00:00:00")
    da = xr.DataArray(data=[f0])
    dti = pd.DatetimeIndex([t0])

    with pytest.raises(AttributeError):
        geoms._dti_from_mjd2000(da)

    da.attrs.update(VAR_UNITS="MJD2K")
    assert geoms._dti_from_mjd2000(da) == dti


def test_cmp_h4tonccf():
    ds = geoms.open_dataset(TEST_FP, rename_all=False)
    ds_h4tonccf = xr.open_dataset(TEST_FP_H4TONCCF)
    # Note: h4tonccf_nc4 replaces all `.` in var names to `_`
    assert sorted(ds.sizes.values()) == sorted(ds_h4tonccf.squeeze().sizes.values())
