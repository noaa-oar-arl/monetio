from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from monetio import raqms

DATA = Path(__file__).parent / "data"

TEST_FP = str(DATA / "uwhyb_06_01_2017_18Z.chem.assim.nc")


def _test_ds(ds):
    # Test _fix_time worked
    assert ds.time.values[0] == pd.Timestamp("2017-06-01 18:00")
    assert set(ds.dims) == {"time", "x", "y", "z"}
    assert "IDATE" not in ds.data_vars
    assert "Times" not in ds.data_vars

    # Test _fix_grid worked
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert np.all(ds.latitude.values[0, :] == ds.latitude.values[0, 0])
    assert np.all(ds.longitude.values[:, 0] == ds.longitude.values[0, 0])
    assert float(ds.longitude.min()) == -180.0  # 1-degree grid
    assert float(ds.longitude.max()) == 179.0
    assert np.all(ds.geop.mean(["time", "x", "y"]) > 0)

    # Test _fix_pres worked
    p_vns = {"surfpres_pa", "dp_pa", "pres_pa_mid"}
    assert p_vns.issubset(ds.variables)
    for vn in p_vns:
        assert ds[vn].units == "Pa"
    assert (ds["pres_pa_mid"].mean(dim=("time", "y", "x")) > 90000).all()
    assert (ds["dp_pa"].mean(dim=("time", "y", "x")) > 1000).all()
    assert 100000 > ds["surfpres_pa"].mean() > 95000


def test_open_dataset():
    ds = raqms.open_dataset(TEST_FP)
    _test_ds(ds)


def test_open_mfdataset():
    ds = raqms.open_mfdataset(TEST_FP)
    _test_ds(ds)


def test_open_dataset_bad():
    with pytest.raises(ValueError, match="^File format "):
        raqms.open_dataset("asdf")


def test_open_mfdataset_bad():
    with pytest.raises(ValueError, match="^File format "):
        raqms.open_mfdataset("asdf")
