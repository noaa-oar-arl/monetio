import shutil
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from filelock import FileLock

from monetio.sat._tropomi_l2_no2_mm import open_dataset, read_trpdataset

HERE = Path(__file__).parent


def retrieve_test_file():
    fn = "TROPOMI-L2-NO2-20190715.nc"

    # Download to tests/data if not already present
    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for TROPOMI L2 test")
        import requests

        r = requests.get(
            "https://csl.noaa.gov/groups/csl4/modeldata/melodies-monet/data/"
            f"example_observation_data/satellite/{fn}",
            stream=True,
        )
        r.raise_for_status()
        with open(p, "wb") as f:
            f.write(r.content)

    return p


@pytest.fixture(scope="module")
def test_file_path(tmp_path_factory, worker_id):
    if worker_id == "master":
        # Not executing with multiple workers;
        # let pytest's fixture caching do its job
        return retrieve_test_file()

    # Get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # Copy to the shared test location
    p_test = root_tmp_dir / "tropomi_l2_test.he5"
    with FileLock(p_test.as_posix() + ".lock"):
        if p_test.is_file():
            return p_test
        else:
            p = retrieve_test_file()
            shutil.copy(p, p_test)
            return p_test


T_REF = pd.Timestamp("2019-07-15")
KEY = T_REF.strftime(r"%Y-%m-%d")


def test_open_dataset(test_file_path):
    vn = "nitrogendioxide_tropospheric_column"  # mol m-2

    ds = open_dataset(test_file_path, vn)[KEY][0]

    with pytest.warns(FutureWarning, match="read_trpdataset is an alias"):
        ds_alias = read_trpdataset(test_file_path, vn)[KEY][0]
    assert ds_alias.identical(ds)

    assert set(ds.coords) == {"time", "lat", "lon", "scan_time"}
    assert set(ds) == {vn}

    assert 0 < ds[vn].mean() < 2e-4
    assert ds[vn].max() < 1e-3
    assert ds[vn].min() < 0

    assert ds.time.ndim == 0
    assert pd.Timestamp(ds.time.values) == T_REF
    assert (ds.scan_time.dt.floor("D") == T_REF).all()

    ds2 = open_dataset(
        test_file_path,
        {
            vn: {"minimum": 1e-9},
            "latitude_bounds": {},
            "longitude_bounds": {},
            "preslev": {},
            "qa_value": None,
        },
    )[KEY][0]

    assert not ds2[vn].isnull().all()
    assert ds2[vn].min() >= 1e-9

    for i in range(4):
        assert not ds2[f"latitude_bounds_{i}"].isnull().any()
        assert ds2[f"latitude_bounds_{i}"].min() >= -90
        assert ds2[f"latitude_bounds_{i}"].max() <= 90
        assert not ds2[f"longitude_bounds_{i}"].isnull().any()
        assert ds2[f"longitude_bounds_{i}"].min() >= -180
        assert ds2[f"longitude_bounds_{i}"].max() <= 180

    assert not ds2["preslev"].isnull().all()
    assert ds2.preslev.mean(dim=("y", "x")).diff("z").to_series().lt(0).all(), "surface first"
    assert not ds2["troppres"].isnull().all()
    assert ds2["troppres"].mean() < ds2["preslev"].mean()

    qa = ds2["qa_value"]
    assert not ds2[vn].where(qa <= 0.7).isnull().all()


def test_open_dataset_qa(test_file_path):
    vn = "nitrogendioxide_tropospheric_column"  # mol m-2

    # Based on example YML from Meng
    ds = open_dataset(
        test_file_path,
        {
            "qa_value": {"quality_flag_min": 0.7, "var_applied": [vn], "fillvalue": None},
            vn: {"scale": 60221410000000000000, "fillvalue": 9.96921e36},
            "averaging_kernel": {"fillvalue": 9.96921e36},
            "air_mass_factor_total": {"fillvalue": 9.96921e36},
            "air_mass_factor_troposphere": {"fillvalue": 9.96921e36},
            "latitude": None,
            "longitude": None,
            "preslev": {
                "tm5_constant_a": {"group": ["PRODUCT"], "maximum": 9e36},
                "tm5_constant_b": {"group": ["PRODUCT"], "maximum": 9e36},
                "surface_pressure": {"group": ["PRODUCT/SUPPORT_DATA/INPUT_DATA"], "maximum": 9e36},
                "tm5_tropopause_layer_index": {"group": ["PRODUCT"]},
            },
        },
    )[KEY][0]

    # assert {vn, "ph", "phb", "pb", "p", "T"} <= set(ds.data_vars)

    qa = ds["qa_value"]
    assert ds[vn].where(qa <= 0.7).isnull().all()


def test_open_dataset_opts(test_file_path):
    vn = "nitrogendioxide_tropospheric_column"  # mol m-2

    def get(**kwargs):
        granules = open_dataset(
            test_file_path,
            {
                vn: kwargs,
            },
        )
        return granules[KEY][0]

    def om(x):
        return np.floor(np.log10(x))

    ds0 = get()
    assert om(ds0[vn].mean()) == -6
    assert ds0[vn].min() < 0
    assert om(ds0[vn].max()) == -4
    assert np.isclose(ds0[vn], 0, atol=0).sum() == 0

    ds = get(scale=1000)
    assert om(ds[vn].mean()) == -3

    ds = get(minimum=0)
    assert ds[vn].min() >= 0
    n = ds[vn].isnull().sum()
    tgt = 1.0311603546142578e-05
    assert np.isclose(ds[vn], tgt, atol=0).sum() > 0

    ds = get(maximum=1e-5)
    assert ds[vn].max() <= 1e-5

    ds = get(minimum=0, fillvalue=tgt)
    assert ds[vn].min() > 0
    assert ds[vn].isnull().sum() > n
    assert np.isclose(ds[vn], tgt, atol=0).sum() == 0
