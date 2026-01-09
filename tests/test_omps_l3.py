import shutil
import warnings
from pathlib import Path

import pandas as pd
import pytest
from filelock import FileLock

from monetio.sat._omps_l3_mm import open_dataset

HERE = Path(__file__).parent

FNS = [
    "OMPS-TO3-L3-DAILY_v2.1_20190905.h5",
    "OMPS-TO3-L3-DAILY_v2.1_20190906.h5",
]


def retrieve_test_file(i):
    fn = FNS[i]

    # Download to tests/data if not already present
    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for OMPS L3 test")
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
def test_file_paths(tmp_path_factory, worker_id):
    if worker_id == "master":
        # Not executing with multiple workers;
        # let pytest's fixture caching do its job
        return [retrieve_test_file(i) for i in range(len(FNS))]

    # Get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # Copy to the shared test location
    p_tests = []
    for i, p in enumerate(FNS):
        p_test = root_tmp_dir / f"omps_l3_test_{i}.he5"
        with FileLock(p_test.as_posix() + ".lock"):
            if not p_test.is_file():
                p = retrieve_test_file(i)
                shutil.copy(p, p_test)
            p_tests.append(p_test)

    return p_tests


def test_open_dataset(test_file_paths):
    ps = test_file_paths
    ds = open_dataset(ps)

    assert ds.sizes["time"] == len(ps) == 2
    assert ds.sizes["x"] == 360
    assert ds.sizes["y"] == 180

    assert ds.time.isel(time=0) == pd.Timestamp("2019-09-05")
    assert ds.time.isel(time=1) == pd.Timestamp("2019-09-06")
    assert ds.longitude.dims == ds.latitude.dims == ("y", "x")
    assert ds.longitude.min() == -179.5
    assert ds.longitude.max() == 179.5
    assert ds.latitude.min() == -89.5
    assert ds.latitude.max() == 89.5

    assert set(ds.data_vars) == {"ozone_column"}
    assert 150 < ds.ozone_column.mean() < 400
    assert ds.ozone_column.min() > 100
    assert ds.ozone_column.max() < 600
