import shutil
import warnings
from pathlib import Path

import pandas as pd
import pytest
from filelock import FileLock

from monetio.sat._mopitt_l3_mm import get_start_time, load_variable, open_dataset

HERE = Path(__file__).parent


def copy_test_file(p_test):
    fn = "MOP03JM-201701-L3V95.9.3.he5"

    # Download to tests/data if not already present
    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for MOPITT L3 test")
        import requests

        r = requests.get(
            "https://csl.noaa.gov/groups/csl4/modeldata/melodies-monet/data/"
            f"example_observation_data/satellite/{fn}",
            stream=True,
        )
        r.raise_for_status()
        with open(p, "wb") as f:
            f.write(r.content)

    # Copy to test location
    shutil.copy(p, p_test)


@pytest.fixture(scope="session")
def test_file_path(tmp_path_factory, worker_id):
    # Get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    p_test = root_tmp_dir / "mop.he5"

    if worker_id == "master":
        # Not executing with multiple workers; just produce the data and let
        # pytest's fixture caching do its job
        copy_test_file(p_test)
        return p_test

    with FileLock(str(p_test) + ".lock"):
        if p_test.is_file():
            return p_test
        else:
            copy_test_file(p_test)
            return p_test


def test_get_start_time(test_file_path):
    t = get_start_time(test_file_path)
    assert t.floor("D") == pd.Timestamp("2017-01-01")


def test_load_variable(test_file_path):
    ds = load_variable(test_file_path, "column")
    assert set(ds.coords) == {"lon", "lat"}
    assert set(ds) == {"column"}
    assert ds.column.mean() > 0


def test_open_dataset(test_file_path):
    ds = open_dataset(test_file_path, "column")
    assert set(ds.coords) == {"time", "lat", "lon"}
    assert set(ds) == {"column"}
    assert ds.column.mean() > 0
    assert (ds.time.dt.floor("D") == pd.Timestamp("2017-01-01")).all()
