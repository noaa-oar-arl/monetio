import shutil
import warnings
from pathlib import Path

import pandas as pd
import pytest
from filelock import FileLock

from monetio.sat._tropomi_l2_no2_mm import open_dataset

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


def test_open_dataset(test_file_path):
    vn = "nitrogendioxide_tropospheric_column"  # mol m-2
    t_ref = pd.Timestamp("2019-07-15")

    ds = open_dataset(test_file_path, vn)[t_ref.strftime(r"%Y%m%d")]

    assert set(ds.coords) == {"time", "lat", "lon", "scan_time"}
    assert set(ds) == {vn}

    assert 0 < ds[vn].mean() < 2e-4
    assert ds[vn].max() < 1e-3
    assert ds[vn].min() < 0

    assert ds.time.ndim == 0
    assert pd.Timestamp(ds.time.values) == t_ref
    assert (ds.scan_time.dt.floor("D") == t_ref).all()

    ds2 = open_dataset(test_file_path, {vn: {"minimum": 1e-9}})[t_ref.strftime(r"%Y%m%d")]
    assert ds2[vn].min() >= 1e-9
