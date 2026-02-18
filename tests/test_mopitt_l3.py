import shutil
import warnings
from pathlib import Path

import pandas as pd
import pytest
from filelock import FileLock

from monetio.sat.mopitt_l3 import get_start_time, load_variable, open_dataset

HERE = Path(__file__).parent


def retrieve_test_file():
    fn = "MOP03JM-201701-L3V95.9.3.he5"

    # Download to tests/data if not already present
    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for MOPITT L3 test")
        import time

        import requests

        url = (
            # "https://csl.noaa.gov/groups/csl4/modeldata/melodies-monet/data/"
            # f"example_observation_data/satellite/{fn}"
            "https://csl.noaa.gov/groups/csl4/modeldata/melodies-monet/data/example_observation_data/satellite/MOP03JM-201701-L3V95.9.3.he5"
        )
        max_retries = 5
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; MonetioTest/1.0; +https://github.com/noaa-oar-arl/monetio)"
        }
        success = False
        for attempt in range(max_retries):
            try:
                r = requests.get(url, stream=True, timeout=60, headers=headers)
                r.raise_for_status()
                with open(p, "wb") as f:
                    f.write(r.content)
                success = True
                break
            except Exception:
                time.sleep(2 * (attempt + 1))
        if not success:
            # Try wget as a fallback
            import subprocess

            try:
                subprocess.run(["wget", "-O", str(p), url], check=True, capture_output=True)
                success = True
            except Exception as e:
                pytest.skip(
                    f"Could not download test file {fn} from CSL using requests or wget: {e}"
                )

        # Post-download: check file size and HDF5 signature
        min_size_mb = 10
        if p.stat().st_size < min_size_mb * 1024 * 1024:
            pytest.skip(
                f"Downloaded file {fn} is too small (likely incomplete): {p.stat().st_size} bytes"
            )
        with open(p, "rb") as f:
            sig = f.read(8)
        if sig != b"\x89HDF\r\n\x1a\n":
            pytest.skip(f"Downloaded file {fn} does not have a valid HDF5 signature; got: {sig}")

        # Post-download: check file size and HDF5 signature
        min_size_mb = 10
        if p.stat().st_size < min_size_mb * 1024 * 1024:
            pytest.skip(
                f"Downloaded file {fn} is too small (likely incomplete): {p.stat().st_size} bytes"
            )
        with open(p, "rb") as f:
            sig = f.read(8)
        if sig != b"\x89HDF\r\n\x1a\n":
            pytest.skip(f"Downloaded file {fn} does not have a valid HDF5 signature; got: {sig}")

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
    p_test = root_tmp_dir / "mopitt_l3_test.he5"
    with FileLock(p_test.as_posix() + ".lock"):
        if p_test.is_file():
            return p_test
        else:
            p = retrieve_test_file()
            shutil.copy(p, p_test)
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
