import shutil
import warnings
from pathlib import Path

import pytest
from filelock import FileLock

from monetio.sat._tempo_l2_no2_mm import open_dataset

HERE = Path(__file__).parent


def retrieve_test_file():
    fn = "sample_TEMPO_NO2_L2_V03_20240826T204005Z_S012G01.nc"

    p = HERE / "data" / fn

    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for TEMPO NO2 L2 test")

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
    p_test = root_tmp_dir / "tempo_l2_test.nc"

    with FileLock(p_test.as_posix() + ".lock"):
        if p_test.is_file():
            return p_test
        else:
            p = retrieve_test_file()
            shutil.copy(p, p_test)
            return p_test


def test_open_dataset(test_file_path):
    vn = "vertical_column_troposphere"
    t_ref = "2024-08-26T20:40:05Z"
    ds = open_dataset(test_file_path, {vn: {}})[t_ref]

    assert set(ds.coords) == {"time", "lat", "lon"}
    assert set(ds) == {vn}
    assert set(ds.attrs) == {"granule_number", "reference_time_string", "scan_num"}

    with pytest.warns(
        UserWarning,
        match=(
            "Calculating pressure in TEMPO data requires surface_pressure. "
            + "Adding surface_pressure to output variables"
        ),
    ):
        ds2 = open_dataset(
            test_file_path,
            {vn: {}, "main_data_quality_flag": {"quality_flag_max": 0}, "pressure": {}},
        )[t_ref]
    assert set(ds2.variables) == {
        "lat",
        "lon",
        "main_data_quality_flag",
        "pressure",
        "surface_pressure",
        "time",
        "vertical_column_troposphere",
    }
    assert ds2["pressure"].dims == ("swt_level_stagg", "x", "y")
