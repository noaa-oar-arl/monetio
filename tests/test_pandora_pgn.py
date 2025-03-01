import shutil
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from filelock import FileLock

from monetio import pandora_pgn

HERE = Path(__file__).parent


def retrieve_test_file(fn, loc_online="BoulderCO-NCAR/Pandora204s1/L2"):
    p = HERE / "data" / fn

    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for Pandora PGN test")

        import requests

        r = requests.get(
            f"https://data.ovh.pandonia-global-network.org/{loc_online}/{fn}",
            stream=True,
        )
        r.raise_for_status()
        with open(p, "wb") as f:
            f.write(r.content)
        print("p:", p)
    return p


@pytest.fixture(scope="module")
def test_file_path(tmp_path_factory, worker_id, fn):
    if worker_id == "master":
        # Not executing with multiple workers;
        # let pytest's fixture caching do its job
        return retrieve_test_file(fn)

    # Get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # Copy to the shared test location
    p_test = root_tmp_dir / "tempo_l2_test.nc"

    with FileLock(p_test.as_posix() + ".lock"):
        if p_test.is_file():
            return p_test
        else:
            p = retrieve_test_file(fn)
            shutil.copy(p, p_test)
            return p_test


def is_valid_xarray(data):
    assert isinstance(data, xr.Dataset)
    assert {"latitude", "longitude", "time"}.issubset(set(data.coords))
    assert np.all((-90 <= data["latitude"]) & (data["latitude"] <= 90))
    assert np.all((-180 <= data["longitude"]) & (data["longitude"] <= 180))
    assert np.issubdtype(data["time"].dtype, np.datetime64)
    assert set(data.dims) == {"time", "x"}
    assert data["siteid"].dims == ("x",)

    # Assert dimensions over every variable except for siteid
    data_vars_col = list(data.data_vars)
    data_vars_col.remove("siteid")

    for v in data_vars_col:
        assert data[v].dims == ("time", "x")
        assert np.issubdtype(data[v].dtype, np.number)


def test_parse_metadata():
    assert isinstance(pandora_pgn._parse_metadata("1"), int)
    assert isinstance(pandora_pgn._parse_metadata("1."), float)
    assert isinstance(pandora_pgn._parse_metadata("1.1"), float)
    assert isinstance(pandora_pgn._parse_metadata("Mock string"), str)
    assert pandora_pgn._parse_metadata(" Mock") == "Mock"
    assert pandora_pgn._parse_metadata("string ") == "string"
    assert pandora_pgn._parse_metadata(" Mock string ") == "Mock string"
    assert pandora_pgn._parse_metadata("") == ""
    assert pandora_pgn._parse_metadata(" ") == ""
    assert pandora_pgn._parse_metadata("     ") == ""
    assert np.issubdtype(pandora_pgn._parse_metadata("2019-01-23T00:01:03"), np.datetime64)


def test_rename_and_format():
    df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]))
    renamed = pandora_pgn._rename_and_format(df)
    assert isinstance(renamed, xr.Dataset)
    assert set(renamed.dims) == {"time", "x"}
    assert "time" in renamed
    # Column 1 should have been renamed to time
    assert "Column 1" not in renamed
    assert "Column 2" in renamed
    assert "Column 3" in renamed


def test_merge_global_attrs():
    temp1 = np.ones(5)
    temp2 = 2 * np.ones(5)
    ds1 = xr.Dataset(
        data_vars={"temp": (("x",), temp1)},
        attrs={"mock1": "mock_my_data_ds1", "mock2": "mock_again_ds1", "only_1": "only_ds1"},
    )
    ds2 = xr.Dataset(
        data_vars={"temp": (("x",), temp2)},
        attrs={"mock1": "mock_my_data_ds2", "mock2": "mock_again_ds2", "only_2": "only_ds2"},
    )
    merged = xr.concat([ds1, ds2], dim="x")
    pandora_pgn._merge_global_attrs(ds1, ds2, merged)
    assert merged.attrs["mock1"] == ["mock_my_data_ds1", "mock_my_data_ds2"]


def test_read_pandora_file():
    fn_with_extra_cols = "Pandora204s1_BoulderCO-NCAR_L2_rfuh5p1-8.txt"
    fn_without_extra_cols = "Pandora204s1_BoulderCO-NCAR_L2_rfus5p1-8.txt"
    files = [fn_with_extra_cols, fn_without_extra_cols]
    for f in files:
        file_path = retrieve_test_file(f)
        file = pandora_pgn._read_pandora_file(file_path)
        is_valid_xarray(file)


def test_open_mfdataset():
    fn_with_extracols = [
        ["Pandora204s1_BoulderCO-NCAR_L2_rfuh5p1-8.txt", "BoulderCO-NCAR/Pandora204s1/L2"],
        ["Pandora57s1_BoulderCO_L2_rfuh5p1-8.txt", "BoulderCO/Pandora57s1/L2"],
    ]
    fn_without_extracols = [
        ["Pandora204s1_BoulderCO-NCAR_L2_rfus5p1-8.txt", "BoulderCO-NCAR/Pandora204s1/L2"],
        ["Pandora57s1_BoulderCO_L2_rfus5p1-8.txt", "BoulderCO/Pandora57s1/L2"],
    ]
    file_paths = [retrieve_test_file(*fn) for fn in fn_with_extracols]
    data_with_extracols = pandora_pgn.open_mfdataset(file_paths)
    is_valid_xarray(data_with_extracols)
    file_paths = [retrieve_test_file(*fn) for fn in fn_without_extracols]
    data_without_extracols = pandora_pgn.open_mfdataset(file_paths)
    is_valid_xarray(data_without_extracols)
