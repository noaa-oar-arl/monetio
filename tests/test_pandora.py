import shutil
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from filelock import FileLock

from monetio import pandora
from monetio.util import _get_pandas_version

PD_GTE_2 = _get_pandas_version() >= (2, 0)

if not PD_GTE_2:
    pytest.skip("needs pandas 2+", allow_module_level=True)

HERE = Path(__file__).parent

URL_PATHS = [
    "BoulderCO-NCAR/Pandora204s1/L2/Pandora204s1_BoulderCO-NCAR_L2_rfuh5p1-8.txt",
    "BoulderCO-NCAR/Pandora204s1/L2/Pandora204s1_BoulderCO-NCAR_L2_rfus5p1-8.txt",
    "BoulderCO/Pandora57s1/L2/Pandora57s1_BoulderCO_L2_rfuh5p1-8.txt",
    "BoulderCO/Pandora57s1/L2/Pandora57s1_BoulderCO_L2_rfus5p1-8.txt",
]


def retrieve_test_file(url_path):
    fn = url_path.split("/")[-1]
    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for Pandora PGN test")
        import requests

        r = requests.get(
            f"https://data.ovh.pandonia-global-network.org/{url_path}",
            stream=True,
        )
        r.raise_for_status()
        with open(p, "wb") as f:
            f.write(r.content)
    return p


@pytest.fixture(scope="module")
def pandora_test_files(tmp_path_factory, worker_id):
    if worker_id == "master":
        # Not executing with multiple workers;
        # let pytest's fixture caching do its job
        return [retrieve_test_file(url_path) for url_path in URL_PATHS]

    # Get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # Copy to the shared test location
    p_tests = []
    for url_path in URL_PATHS:
        fn = url_path.split("/")[-1]
        p_test = root_tmp_dir / fn
        with FileLock(p_test.as_posix() + ".lock"):
            if not p_test.is_file():
                p = retrieve_test_file(url_path)
                shutil.copy(p, p_test)
        p_tests.append(p_test)

    return p_tests


def assert_is_valid_xarray(ds):
    assert isinstance(ds, xr.Dataset)
    assert {"latitude", "longitude", "time"}.issubset(set(ds.coords))
    assert np.all((-90 <= ds["latitude"]) & (ds["latitude"] <= 90))
    assert np.all((-180 <= ds["longitude"]) & (ds["longitude"] <= 180))
    assert np.issubdtype(ds["time"].dtype, np.datetime64)
    assert set(ds.dims) <= {"time", "x", "z"}
    assert ds["siteid"].dims == ("x",)

    # Assert dimensions over every variable except for siteid
    data_vars_col = list(ds.data_vars)
    data_vars_col.remove("siteid")
    for v in data_vars_col:
        assert ds[v].dims == ("time", "x") or ds[v].dims == ("time", "x", "z")
        assert np.issubdtype(ds[v].dtype, np.number)
        assert ds[v].attrs.keys() == {"description"}


def test_parse_metadata():
    assert isinstance(pandora._parse_metadata("1"), int)
    assert isinstance(pandora._parse_metadata("1."), float)
    assert isinstance(pandora._parse_metadata("1.1"), float)
    assert isinstance(pandora._parse_metadata("Mock string"), str)
    assert pandora._parse_metadata(" Mock") == "Mock"
    assert pandora._parse_metadata("string ") == "string"
    assert pandora._parse_metadata(" Mock string ") == "Mock string"
    assert pandora._parse_metadata("") == ""
    assert pandora._parse_metadata(" ") == ""
    assert pandora._parse_metadata("     ") == ""
    assert isinstance(pandora._parse_metadata("2019-01-23T00:01:03"), pd.Timestamp)


def test_rename_and_format():
    df = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]))
    df.attrs["_col_descs"] = {"Column 1": "time", "Column 2": "fizz", "Column 3": "buzz"}
    renamed = pandora._rename_and_format(df)
    assert isinstance(renamed, pd.DataFrame)
    assert renamed.index.name == "time"
    # "Column 1" becomes the time index; remaining columns use zero-padded names,
    # but consistent with the column description section (1-based numbering).
    assert "Column 1" not in renamed.columns, "time"
    assert "col1" not in renamed.columns, "time"
    assert "Column 2" not in renamed.columns, "renamed"
    assert "col2" in renamed.columns, "renamed"


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
    pandora._merge_global_attrs(ds1, ds2, merged)
    assert merged.attrs["mock1"] == ["mock_my_data_ds1", "mock_my_data_ds2"]


def test_open_dataset(pandora_test_files, tmp_path):
    # indices 0 and 1: BoulderCO-NCAR files with and without extra columns
    for file_path in pandora_test_files[:2]:
        ds = pandora.open_dataset(file_path)
        assert_is_valid_xarray(ds)
        assert set(ds.dims) == {"time", "x"}

        # Test saving to nc (and roundtrip)
        p = tmp_path / file_path.name
        ds.to_netcdf(p)
        ds2 = xr.open_dataset(p)
        xr.testing.assert_identical(ds, ds2)


def test_open_dataset_profiles(pandora_test_files):
    patt = "rfuh"
    n = 0
    for file_path in pandora_test_files:
        if patt in file_path.name:
            ds = pandora.open_dataset(file_path, layers=True)
            assert_is_valid_xarray(ds)
            assert set(ds.dims) == {"time", "x", "z"}
            assert ds.sizes["z"] > 1, "multiple layers"
            layer_vars = [k for k in ds.data_vars if ds[k].dims == ("time", "x", "z")]
            assert len(layer_vars) > 0
            for v in layer_vars:
                desc = ds[v].attrs["description"]
                assert "layer" in desc and "layer 1" not in desc, "generalized"
            n += 1
    if n == 0:
        raise AssertionError(f"Expected at least one {patt} file")


def test_open_mfdataset(pandora_test_files):
    # indices 0, 2: rfuh5p1-8 (extra columns) from two different sites
    files = [pandora_test_files[0], pandora_test_files[2]]
    ds_std = pandora.open_mfdataset(files)
    assert_is_valid_xarray(ds_std)
    assert ds_std.attrs["history"].count("open_mfdataset") == 1

    ds_lay = pandora.open_mfdataset(files, layers=True)
    assert_is_valid_xarray(ds_lay)
    assert ds_lay.attrs["history"].count("open_mfdataset") == 1
    assert ds_lay.sizes["z"] > 1, "multiple layers"
    assert ds_std.data_vars.keys() == ds_lay.data_vars.keys(), "same variables"

    # indices 1, 3: rfus5p1-8 (standard columns) from two different sites
    files = [pandora_test_files[1], pandora_test_files[3]]
    ds_std = pandora.open_mfdataset(files)
    assert_is_valid_xarray(ds_std)
    assert ds_std.attrs["history"].count("open_mfdataset") == 1


def test_open_mfdataset_no_files():
    with pytest.raises(ValueError, match=r"No files found from input asdf\*\.txt"):
        _ = pandora.open_mfdataset("asdf*.txt")


def test_get_locations():
    df = pandora.get_locations()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert {"name", "long_name", "lat", "lon", "alt", "aliases"}.issubset(df.columns)
    assert df["lat"].between(-90, 90, inclusive="both").all()
    assert df["lon"].between(-180, 180, inclusive="left").all()
    assert df["name"].str.len().gt(0).all()
    assert "BoulderCO" in df["name"].values
    assert "BoulderCO-NCAR" in df["name"].values


def test_get_location_files():
    df = pandora.get_location_files("BoulderCO", ("2024-07-01", "2024-07-31"), prod=None)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "filename" in df.columns
    assert df["filename"].str.len().gt(0).all()
    assert (df["location"] == "BoulderCO").all()
    assert df["pan_id"].notna().all()
    assert df["spectrometer"].isin(["1", "2"]).all()


def test_get_location_files_empty():
    with pytest.warns(UserWarning, match="No files found for BoulderCO"):
        df = pandora.get_location_files("BoulderCO", ("1900-01-01", "1900-01-31"), prod=None)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_location_files_invalid_prod():
    with pytest.raises(RuntimeError, match="Got HTTP error 422"):
        _ = pandora.get_location_files("BoulderCO", ("2024-07-01", "2024-07-31"), prod="asdf")
