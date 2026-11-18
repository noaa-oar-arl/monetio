import json

import pandas as pd
import pytest
import xarray as xr

from monetio.readers.pams import PAMSReader


def create_mock_pams_json(filepath, parameter="Ozone", units="Parts per billion"):
    data = {
        "Data": [
            {
                "state_code": "06",
                "county_code": "037",
                "site_number": "1103",
                "parameter": parameter,
                "sample_measurement": 42.0,
                "units_of_measure": units,
                "date_gmt": "2023-01-01",
                "time_gmt": "12:00",
                "latitude": 34.05,
                "longitude": -118.24,
            },
            {
                "state_code": "06",
                "county_code": "037",
                "site_number": "1103",
                "parameter": parameter,
                "sample_measurement": 43.0,
                "units_of_measure": units,
                "date_gmt": "2023-01-01",
                "time_gmt": "13:00",
                "latitude": 34.05,
                "longitude": -118.24,
            },
        ]
    }
    with open(filepath, "w") as f:
        json.dump(data, f)


@pytest.fixture
def mock_pams_files(tmp_path):
    f1 = tmp_path / "pams_1.json"
    create_mock_pams_json(f1, parameter="Ozone", units="Parts per billion")
    return str(f1)


def test_pams_eager_lazy_consistency(mock_pams_files):
    reader = PAMSReader()

    # Eager (Pandas)
    ds_eager = reader.open_dataset(mock_pams_files, as_xarray=True, lazy=False)

    # Lazy (Dask)
    ds_lazy = reader.open_dataset(mock_pams_files, as_xarray=True, lazy=True)

    # Check if lazy is actually lazy (has dask graph)
    assert hasattr(ds_lazy.Ozone.data, "dask")

    # Consistency check
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())

    # Verify units are propagated correctly in both
    assert ds_eager.Ozone.attrs["units"] == "ppb"
    assert ds_lazy.Ozone.attrs["units"] == "ppb"


@pytest.mark.parametrize("lazy_option", [{"lazy": True}, {"use_dask": True}])
def test_pams_lazy_output_preserves_dask_arrays(mock_pams_files, lazy_option):
    reader = PAMSReader()

    ds_lazy = reader.open_dataset(mock_pams_files, as_xarray=True, **lazy_option)
    # But it should still be "lazy" for the actual data.
    assert all(hasattr(var.data, "dask") for var in ds_lazy.data_vars.values() if var.ndim)


def test_pams_lazy_table_output_preserves_dask_dataframe(mock_pams_files):
    import dask.dataframe as dd

    reader = PAMSReader()
    eager = reader.open_dataset(mock_pams_files, as_xarray=False)
    lazy = reader.open_dataset(mock_pams_files, as_xarray=False, use_dask=True)

    assert isinstance(lazy, dd.DataFrame)
    pd.testing.assert_frame_equal(eager, lazy.compute(), check_dtype=False)


def test_pams_reuses_first_file_parse_for_schema_and_units(mock_pams_files, monkeypatch):
    import monetio.readers.pams as pams_module

    original = pams_module.read_pams
    calls = []

    def counted(path, **kwargs):
        calls.append(path)
        return original(path, **kwargs)

    monkeypatch.setattr(pams_module, "read_pams", counted)
    ds = PAMSReader().open_dataset(mock_pams_files, as_xarray=True, use_dask=True)

    assert all(hasattr(var.data, "dask") for var in ds.data_vars.values() if var.ndim)
    # One full parse supplies the explicit schema and units. The Xarray point
    # index preparation performs one additional full parse to learn lengths;
    # it does not repeat per coordinate or let Dask infer schema from a read.
    assert calls.count(mock_pams_files) == 2
