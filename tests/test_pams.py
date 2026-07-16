import json

import pytest
import xarray as xr

from monetio.readers.pams import PAMSReader


def test_pams_eager_lazy(tmp_path):
    # Create a dummy PAMS JSON file
    dummy_data = {
        "Data": [
            {
                "state_code": 1,
                "county_code": 1,
                "site_number": 1,
                "date_gmt": "2023-01-01",
                "time_gmt": "00:00",
                "date_local": "2023-01-01",
                "time_local": "01:00",
                "sample_measurement": 10.0,
                "units_of_measure": "Parts per billion",
                "latitude": 40.0,
                "longitude": -100.0,
                "parameter": "Ozone",
            }
        ]
    }

    f = tmp_path / "test.json"
    with open(f, "w") as out:
        json.dump(dummy_data, out)

    reader = PAMSReader()

    # Test Eager Mode
    # By default, open_dataset pivots PAMS data (parameters become variables)
    ds_eager = reader.open_dataset(files=[str(f)], as_xarray=True, lazy=False)
    assert isinstance(ds_eager, xr.Dataset)
    assert "Ozone" in ds_eager.data_vars
    assert ds_eager.Ozone.values[0] == 10.0
    # siteid is renamed to node during 2D expansion, but preserved as siteid coord
    assert ds_eager.coords["siteid"].values[0] == "010010001"

    # Test Lazy Mode
    ds_lazy = reader.open_dataset(files=[str(f)], as_xarray=True, lazy=True)
    assert isinstance(ds_lazy, xr.Dataset)

    # Assert identical values
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )

    # Check history
    assert "history" in ds_eager.attrs
    assert "Read PAMS data." in ds_eager.attrs["history"]

    # Check column renaming and units
    assert "Ozone" in ds_eager.data_vars
    assert ds_eager.Ozone.attrs["units"] == "ppb"


def test_pams_multi_file(tmp_path):
    # Create two dummy PAMS JSON files
    data1 = {
        "Data": [
            {
                "state_code": 1,
                "county_code": 1,
                "site_number": 1,
                "date_gmt": "2023-01-01",
                "time_gmt": "00:00",
                "sample_measurement": 10.0,
                "units_of_measure": "Parts per billion",
                "latitude": 40.0,
                "longitude": -100.0,
                "parameter": "Ozone",
            }
        ]
    }
    data2 = {
        "Data": [
            {
                "state_code": 1,
                "county_code": 1,
                "site_number": 1,
                "date_gmt": "2023-01-01",
                "time_gmt": "01:00",
                "sample_measurement": 20.0,
                "units_of_measure": "Parts per billion",
                "latitude": 40.0,
                "longitude": -100.0,
                "parameter": "Ozone",
            }
        ]
    }

    f1 = tmp_path / "test1.json"
    f2 = tmp_path / "test2.json"
    with open(f1, "w") as out:
        json.dump(data1, out)
    with open(f2, "w") as out:
        json.dump(data2, out)

    reader = PAMSReader()

    # Test Eager Multi-file (pd.concat might drop attrs)
    ds = reader.open_dataset(files=[str(f1), str(f2)], as_xarray=True, lazy=False)
    assert "Ozone" in ds.data_vars
    assert ds.Ozone.attrs["units"] == "ppb"
    assert ds.Ozone.sizes["time"] == 2

    # Test Lazy Multi-file
    ds_lazy = reader.open_dataset(files=[str(f1), str(f2)], as_xarray=True, lazy=True)
    assert "Ozone" in ds_lazy.data_vars
    assert ds_lazy.Ozone.attrs["units"] == "ppb"


if __name__ == "__main__":
    pytest.main([__file__])
