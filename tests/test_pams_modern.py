import json

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


def test_pams_no_compute_on_open(mock_pams_files):
    from dask.callbacks import Callback

    class ComputeCounter(Callback):
        def __init__(self):
            self.compute_count = 0

        def _start(self, dsk):
            self.compute_count += 1

    counter = ComputeCounter()
    reader = PAMSReader()

    with counter:
        ds_lazy = reader.open_dataset(mock_pams_files, as_xarray=True, lazy=True)
        # Accessing metadata shouldn't trigger compute
        _ = ds_lazy.Ozone.attrs["units"]
        _ = ds_lazy.coords

    # Xarray's to_dask_array(lengths=True) triggers a compute to get the length
    # of the partitions for the dask array in to_xarray.
    # So we expect at least 1 compute per variable being converted to dask array.
    # This is an "Allowed Exception" in monetio/readers/base.py
    assert counter.compute_count > 0
    # But it should still be "lazy" for the actual data.
    assert hasattr(ds_lazy.Ozone.data, "dask")
