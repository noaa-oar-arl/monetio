import json

import numpy as np
import pytest
import xarray as xr

from monetio.readers.openaq import OpenAQReader


@pytest.fixture
def sample_openaq_jsonl(tmp_path):
    """Create a sample OpenAQ JSONL file."""
    fn = tmp_path / "openaq_test.jsonl"
    data = [
        {
            "date": {"utc": "2023-01-01T00:00:00Z", "local": "2023-01-01T01:00:00+01:00"},
            "averagingPeriod": {"value": 1, "unit": "hours"},
            "coordinates": {"latitude": 40.0, "longitude": -75.0},
            "parameter": "o3",
            "unit": "µg/m³",
            "value": 19.9,
            "location": "Test Site",
            "country": "US",
        },
        {
            "date": {"utc": "2023-01-01T00:00:00Z", "local": "2023-01-01T01:00:00+01:00"},
            "averagingPeriod": {"value": 1, "unit": "hours"},
            "coordinates": {"latitude": 40.0, "longitude": -75.0},
            "parameter": "pm25",
            "unit": "µg/m³",
            "value": 10.0,
            "location": "Test Site",
            "country": "US",
        },
        {
            "date": {"utc": "2023-01-01T01:00:00Z", "local": "2023-01-01T02:00:00+01:00"},
            "averagingPeriod": {"value": 1, "unit": "hours"},
            "coordinates": {"latitude": 40.0, "longitude": -75.0},
            "parameter": "o3",
            "unit": "µg/m³",
            "value": 25.0,
            "location": "Test Site",
            "country": "US",
        },
    ]
    with open(fn, "w") as f:
        for entry in data:
            f.write(json.dumps(entry) + "\n")
    return str(fn)


def test_openaq_eager_lazy_consistency(sample_openaq_jsonl):
    """Verify that Eager and Lazy modes return identical results for OpenAQ."""
    reader = OpenAQReader()

    # Eager (Pandas -> Xarray)
    ds_eager = reader.open_dataset(files=sample_openaq_jsonl, lazy=False, wide_fmt=True)

    # Lazy (Dask -> Xarray)
    ds_lazy = reader.open_dataset(files=sample_openaq_jsonl, lazy=True, wide_fmt=True)

    # 1. Check types
    # ds_eager variables should be numpy-backed
    assert isinstance(ds_eager.o3_ppm.data, np.ndarray)

    # ds_lazy variables should be dask-backed
    import dask.array as da

    assert isinstance(ds_lazy.o3_ppm.data, da.Array)

    # 2. Check data values (after computing lazy)
    # Note: o3 is converted from µg/m³ to ppm (19.9 / 1990 = 0.01)
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())

    # 3. Verify naming and units
    assert "o3_ppm" in ds_eager.data_vars
    assert "pm25_ugm3" in ds_eager.data_vars
    assert ds_eager.o3_ppm.attrs["units"] == "ppm"
    # Note: _format_units in base.py changes µg/m³ to LaTeX form
    assert r"$\mu g m^{-3}$" in ds_eager.pm25_ugm3.attrs["units"]

    # 4. Check coordinates
    assert "time" in ds_eager.coords
    assert "siteid" in ds_eager.coords
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords


def test_openaq_no_hidden_compute(sample_openaq_jsonl):
    """Ensure that lazy loading does not trigger immediate computes."""
    import dask

    reader = OpenAQReader()

    with dask.config.set(scheduler="single-threaded"):
        ds = reader.open_dataset(files=sample_openaq_jsonl, lazy=True, wide_fmt=True)
        assert hasattr(ds.o3_ppm.data, "dask")


def test_openaq_unit_conversion(sample_openaq_jsonl):
    """Verify unit conversion logic for O3."""
    reader = OpenAQReader()
    ds = reader.open_dataset(files=sample_openaq_jsonl, lazy=False, wide_fmt=True)

    # 19.9 µg/m³ O3 should be 0.01 ppm
    val = ds.o3_ppm.isel(time=0, node=0).values
    assert np.isclose(val, 0.01)
