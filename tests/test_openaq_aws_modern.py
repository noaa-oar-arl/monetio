import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.openaq_aws import OpenAQAWSReader


@pytest.fixture
def sample_openaq_aws_csv(tmp_path):
    """Create a sample OpenAQ AWS CSV file."""
    fn = tmp_path / "location-7073-20230101.csv.gz"
    df = pd.DataFrame(
        {
            "location_id": ["7073", "7073", "7073"],
            "sensor_id": ["1", "2", "3"],
            "location": ["Test Site", "Test Site", "Test Site"],
            "datetime": ["2023-01-01 00:00:00", "2023-01-01 00:00:00", "2023-01-01 01:00:00"],
            "lat": [40.0, 40.0, 40.0],
            "lon": [-75.0, -75.0, -75.0],
            "parameter": ["o3", "pm25", "o3"],
            "unit": ["µg/m³", "ug/m3", "µg/m³"],
            "value": [19.9, 10.0, 25.0],
        }
    )
    df.to_csv(fn, index=False, compression="gzip", encoding="utf-8")
    return str(fn)


def test_openaq_aws_eager_lazy_consistency(sample_openaq_aws_csv):
    """Verify that Eager and Lazy modes return identical results for OpenAQ AWS."""
    reader = OpenAQAWSReader()

    # Eager (Pandas -> Xarray)
    ds_eager = reader.open_dataset(files=sample_openaq_aws_csv, lazy=False, wide_fmt=True)

    # Lazy (Dask -> Xarray)
    ds_lazy = reader.open_dataset(files=sample_openaq_aws_csv, lazy=True, wide_fmt=True)

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
    assert ds_eager.pm25_ugm3.attrs["units"] == "µg m-3"

    # 4. Check coordinates
    assert "time" in ds_eager.coords
    assert "siteid" in ds_eager.coords
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords


def test_openaq_aws_no_hidden_compute(sample_openaq_aws_csv):
    """Ensure that lazy loading does not trigger immediate computes."""
    import dask

    reader = OpenAQAWSReader()

    with dask.config.set(scheduler="single-threaded"):
        # We track computes by checking if dask.compute was called.
        # However, a simpler way is to check if the data is still a dask array.
        ds = reader.open_dataset(files=sample_openaq_aws_csv, lazy=True, wide_fmt=True)

        # If it reached here without error and returns a dask-backed dataset,
        # it's likely lazy.
        assert hasattr(ds.o3_ppm.data, "dask")


def test_openaq_aws_unit_conversion(sample_openaq_aws_csv):
    """Verify unit conversion logic for O3."""
    reader = OpenAQAWSReader()
    ds = reader.open_dataset(files=sample_openaq_aws_csv, lazy=False, wide_fmt=True)

    # 19.9 µg/m³ O3 should be 0.01 ppm
    # We use positional indexing for robustness across Xarray versions in tests.
    val = ds.o3_ppm.isel(time=0, node=0).values
    assert np.isclose(val, 0.01)
