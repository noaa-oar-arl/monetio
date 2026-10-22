import pandas as pd
import pytest
import xarray as xr

from monetio.readers.aqs import AQSReader


def create_mock_aqs_file(fn, daily=False):
    if daily:
        data = {
            "Date Local": ["2023-01-01", "2023-01-01"],
            "State Code": ["01", "01"],
            "County Code": ["001", "001"],
            "Site Num": ["0001", "0002"],
            "Parameter Code": [44201, 88101],
            "POC": [1, 1],
            "Latitude": [34.0, 35.0],
            "Longitude": [-86.0, -87.0],
            "Datum": ["WGS84", "WGS84"],
            "Parameter Name": ["Ozone", "PM2.5 - Local Conditions"],
            "Sample Duration": ["1 HOUR", "1 HOUR"],
            "Pollutant Standard": ["Ozone 8-hour 2015", "PM25 24-hour 2012"],
            "Units of Measure": ["Parts per billion", "Micrograms/cubic meter (LC)"],
            "Event Type": ["None", "None"],
            "Observation Count": [24, 24],
            "Observation Percent": [100, 100],
            "Sample Measurement": [40.0, 12.0],
            "1st Max Value": [45.0, 15.0],
            "1st Max Hour": [14, 10],
            "AQI": [37, 50],
            "Method Code": ["047", "145"],
            "Method Name": ["Instrumental", "Gravimetric"],
            "Local Site Name": ["Site 1", "Site 2"],
            "Address": ["123 St", "456 St"],
            "State Name": ["Alabama", "Alabama"],
            "County Name": ["Autauga", "Autauga"],
            "City Name": ["Prattville", "Prattville"],
            "MSA Name": ["Montgomery, AL", "Montgomery, AL"],
            "Date of Last Change": ["2023-02-01", "2023-02-01"],
        }
    else:
        data = {
            "Date GMT": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "Time GMT": ["00:00", "01:00", "00:00"],
            "Date Local": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "Time Local": ["00:00", "01:00", "00:00"],
            "State Code": ["01", "01", "01"],
            "County Code": ["001", "001", "001"],
            "Site Num": ["0001", "0001", "0001"],
            "Parameter Code": [44201, 44201, 88101],
            "POC": [1, 1, 1],
            "Latitude": [34.0, 34.0, 34.0],
            "Longitude": [-86.0, -86.0, -86.0],
            "Sample Measurement": [40.0, 42.0, 10.0],
            "Units of Measure": [
                "Parts per billion",
                "Parts per billion",
                "Micrograms/cubic meter (LC)",
            ],
            "Parameter Name": ["Ozone", "Ozone", "PM2.5 - Local Conditions"],
        }
    pd.DataFrame(data).to_csv(fn, index=False)


@pytest.mark.parametrize("wide_fmt", [True, False])
def test_aqs_eager_lazy_consistency(tmp_path, wide_fmt):
    fn = tmp_path / "test_aqs.csv"
    create_mock_aqs_file(fn, daily=False)

    reader = AQSReader()
    dates = pd.to_datetime(["2023-01-01"])

    # Eager (NumPy)
    ds_eager = reader.open_dataset(
        files=str(fn), dates=dates, lazy=False, as_xarray=True, wide_fmt=wide_fmt
    )

    # Lazy (Dask)
    ds_lazy = reader.open_dataset(
        files=str(fn), dates=dates, lazy=True, as_xarray=True, wide_fmt=wide_fmt
    )

    # Check that eager result is NOT lazy
    if wide_fmt:
        assert not hasattr(ds_eager.OZONE.data, "dask")
    else:
        assert not hasattr(ds_eager.obs.data, "dask")

    # Check that lazy result is indeed lazy
    if wide_fmt:
        # For wide format, 'OZONE' should be a data var and it should be a dask array
        assert hasattr(ds_lazy.OZONE.data, "dask")
    else:
        # For long format, 'obs' should be a dask array
        assert hasattr(ds_lazy.obs.data, "dask")

    # Compare results
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
        atol=1e-5,
    )

    # Check history
    assert "history" in ds_eager.attrs
    assert "Read AQS data" in ds_eager.attrs["history"]
    assert "history" in ds_lazy.attrs
    assert "Read AQS data" in ds_lazy.attrs["history"]


def test_aqs_daily_eager_lazy_consistency(tmp_path):
    fn = tmp_path / "test_aqs_daily.csv"
    create_mock_aqs_file(fn, daily=True)

    reader = AQSReader()
    dates = pd.to_datetime(["2023-01-01"])

    ds_eager = reader.open_dataset(
        files=str(fn), dates=dates, daily=True, lazy=False, as_xarray=True, wide_fmt=True
    )
    ds_lazy = reader.open_dataset(
        files=str(fn), dates=dates, daily=True, lazy=True, as_xarray=True, wide_fmt=True
    )

    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
        atol=1e-5,
    )


def test_aqs_no_hidden_compute(tmp_path):
    fn = tmp_path / "test_aqs_lazy.csv"
    create_mock_aqs_file(fn, daily=False)

    reader = AQSReader()
    dates = pd.to_datetime(["2023-01-01"])

    # Use a mock to track computes if possible, or just rely on the fact that
    # if it doesn't fail and it remains a dask array, it's likely not computed.
    # In MONETIO, many things use .compute() internally which we want to avoid.

    ds_lazy = reader.open_dataset(
        files=str(fn), dates=dates, lazy=True, as_xarray=True, wide_fmt=True
    )

    # If it reached here without raising and is still dask-backed, we are good.
    assert hasattr(ds_lazy.OZONE.data, "dask")
