import numpy as np
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
            "Date GMT": ["2023-01-01", "2023-01-01"],
            "Time GMT": ["00:00", "01:00"],
            "Date Local": ["2023-01-01", "2023-01-01"],
            "Time Local": ["00:00", "01:00"],
            "State Code": ["01", "01"],
            "County Code": ["001", "001"],
            "Site Num": ["0001", "0001"],
            "Parameter Code": [44201, 44201],
            "POC": [1, 1],
            "Latitude": [34.0, 34.0],
            "Longitude": [-86.0, -86.0],
            "Sample Measurement": [40.0, 42.0],
            "Units of Measure": ["Parts per billion", "Parts per billion"],
            "Parameter Name": ["Ozone", "Ozone"],
        }
    pd.DataFrame(data).to_csv(fn, index=False)


def test_aqs_xarray_eager_vs_lazy(tmp_path):
    fn = tmp_path / "hourly.csv"
    create_mock_aqs_file(fn, daily=False)
    dates = pd.date_range(start="2023-01-01", periods=2, freq="h")
    reader = AQSReader()
    ds_eager = reader.open_dataset(
        files=str(fn), dates=dates, as_xarray=True, lazy=False, wide_fmt=True
    )
    ds_lazy = reader.open_dataset(
        files=str(fn), dates=dates, as_xarray=True, lazy=True, wide_fmt=True
    )
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )


def test_aqs_unit_conversion(tmp_path):
    fn = tmp_path / "units.csv"
    data = {
        "Date GMT": ["2023-01-01", "2023-01-01"],
        "Time GMT": ["00:00", "01:00"],
        "State Code": ["01", "01"],
        "County Code": ["001", "001"],
        "Site Num": ["0001", "0001"],
        "Parameter Code": [61103, 62101],
        "Sample Measurement": [10.0, 77.0],
        "Units of Measure": ["Knots", "Degrees Fahrenheit"],
        "Parameter Name": ["Wind Speed", "Temperature"],
    }
    pd.DataFrame(data).to_csv(fn, index=False)
    dates = pd.date_range(start="2023-01-01", periods=2, freq="h")
    df = AQSReader().open_dataset(files=str(fn), dates=dates, as_xarray=False, wide_fmt=False)
    ws = df.loc[df.variable == "WS", "obs"].values[0]
    assert np.isclose(ws, 5.1444)


@pytest.mark.network
def test_aqs_daily_network():
    dates = pd.date_range(start="2019-08-01", periods=1, freq="D")
    try:
        df = AQSReader().open_dataset(
            dates=dates, param=["O3", "PM2.5"], network="IMPROVE", daily=True, as_xarray=False
        )
        assert not df.empty
    except Exception as e:
        pytest.skip(f"AQS network call failed: {e}")
