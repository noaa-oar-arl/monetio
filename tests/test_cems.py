import pandas as pd
import pytest
import xarray as xr

from monetio.readers.cems import CEMSReader


def test_cems_eager_lazy(tmp_path):
    # Create a dummy CEMS CSV file
    # Columns expected: facility name, orispl, date, hour, so2 lbs, nox lbs, co2 short tons, lat, lon, state
    df_data = pd.DataFrame(
        {
            "Facility Name": ["Test Plant"],
            "ORISPL": [1234],
            "Date": ["2023-01-01"],
            "Hour": [0],
            "SO2 (lbs)": [100.0],
            "NOX (lbs)": [50.0],
            "CO2 (short tons)": [10.0],
            "Latitude": [40.0],
            "Longitude": [-100.0],
            "State": ["MD"],
        }
    )

    f = tmp_path / "test_cems.csv"
    df_data.to_csv(f, index=False)

    reader = CEMSReader()

    # Test Eager Mode
    ds_eager = reader.open_dataset(files=[str(f)], as_xarray=True, lazy=False)
    assert isinstance(ds_eager, xr.Dataset)
    assert "so2_lbs" in ds_eager.data_vars
    assert ds_eager.so2_lbs.attrs["units"] == "lbs"
    assert ds_eager.nox_lbs.attrs["units"] == "lbs"
    assert ds_eager.co2_short_tons.attrs["units"] == "short tons"
    assert ds_eager.so2_lbs.values[0] == 100.0

    # Test Lazy Mode
    ds_lazy = reader.open_dataset(files=[str(f)], as_xarray=True, lazy=True)
    assert isinstance(ds_lazy, xr.Dataset)
    assert "so2_lbs" in ds_lazy.data_vars
    assert ds_lazy.so2_lbs.attrs["units"] == "lbs"

    # Assert identical values
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )

    # Check history
    assert "history" in ds_eager.attrs
    assert "Read CEMS data." in ds_eager.attrs["history"]


def test_cems_multi_file(tmp_path):
    df1 = pd.DataFrame(
        {
            "Facility Name": ["Test Plant"],
            "ORISPL": [1234],
            "Date": ["2023-01-01"],
            "Hour": [0],
            "SO2 (lbs)": [100.0],
            "Latitude": [40.0],
            "Longitude": [-100.0],
            "State": ["MD"],
        }
    )
    df2 = pd.DataFrame(
        {
            "Facility Name": ["Test Plant"],
            "ORISPL": [1234],
            "Date": ["2023-01-01"],
            "Hour": [1],
            "SO2 (lbs)": [110.0],
            "Latitude": [40.0],
            "Longitude": [-100.0],
            "State": ["MD"],
        }
    )

    f1 = tmp_path / "test1.csv"
    f2 = tmp_path / "test2.csv"
    df1.to_csv(f1, index=False)
    df2.to_csv(f2, index=False)

    reader = CEMSReader()

    # Eager Multi-file
    ds = reader.open_dataset(files=[str(f1), str(f2)], as_xarray=True, lazy=False)
    assert "so2_lbs" in ds.data_vars
    assert ds.so2_lbs.attrs["units"] == "lbs"
    assert ds.so2_lbs.sizes["time"] == 2

    # Lazy Multi-file
    ds_lazy = reader.open_dataset(files=[str(f1), str(f2)], as_xarray=True, lazy=True)
    assert "so2_lbs" in ds_lazy.data_vars
    assert ds_lazy.so2_lbs.attrs["units"] == "lbs"


if __name__ == "__main__":
    pytest.main([__file__])
