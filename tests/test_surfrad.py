import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.surfrad import SURFRADReader


def test_surfrad_reader_basic(tmp_path):
    # Create a mock SURFRAD file
    mock_content = """Bondville, IL
40.05192 -88.37309 213 1
2024 1 1 1 0 0 0.000 0.00 -9999.9 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 0.00 0 25.0 0 50.0 0 5.0 0 180.0 0 1013.2 0
2024 1 1 1 0 1 0.017 0.00 100.0 0 10.0 0 50.0 0 20.0 0 300.0 0 290.0 0 290.0 0 310.0 0 295.0 0 295.0 0 0.50 0 10.0 0 90.0 0 -10.0 0 80.0 0 25.1 0 50.1 0 5.1 0 181.0 0 1013.3 0
"""
    f = tmp_path / "bon24001.dat"
    f.write_text(mock_content)

    reader = SURFRADReader()

    # Test Eager
    df = reader.open_dataset(files=str(f), as_xarray=False)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df["air_temperature"].iloc[0] == 25.0
    assert df["siteid"].iloc[0] == "Bondville, IL"
    assert df["latitude"].iloc[0] == 40.05192
    assert df["longitude"].iloc[0] == -88.37309
    assert df["elevation"].iloc[0] == 213.0
    assert df["time"].iloc[0] == pd.Timestamp("2024-01-01 00:00:00")
    assert df["time"].iloc[1] == pd.Timestamp("2024-01-01 00:01:00")
    # Check NaN handling
    assert np.isnan(df["ghi"].iloc[0])
    assert df["ghi"].iloc[1] == 100.0

    # Test Lazy (if dask is available, PointReader handles it via driver)
    # By default it expands to 2D (time, node)
    ds = reader.open_dataset(files=str(f), as_xarray=True, lazy=True)
    assert isinstance(ds, xr.Dataset)
    assert "air_temperature" in ds.data_vars
    assert "time" in ds.coords
    # 1 site -> node size 1; 2 time steps -> time size 2
    assert ds.sizes["node"] == 1
    assert ds.sizes["time"] == 2

    # Test without 2D expansion
    ds = reader.open_dataset(files=str(f), as_xarray=True, expand2d=False)
    assert ds.sizes["time"] == 2
    assert "time" in ds.coords

    # Verify coordinates
    assert ds.latitude.attrs["units"] == "degrees_north"
    assert ds.longitude.attrs["units"] == "degrees_east"
    assert ds.elevation.attrs["units"] == "m"


def test_surfrad_build_urls():
    reader = SURFRADReader()
    dates = pd.to_datetime(["2024-01-01"])
    sites = ["bon", "tbl"]
    urls = reader.build_urls(dates, sites)

    assert len(urls) == 2
    assert "https://gml.noaa.gov/aftp/data/radiation/surfrad/Bondville_IL/2024/bon24001.dat" in urls
    assert (
        "https://gml.noaa.gov/aftp/data/radiation/surfrad/Table_Mountain_CO/2024/tbl24001.dat"
        in urls
    )
