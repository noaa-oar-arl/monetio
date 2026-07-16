"""
Test AmeriFlux Reader
"""

import pandas as pd

from monetio.readers.ameriflux import AmeriFluxReader


def create_mock_ameriflux_df():
    """Create a mock AmeriFlux BASE dataframe."""
    data = {
        "TIMESTAMP_START": ["202301010000", "202301010030", "202301010100"],
        "CO2": [400.1, 400.5, 400.3],
        "FC": [1.2, 1.3, 1.1],
    }
    return pd.DataFrame(data)


def test_ameriflux_reader_logic(monkeypatch):
    """Test AmeriFlux reader preprocessing logic."""
    mock_df = create_mock_ameriflux_df()

    def mock_open(*args, **kwargs):
        # We need to return what PandasDriver.open returns
        # For simplicity, we mock the harmonize call in PointReader or just return the df
        return mock_df

    # Mocking the driver's open method
    monkeypatch.setattr("monetio.readers.drivers.PandasDriver.open", mock_open)

    reader = AmeriFluxReader()
    # PointReader.open_dataset calls driver.open, then harmonize, then to_xarray
    ds = reader.open_dataset(files="dummy.csv")

    assert "time" in ds.coords
    assert ds.time.values[0] == pd.Timestamp("2023-01-01 00:00:00")
    assert "CO2" in ds.data_vars
    assert "FC" in ds.data_vars
