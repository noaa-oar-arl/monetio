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


def test_ameriflux_use_dask_uses_bounded_schema_probe(tmp_path, monkeypatch):
    import dask.dataframe as dd

    csv_path = tmp_path / "ameriflux.csv"
    csv_path.write_text(
        "TIMESTAMP_START,CO2,FC\n202301010000,400.1,1.2\n202301010030,400.5,1.3\n",
        encoding="utf-8",
    )
    original = pd.read_csv
    calls = []

    def counted(source, *args, **kwargs):
        calls.append(kwargs.get("nrows"))
        return original(source, *args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", counted)
    reader = AmeriFluxReader()
    eager = reader.open_dataset(str(csv_path), as_xarray=False, use_dask=False)
    calls.clear()
    lazy = reader.open_dataset(str(csv_path), as_xarray=False, use_dask=True)

    assert isinstance(lazy, dd.DataFrame)
    assert calls == [1]
    pd.testing.assert_frame_equal(lazy.compute(), eager, check_dtype=False)
    assert calls == [1, None]


def test_ameriflux_use_dask_xarray_shares_length_preparation(tmp_path, monkeypatch):
    import dask.array as da
    import xarray as xr

    csv_path = tmp_path / "ameriflux.csv"
    csv_path.write_text(
        "TIMESTAMP_START,CO2,FC\n202301010000,400.1,1.2\n202301010030,400.5,1.3\n",
        encoding="utf-8",
    )
    original = pd.read_csv
    calls = []

    def counted(source, *args, **kwargs):
        calls.append(kwargs.get("nrows"))
        return original(source, *args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", counted)
    reader = AmeriFluxReader()
    eager = reader.open_dataset(str(csv_path), as_xarray=True, use_dask=False)
    calls.clear()
    lazy = reader.open_dataset(str(csv_path), as_xarray=True, use_dask=True)

    assert isinstance(lazy, xr.Dataset)
    assert isinstance(lazy.CO2.data, da.Array)
    # The one-row probe establishes schema and the single full preparation
    # pass discovers row count/coordinates. Explicit evaluation reads once more.
    assert calls == [1, None]
    xr.testing.assert_equal(eager.CO2, lazy.CO2.compute())
    assert calls == [1, None, None]
