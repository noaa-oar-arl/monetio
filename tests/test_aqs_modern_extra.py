import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.aqs import AQSReader


def create_mock_aqs_file(fn):
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


def test_aqs_lazy_no_compute(tmp_path):
    fn = tmp_path / "test_aqs_no_compute.csv"
    create_mock_aqs_file(fn)

    reader = AQSReader()
    dates = pd.to_datetime(["2023-01-01"])

    # Load lazily
    ds = reader.open_dataset(files=str(fn), dates=dates, lazy=True, as_xarray=True, wide_fmt=True)

    # Verify OZONE is present and is a dask-backed array
    assert "OZONE" in ds.data_vars
    assert hasattr(ds.OZONE.data, "dask"), "OZONE data should be dask-backed"

    # Ensure no hidden compute was triggered for the data
    assert not isinstance(ds.OZONE.data, np.ndarray)


def test_aqs_eager_lazy_identity(tmp_path):
    """Aero Protocol: Verify eager and lazy results are identical."""
    fn = tmp_path / "test_aqs_identity.csv"
    create_mock_aqs_file(fn)

    reader = AQSReader()
    dates = pd.to_datetime(["2023-01-01"])

    # Eager path
    ds_eager = reader.open_dataset(
        files=str(fn), dates=dates, lazy=False, as_xarray=True, wide_fmt=True
    )

    # Lazy path
    ds_lazy = reader.open_dataset(
        files=str(fn), dates=dates, lazy=True, as_xarray=True, wide_fmt=True
    )

    # Compute lazy result for comparison
    ds_lazy_computed = ds_lazy.compute()

    # Drop history for comparison
    ds_eager = ds_eager.drop_vars("history", errors="ignore")
    ds_lazy_computed = ds_lazy_computed.drop_vars("history", errors="ignore")

    xr.testing.assert_allclose(ds_eager, ds_lazy_computed)


def test_epa_utils_provenance():
    from monetio.readers.epa_utils import convert_statenames_to_abv, standardize_epa_units

    df = pd.DataFrame({"state_name": ["Alabama"], "obs": [10.0], "units": ["knots"]})

    df = convert_statenames_to_abv(df)
    assert df.state_name.iloc[0] == "AL"
    assert "Converted full state names" in df.attrs.get("history", "")

    df = standardize_epa_units(df)
    assert df.units.iloc[0] == "m/s"
    assert "Standardized units" in df.attrs.get("history", "")


def test_add_monitor_metadata_backend_agnostic(tmp_path):
    """Verify backend-agnostic behavior and fix for timedelta bug."""
    from unittest.mock import patch

    from monetio.readers.epa_utils import add_monitor_metadata

    # Create mock data with GMT offset as integer
    data = {
        "siteid": ["010010001"],
        "time_local": [pd.to_datetime("2023-01-01 12:00")],
        "gmt_offset": [-5],
    }
    df_pandas = pd.DataFrame(data)

    # Mock read_monitor_file to return a minimal valid DF to avoid early return
    mock_monitor = pd.DataFrame({"siteid": ["010010001"], "latitude": [34.0], "longitude": [-86.0]})

    with patch("monetio.readers.epa_utils.read_monitor_file", return_value=mock_monitor):
        # 1. Eager path
        df_eager = add_monitor_metadata(df_pandas.copy(), daily=True)
        # 12:00 local - (-5) offset = 17:00 UTC
        assert "time" in df_eager.columns
        assert df_eager["time"].iloc[0] == pd.to_datetime("2023-01-01 17:00")

        # 2. Lazy path
        import dask.dataframe as dd

        df_dask = dd.from_pandas(df_pandas.copy(), npartitions=1)
        df_lazy = add_monitor_metadata(df_dask, daily=True)

        # Check that it's still lazy and matches eager
        assert isinstance(df_lazy, dd.DataFrame)
        pd.testing.assert_frame_equal(
            df_eager.drop(columns="history", errors="ignore"),
            df_lazy.compute().drop(columns="history", errors="ignore"),
            check_dtype=False,
        )
