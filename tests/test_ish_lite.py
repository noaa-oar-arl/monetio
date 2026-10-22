import gzip
from unittest.mock import patch

import pandas as pd
import pytest
import xarray as xr

from monetio.readers.ish_lite import ISHLiteReader, read_ish_lite_file

# The ISD history frame comes from the shared ``ish_history`` fixture in
# conftest.py, which is byte-identical to the ``ish`` reader's copy.


def test_read_ish_lite_file_logic(tmp_path):
    # year month day hour temp dew_pt_temp press wdir ws sky_condition precip_1hr precip_6hr
    line1 = "2020 09 01 00  256  184 10185  220   40 0 0 0"
    fn = tmp_path / "722244-00358-2020.gz"
    with gzip.open(fn, "wb") as f:
        f.write((line1 + "\n").encode())

    df = read_ish_lite_file(str(fn))
    assert len(df) == 1
    assert df.time.iloc[0] == pd.Timestamp("2020-09-01 00:00:00")
    assert df.temp.iloc[0] == 25.6
    assert df.siteid.iloc[0] == "72224400358"


def test_ish_lite_eager_lazy_consistency(tmp_path, ish_history):
    """Verify that Eager (Pandas) and Lazy (Dask) backends produce identical results."""
    line1 = "2020 09 01 00  256  184 10185  220   40 0 0 0"
    fn = tmp_path / "722244-00358-2020.gz"
    with gzip.open(fn, "wb") as f:
        f.write((line1 + "\n").encode())

    reader = ISHLiteReader()

    def side_effect(self, dates=None):
        self.history = ish_history

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        # 1. Eager (NumPy/Pandas)
        ds_eager = reader.open_dataset(files=str(fn), as_xarray=True, lazy=False, resample=False)
        table_eager = reader.open_dataset(
            files=str(fn), as_xarray=False, lazy=False, resample=False
        )
        # 2. Lazy (Dask)
        ds_lazy = reader.open_dataset(files=str(fn), as_xarray=True, lazy=True, resample=False)
        df_lazy = reader.open_dataset(files=str(fn), as_xarray=False, lazy=True, resample=False)
        with pytest.warns(UserWarning, match="Resampling is currently not supported"):
            df_alias = reader.open_dataset(
                files=str(fn), as_xarray=False, use_dask=True, resample=True
            )

    # Check values
    assert isinstance(table_eager, pd.DataFrame)
    assert len(table_eager) == 1
    assert all(var.chunks is None for var in ds_eager.data_vars.values())
    assert all(var.chunks is not None for var in ds_lazy.data_vars.values() if var.dims)
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    assert ds_eager.state.values[0] == "MD"
    assert "history" in ds_eager.attrs
    assert "Read ISH Lite data" in ds_eager.attrs["history"]
    import dask.dataframe as dd

    assert isinstance(df_lazy, dd.DataFrame)
    pd.testing.assert_frame_equal(table_eager, df_lazy.compute())
    assert isinstance(df_alias, dd.DataFrame)


def test_ish_lite_resampling_logic(tmp_path, ish_history):
    # Create 3 hours of data
    line1 = "2020 09 01 00  200  100 10000  220   40 0 0 0"
    line2 = "2020 09 01 01  210  110 10010  220   40 0 0 0"
    line3 = "2020 09 01 02  220  120 10020  220   40 0 0 0"

    fn = tmp_path / "722244-00358-2020.gz"
    with gzip.open(fn, "wb") as f:
        f.write((line1 + "\n").encode())
        f.write((line2 + "\n").encode())
        f.write((line3 + "\n").encode())

    reader = ISHLiteReader()

    def side_effect(self, dates=None):
        self.history = ish_history

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        # Resample to 3h
        ds = reader.open_dataset(
            files=str(fn), as_xarray=True, lazy=False, resample=True, window="3h"
        )

    assert len(ds.time) == 1
    assert ds.t2m.values[0] == 21.0  # Average of 20, 21, 22
    assert ds.press.values[0] == 1001.0
    assert "Resampled ISH Lite data" in ds.attrs["history"]


def test_ish_lite_metadata_merging_robustness(tmp_path, ish_history):
    """Test that metadata merging handles missing sites gracefully."""
    line1 = "2020 09 01 00  256  184 10185  220   40 0 0 0"
    line2 = "2020 09 01 00  100   50 10000  180   30 0 0 0"

    fn1 = tmp_path / "722244-00358-2020.gz"
    fn2 = tmp_path / "888888-99999-2020.gz"

    with gzip.open(fn1, "wb") as f:
        f.write((line1 + "\n").encode())
    with gzip.open(fn2, "wb") as f:
        f.write((line2 + "\n").encode())

    reader = ISHLiteReader()

    def side_effect(self, dates=None):
        self.history = ish_history

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        ds = reader.open_dataset(
            files=[str(fn1), str(fn2)], as_xarray=True, lazy=False, resample=False
        )

    # One site is dropped because it lacks lat/lon metadata (not in ish_history)
    assert len(ds.node) == 1
    assert ds.siteid.values[0] == "72224400358"
