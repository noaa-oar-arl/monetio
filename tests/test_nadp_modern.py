from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.nadp import NADPReader


@pytest.fixture
def mock_meta():
    """Create dummy metadata."""
    df = pd.DataFrame(
        {"siteid": ["TX01"], "latitude": [30.0], "longitude": [-100.0], "elevation": [100.0]}
    )
    return df


def test_nadp_modern_eager(tmp_path, mock_meta):
    """Test NADP reader with Eager (Pandas) backend."""
    # Create dummy NTN data
    df_ntn = pd.DataFrame(
        {
            "siteid": ["TX01", "TX01"],
            "network": ["NTN", "NTN"],
            "dateon": ["2023-01-01", "2023-01-08"],
            "dateoff": ["2023-01-08", "2023-01-15"],
            "mg": [1.0, -1.0],  # One valid, one negative
            "flagmg": [" ", " "],
            "so4": [3.0, 4.0],
            "flagso4": [" ", "<"],  # Flagged
        }
    )
    fn = tmp_path / "NTN-All-w.csv"
    df_ntn.to_csv(fn, index=False)

    reader = NADPReader()

    original_read_csv = pd.read_csv

    def side_effect(arg, **kwargs):
        if isinstance(arg, str) and arg.startswith("http"):
            return mock_meta
        return original_read_csv(arg, **kwargs)

    with patch("pandas.read_csv", side_effect=side_effect):
        # Eager Path
        ds = reader.open_dataset(files=str(fn), network="NTN", as_xarray=True, lazy=False)

    assert isinstance(ds, xr.Dataset)
    # Check cleaning
    mg = ds.mg.values.flatten()
    assert mg[0] == 1.0
    assert np.isnan(mg[1])  # Negative masked

    so4 = ds.so4.values.flatten()
    assert so4[0] == 3.0
    assert np.isnan(so4[1])  # Flagged '<' masked


def test_nadp_modern_lazy(tmp_path, mock_meta):
    """Test NADP reader with Lazy (Dask) backend."""
    pytest.importorskip("dask")
    # Create dummy NTN data
    df_ntn = pd.DataFrame(
        {
            "siteid": ["TX01", "TX01"],
            "network": ["NTN", "NTN"],
            "dateon": ["2023-01-01", "2023-01-08"],
            "dateoff": ["2023-01-08", "2023-01-15"],
            "mg": [1.0, -1.0],
            "flagmg": [" ", " "],
            "so4": [3.0, 4.0],
            "flagso4": [" ", "<"],
        }
    )
    fn = tmp_path / "NTN-All-w.csv"
    df_ntn.to_csv(fn, index=False)

    reader = NADPReader()

    original_read_csv = pd.read_csv

    def side_effect(arg, **kwargs):
        if isinstance(arg, str) and arg.startswith("http"):
            return mock_meta
        return original_read_csv(arg, **kwargs)

    with patch("pandas.read_csv", side_effect=side_effect):
        # Lazy Path
        ds = reader.open_dataset(files=str(fn), network="NTN", as_xarray=True, lazy=True)

    assert isinstance(ds, xr.Dataset)
    assert ds.mg.chunks is not None

    ds_computed = ds.compute()

    # Check cleaning
    mg = ds_computed.mg.values.flatten()
    assert mg[0] == 1.0
    assert np.isnan(mg[1])

    so4 = ds_computed.so4.values.flatten()
    assert so4[0] == 3.0
    assert np.isnan(so4[1])


def test_nadp_mdn_cleaning(tmp_path, mock_meta):
    """Test MDN global flag cleaning."""
    df_mdn = pd.DataFrame(
        {
            "siteid": ["TX01", "TX01"],
            "dateon": ["2023-01-01", "2023-01-08"],
            "dateoff": ["2023-01-08", "2023-01-15"],
            "qr": [" ", "C"],
            "hgconc": [10.0, 20.0],
        }
    )
    fn = tmp_path / "MDN-All-w.csv"
    df_mdn.to_csv(fn, index=False)

    reader = NADPReader()

    original_read_csv = pd.read_csv

    def side_effect(arg, **kwargs):
        if isinstance(arg, str) and arg.startswith("http"):
            return mock_meta
        return original_read_csv(arg, **kwargs)

    with patch("pandas.read_csv", side_effect=side_effect):
        ds = reader.open_dataset(files=str(fn), network="MDN", as_xarray=True, lazy=False)

    hg = ds.hgconc.values.flatten()
    assert hg[0] == 10.0
    assert np.isnan(hg[1])  # Flagged 'C' masked
