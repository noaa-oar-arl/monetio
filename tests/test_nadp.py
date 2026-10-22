from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.nadp import NADPReader


@pytest.fixture
def mock_ntn_data():
    """Create dummy NTN data."""
    df = pd.DataFrame(
        {
            "siteid": ["TX01", "TX01"],
            "network": ["NTN", "NTN"],
            "dateon": ["2023-01-01", "2023-01-08"],
            "dateoff": ["2023-01-08", "2023-01-15"],
            "mg": [1.0, 2.0],
            "flagmg": [" ", "<"],
            "so4": [3.0, 4.0],
            "flagso4": [" ", " "],
        }
    )
    return df


@pytest.fixture
def mock_meta():
    """Create dummy metadata."""
    df = pd.DataFrame(
        {"siteid": ["TX01"], "latitude": [30.0], "longitude": [-100.0], "elevation": [100.0]}
    )
    return df


def test_nadp_ntn_eager(tmp_path, mock_ntn_data, mock_meta):
    """Test NTN reader in eager mode."""
    fn = tmp_path / "NTN-All-w.csv"
    mock_ntn_data.to_csv(fn, index=False)

    reader = NADPReader()

    original_read_csv = pd.read_csv

    def side_effect(arg, **kwargs):
        if isinstance(arg, str) and arg.startswith("http"):
            return mock_meta
        return original_read_csv(arg, **kwargs)

    with patch("pandas.read_csv", side_effect=side_effect):
        ds = reader.open_dataset(files=str(fn), network="NTN", as_xarray=True, lazy=False)

    assert isinstance(ds, xr.Dataset)
    assert "mg" in ds.data_vars
    assert "node" in ds.dims
    assert "time" in ds.dims
    assert ds.sizes["node"] == 1
    assert ds.sizes["time"] == 2

    # Verify cleaning: flagmg '<' should lead to NaN
    # Use where to select siteid since it might not be indexed in all xarray versions
    mg_vals = ds.mg.where(ds.siteid == "TX01", drop=True).values.flatten()
    assert 1.0 in mg_vals
    assert np.isnan(mg_vals).any()
    assert ds.latitude.values[0] == 30.0
    # Verify provenance
    assert "history" in ds.attrs
    assert "Merged with NADP (NTN) station metadata" in ds.attrs["history"]


def test_nadp_ntn_lazy(tmp_path, mock_ntn_data, mock_meta):
    """Test NTN reader in lazy mode."""
    pytest.importorskip("dask")
    fn = tmp_path / "NTN-All-w.csv"
    mock_ntn_data.to_csv(fn, index=False)

    reader = NADPReader()

    original_read_csv = pd.read_csv

    def side_effect(arg, **kwargs):
        if isinstance(arg, str) and arg.startswith("http"):
            return mock_meta
        return original_read_csv(arg, **kwargs)

    with patch("pandas.read_csv", side_effect=side_effect):
        ds = reader.open_dataset(files=str(fn), network="NTN", as_xarray=True, lazy=True)

    assert isinstance(ds, xr.Dataset)
    assert ds.mg.chunks is not None

    ds_computed = ds.compute()
    mg_vals = ds_computed.mg.where(ds_computed.siteid == "TX01", drop=True).values.flatten()
    assert 1.0 in mg_vals
    assert np.isnan(mg_vals).any()
    assert ds_computed.latitude.values[0] == 30.0


def test_nadp_mdn_eager(tmp_path, mock_meta):
    """Test MDN reader cleaning logic."""
    df_mdn = pd.DataFrame(
        {
            "siteid": ["TX01"],
            "network": ["MDN"],
            "dateon": ["2023-01-01"],
            "dateoff": ["2023-01-08"],
            "qr": ["C"],
            "hgconc": [10.0],
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

    # QR='C' should set hgconc to NaN
    assert np.isnan(ds.hgconc.values).all()


def test_nadp_amon_eager(tmp_path, mock_meta):
    """Test AMoN reader cleaning logic."""
    df_amon = pd.DataFrame(
        {
            "siteid": ["TX01"],
            "network": ["AMoN"],
            "startdate": ["2023-01-01"],
            "enddate": ["2023-01-15"],
            "qr": ["C"],
            "conc": [5.0],
        }
    )
    fn = tmp_path / "all-ave.csv"
    df_amon.to_csv(fn, index=False)

    reader = NADPReader()

    original_read_csv = pd.read_csv

    def side_effect(arg, **kwargs):
        if isinstance(arg, str) and arg.startswith("http"):
            return mock_meta
        return original_read_csv(arg, **kwargs)

    with patch("pandas.read_csv", side_effect=side_effect):
        ds = reader.open_dataset(files=str(fn), network="amon", as_xarray=True, lazy=False)

    assert np.isnan(ds.conc.values).all()
