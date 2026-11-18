import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.nadp import NADPReader

# Station metadata and the offline read_csv patch come from the shared
# ``nadp_meta`` / ``patch_remote_read_csv`` fixtures in conftest.py (the NADP
# reader fetches station metadata over the network; both are patched out here).


def _ntn_frame():
    return pd.DataFrame(
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


@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
def test_nadp_ntn_cleaning(tmp_path, nadp_meta, patch_remote_read_csv, lazy):
    """NTN negative-value and '<' flag masking is identical eager vs lazy."""
    if lazy:
        pytest.importorskip("dask")

    fn = tmp_path / "NTN-All-w.csv"
    _ntn_frame().to_csv(fn, index=False)

    with patch_remote_read_csv(nadp_meta):
        ds = NADPReader().open_dataset(files=str(fn), network="NTN", as_xarray=True, lazy=lazy)

    assert isinstance(ds, xr.Dataset)
    if lazy:
        assert ds.mg.chunks is not None
    else:
        assert ds.mg.chunks is None

    ds = ds.compute()
    # Negative mg and flagged '<' so4 are both masked to NaN.
    mg = ds.mg.values.flatten()
    assert mg[0] == 1.0
    assert np.isnan(mg[1])
    so4 = ds.so4.values.flatten()
    assert so4[0] == 3.0
    assert np.isnan(so4[1])


def test_nadp_ntn_native_table_eager_lazy_parity(tmp_path, nadp_meta, patch_remote_read_csv):
    pytest.importorskip("dask")
    fn = tmp_path / "NTN-All-w.csv"
    _ntn_frame().to_csv(fn, index=False)

    with patch_remote_read_csv(nadp_meta):
        eager = NADPReader().open_dataset(files=str(fn), network="NTN", as_xarray=False, lazy=False)
        lazy = NADPReader().open_dataset(files=str(fn), network="NTN", as_xarray=False, lazy=True)

    assert hasattr(lazy, "compute")
    pd.testing.assert_frame_equal(eager, lazy.compute(), check_dtype=False)


def test_nadp_mdn_cleaning(tmp_path, nadp_meta, patch_remote_read_csv):
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

    with patch_remote_read_csv(nadp_meta):
        ds = NADPReader().open_dataset(files=str(fn), network="MDN", as_xarray=True, lazy=False)

    hg = ds.hgconc.values.flatten()
    assert hg[0] == 10.0
    assert np.isnan(hg[1])  # Flagged 'C' masked
