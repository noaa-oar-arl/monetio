from pathlib import Path

import xarray as xr

from monetio.readers.raqms import RAQMSReader

DATA = Path(__file__).parent / "data"
TEST_FP = str(DATA / "uwhyb_06_01_2017_18Z.chem.assim.nc")


def test_raqms_eager_vs_lazy():
    """Verifies RAQMS reader produces identical results for Eager and Lazy backends."""
    reader = RAQMSReader()

    # 1. Eager (NumPy)
    ds_eager = reader.open_dataset(TEST_FP, lazy=False)

    # 2. Lazy (Dask)
    ds_lazy = reader.open_dataset(TEST_FP, lazy=True, chunks={"time": 1})

    # Basic checks
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords
    assert "time" in ds_eager.coords
    assert "surfpres_pa" in ds_eager.data_vars

    # Ensure lazy is actually lazy
    assert hasattr(ds_lazy.surfpres_pa.data, "dask")

    # 3. Assert equality
    # We drop history as it contains timestamps which will differ
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )


def test_raqms_surf_only():
    """Verifies surf_only flag in RAQMS reader."""
    reader = RAQMSReader()

    ds = reader.open_dataset(TEST_FP, surf_only=True)

    assert ds.sizes["z"] == 1
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords


def test_raqms_var_list():
    """Verifies var_list filtering in RAQMS reader."""
    reader = RAQMSReader()

    # Keep only ozone and required variables
    ds = reader.open_dataset(TEST_FP, var_list=["o3vmr"])

    assert "o3vmr" in ds.data_vars
    assert "surfpres_pa" in ds.data_vars  # psfc is required
    # Check that a non-requested, non-required var is NOT present
    # In RAQMS, 'geop' is usually present but not in our required list
    assert "geop" not in ds.data_vars


def test_raqms_unit_conversion():
    """Verifies ppv to ppbv conversion in RAQMS reader."""
    reader = RAQMSReader()

    # With conversion (default)
    ds_ppb = reader.open_dataset(TEST_FP, convert_to_ppb=True)
    assert ds_ppb.o3vmr.attrs["units"] == "ppbv"

    # Without conversion
    ds_ppv = reader.open_dataset(TEST_FP, convert_to_ppb=False)
    # The original units in the file are 'ppv'
    assert ds_ppv.o3vmr.attrs["units"] == "ppv"

    # Check the factor of 10^9
    xr.testing.assert_allclose(ds_ppb.o3vmr, ds_ppv.o3vmr * 1e9)
