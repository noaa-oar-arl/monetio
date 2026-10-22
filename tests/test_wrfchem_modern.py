import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.wrfchem import WRFChemReader


def create_synthetic_wrfchem_ds():
    """Create a synthetic WRF-Chem dataset for modernization testing."""
    times = pd.date_range("2023-01-01", periods=2, freq="h")
    times_strings = [t.strftime("%Y-%m-%d_%H:%M:%S") for t in times]
    times_bytes = np.array([list(s) for s in times_strings], dtype="|S1")

    data_vars = {
        "Times": (("time", "DateStrLen"), times_bytes),
        "XLAT": (("time", "south_north", "west_east"), np.zeros((2, 4, 4))),
        "XLONG": (("time", "south_north", "west_east"), np.zeros((2, 4, 4))),
        "O3": (
            ("time", "bottom_top", "south_north", "west_east"),
            np.ones((2, 2, 4, 4)),
            {"units": "ppmv"},
        ),
    }
    ds = xr.Dataset(data_vars)
    ds = ds.set_coords(["XLAT", "XLONG"])
    return ds


@pytest.mark.parametrize("use_dask", [False, True])
def test_wrfchem_modern_params(use_dask, tmp_path):
    """Test that WRFChemReader accepts modern parameters and use_dask."""
    ds_orig = create_synthetic_wrfchem_ds()
    file_path = tmp_path / "wrfout_modern.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()

    # Test explicit use_dask parameter instead of chunks={}
    ds = reader.open_dataset(
        str(file_path),
        use_dask=use_dask,
        use_virtualizarr=False,  # Can't test True without extra deps
        use_icechunk=False,
    )

    if use_dask:
        assert ds.O3.chunks is not None
    else:
        assert ds.O3.chunks is None

    assert "time" in ds.coords
    assert ds.O3.attrs["units"] == "ppbV"


def test_wrfchem_dimension_ordering(tmp_path):
    """Verify standard dimension ordering (time, z, y, x)."""
    ds_orig = create_synthetic_wrfchem_ds()
    file_path = tmp_path / "wrfout_dims.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()
    ds = reader.open_dataset(str(file_path))

    expected_dims = ("time", "z", "y", "x")
    assert ds.O3.dims == expected_dims


def test_wrfchem_history_provenance(tmp_path):
    """Verify that history accurately reflects transformations."""
    ds_orig = create_synthetic_wrfchem_ds()
    file_path = tmp_path / "wrfout_hist.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()
    ds = reader.open_dataset(str(file_path))

    assert "history" in ds.attrs
    hist = ds.attrs["history"]
    assert "Preprocessed WRF-Chem data" in hist
    assert "renaming" in hist
    assert "unit conversion" in hist
    assert "hygiene" in hist
