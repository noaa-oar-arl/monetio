import numpy as np
import xarray as xr

from monetio.readers.hysplit import get_thickness, mass_loading


def _get_mock_hysplit():
    """Returns a mock HYSPLIT-like dataset for testing."""
    # Vertical grid: 0 (dep), 100, 500, 1000
    z = [0.0, 100.0, 500.0, 1000.0]
    data = np.random.rand(4, 5, 5).astype(np.float32)
    ds = xr.Dataset(
        {"conc": (("z", "y", "x"), data)},
        coords={"z": z, "y": np.arange(5), "x": np.arange(5)},
    )
    ds.attrs["history"] = "Mock HYSPLIT data."
    return ds


def test_hysplit_mass_loading_eager_vs_lazy():
    """Verify that Eager (NumPy) and Lazy (Dask) mass loading results are identical."""
    ds = _get_mock_hysplit()

    # 1. Eager
    ml_eager = mass_loading(ds)

    # 2. Lazy
    ds_lazy = ds.chunk({"z": -1, "y": 2, "x": 2})
    ml_lazy = mass_loading(ds_lazy)

    # Check laziness
    assert hasattr(ml_lazy.conc.data, "dask")

    # Compute and compare
    ml_lazy_computed = ml_lazy.compute()

    xr.testing.assert_allclose(ml_eager, ml_lazy_computed)

    # Check history
    assert "Calculated mass loading using standardized preprocessing." in ml_eager.attrs["history"]


def test_get_thickness():
    """Verify thickness calculation for various vertical grids."""
    # Case 1: With deposition layer
    z1 = xr.DataArray([0, 100, 500], dims="z", coords={"z": [0, 100, 500]})
    ds1 = xr.Dataset({"conc": (("z"), [1, 2, 3])}, coords={"z": z1})
    thick1 = get_thickness(ds1)
    # Expected: [0, 100, 400]
    np.testing.assert_array_equal(thick1.values, [0, 100, 400])

    # Case 2: Without deposition layer
    z2 = xr.DataArray([100, 500], dims="z", coords={"z": [100, 500]})
    ds2 = xr.Dataset({"conc": (("z"), [2, 3])}, coords={"z": z2})
    thick2 = get_thickness(ds2)
    # Expected: [100, 400]
    np.testing.assert_array_equal(thick2.values, [100, 400])


def test_mass_loading_with_delta():
    """Verify mass loading when delta is explicitly provided."""
    ds = _get_mock_hysplit()
    # delta for all layers including dep
    delta = np.array([0, 100, 400, 500], dtype=float)

    ml = mass_loading(ds, delta=delta)

    # Expected sum: conc[1]*100 + conc[2]*400 + conc[3]*500
    # Drop z coordinate on expected to match the integrated result
    expected = (ds.conc[1] * 100 + ds.conc[2] * 400 + ds.conc[3] * 500).drop_vars("z")
    xr.testing.assert_allclose(ml.conc.drop_vars("z", errors="ignore"), expected)
