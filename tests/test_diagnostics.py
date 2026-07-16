import numpy as np
import xarray as xr

from monetio.readers.base import DiagnosticSpec, _convert_to_ppb, _format_units, add_lazy_diagnostic


def test_add_lazy_diagnostic_eager_vs_lazy():
    """Verify add_lazy_diagnostic works identically on Eager (NumPy) and Lazy (Dask)."""
    # 1. Setup Eager Dataset
    ds = xr.Dataset(
        {
            "v1": (("time", "y", "x"), np.ones((2, 3, 3), dtype=float)),
            "v2": (("time", "y", "x"), np.full((2, 3, 3), 2.0, dtype=float)),
            "v3": (("time", "y", "x"), np.full((2, 3, 3), 3.0, dtype=float)),
        }
    )
    ds["v1"].attrs["units"] = "ppbV"
    ds["v2"].attrs["units"] = "ppbV"
    ds["v3"].attrs["units"] = "ppbV"

    spec = DiagnosticSpec(
        variables=["v1", "v2", "v3"],
        weights=[1.0, 2.0, 0.5],
        units="ppbV",
        long_name="Sum Var",
        name="sum_var",
    )

    # 2. Run Eager
    ds_eager = add_lazy_diagnostic(ds.copy(), "sum_var", spec)
    assert "sum_var" in ds_eager.data_vars
    # Expected: 1.0*1.0 + 2.0*2.0 + 3.0*0.5 = 1.0 + 4.0 + 1.5 = 6.5
    np.testing.assert_allclose(ds_eager["sum_var"].values, 6.5)
    assert isinstance(ds_eager["sum_var"].data, np.ndarray)

    # 3. Run Lazy
    ds_lazy = add_lazy_diagnostic(ds.chunk({"time": 1}), "sum_var", spec)
    assert "sum_var" in ds_lazy.data_vars
    assert hasattr(ds_lazy["sum_var"].data, "dask")
    np.testing.assert_allclose(ds_lazy["sum_var"].values, 6.5)

    # Cross-check. Use assert_allclose to ignore timestamp differences in history attribute.
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())


def test_add_lazy_diagnostic_unit_sync():
    """Verify unit synchronization (ppmV <-> ppbV) in add_lazy_diagnostic."""
    ds = xr.Dataset(
        {
            "v_ppm": (("x",), np.array([1.0])),
            "v_ppb": (("x",), np.array([1000.0])),
        }
    )
    ds["v_ppm"].attrs["units"] = "ppmV"
    ds["v_ppb"].attrs["units"] = "ppbV"

    spec = DiagnosticSpec(variables=["v_ppm", "v_ppb"], name="total", units="ppbV")

    # Case A: Base is ppmV, result should be ppmV (sum in ppmV: 1.0 + 1000.0/1000.0 = 2.0)
    ds_a = add_lazy_diagnostic(ds.copy(), "total", spec)
    assert ds_a["total"].attrs["units"] == "ppmV"
    np.testing.assert_allclose(ds_a["total"].values, 2.0)

    # Case B: Base is ppbV, result should be ppbV (sum in ppbV: 1000.0 + 1.0*1000.0 = 2000.0)
    # We must ensure v_ppb is FIRST in spec.variables to make it the base.
    spec_b = DiagnosticSpec(variables=["v_ppb", "v_ppm"], name="total", units="ppbV")
    ds_b = xr.Dataset(
        {
            "v_ppb": (("x",), np.array([1000.0])),
            "v_ppm": (("x",), np.array([1.0])),
        }
    )
    ds_b["v_ppb"].attrs["units"] = "ppbV"
    ds_b["v_ppm"].attrs["units"] = "ppmV"
    ds_b = add_lazy_diagnostic(ds_b, "total", spec_b)
    assert ds_b["total"].attrs["units"] == "ppbV"
    np.testing.assert_allclose(ds_b["total"].values, 2000.0)


def test_add_lazy_diagnostic_alias():
    """Verify alias handling in add_lazy_diagnostic."""
    ds = xr.Dataset({"PM2_5": (("x",), [10.0])})
    ds["PM2_5"].attrs["units"] = "ug/m3"

    spec = DiagnosticSpec(variables=["non_existent"], name="PM25", units="ug/m3")

    # Default alias PM2_5 should be picked up for PM25
    ds_res = add_lazy_diagnostic(ds, "PM25", spec)
    assert "PM25" in ds_res.data_vars
    np.testing.assert_allclose(ds_res["PM25"].values, 10.0)
    assert "Added lazy diagnostic: PM25 (using alias PM2_5)" in ds_res.attrs["history"]


def test_convert_to_ppb_eager_vs_lazy():
    """Verify _convert_to_ppb works identically on Eager and Lazy."""
    ds = xr.Dataset({"gas": (("x",), [1.0])})
    ds["gas"].attrs["units"] = "ppmv"

    # Eager
    ds_e = _convert_to_ppb(ds.copy())
    np.testing.assert_allclose(ds_e["gas"].values, 1000.0)
    assert ds_e["gas"].attrs["units"] == "ppbV"

    # Lazy
    ds_l = _convert_to_ppb(ds.chunk({"x": 1}))
    assert hasattr(ds_l["gas"].data, "dask")
    np.testing.assert_allclose(ds_l["gas"].values, 1000.0)
    assert ds_l["gas"].attrs["units"] == "ppbV"


def test_format_units_eager_vs_lazy():
    """Verify _format_units works identically on Eager and Lazy."""
    ds = xr.Dataset({"pm": (("x",), [1.0])})
    ds["pm"].attrs["units"] = "micrograms/m3"

    # Eager
    ds_e = _format_units(ds.copy())
    assert ds_e["pm"].attrs["units"] == r"$\mu g m^{-3}$"

    # Lazy
    ds_l = _format_units(ds.chunk({"x": 1}))
    assert ds_l["pm"].attrs["units"] == r"$\mu g m^{-3}$"
