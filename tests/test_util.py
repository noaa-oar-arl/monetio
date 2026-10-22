import dask.array as da
import numpy as np
import pytest
import xarray as xr

from monetio.util import _try_merge_exact, calc_13_category_usda_soil_type


def test_merge_exact_helper():
    x = np.r_[1:11]
    a = x**2
    b = x**3
    left = xr.Dataset(
        data_vars={
            "a": ("x", a),
        },
        coords={
            "x": ("x", x),
        },
    )
    right = xr.Dataset(
        data_vars={
            "b": ("x", b),
        },
        coords={
            "x": ("x", x),
        },
    )
    new = _try_merge_exact(left, right)

    assert set(left.data_vars) == {"a"}
    assert set(right.data_vars) == {"b"}
    assert set(new.data_vars) == {"a", "b"}

    assert new.x.equals(left.x) and new.x.equals(right.x)


def test_issue78():
    # In this issue, dimension coordinate 'grid_xt' fails to match exactly
    # since one dataset (normal output) has it in float64 and the other (PM2.5)
    # has it in float32.
    x64 = np.linspace(20, 65, 90, dtype=np.float64)
    y64 = np.linspace(20, 45, 72, dtype=np.float64)
    x32 = x64.astype(np.float32)
    y32 = y64.astype(np.float32)
    a = np.random.rand(y64.size, x64.size)
    b = np.random.rand(y64.size, x64.size)
    left = xr.Dataset(
        data_vars={
            "a": (("y", "x"), a),
        },
        coords={
            "x": ("x", x64),
            "y": ("y", y64),
        },
    )
    right = xr.Dataset(
        data_vars={
            "b": (("y", "x"), b),
        },
        coords={
            "x": ("x", x32),
            "y": ("y", y32),
        },
    )

    assert not left.x.equals(right.x)
    assert not left.y.equals(right.y)

    with pytest.raises(ValueError, match="Unable to merge blah due to issue matching coordinates."):
        _ = _try_merge_exact(left, right, right_name="blah")


def test_calc_soil_type_eager_vs_lazy():
    """Verify that calc_13_category_usda_soil_type works identically for NumPy and Dask."""
    # Create some test data representing various categories
    # 1: SAND (si + c * 1.5 < 15.0)
    # 12: CLAY (c >= 40 & sa <= 45 & si < 40)
    # 6: LOAM (c >= 7 & c < 27 & si >= 28 & si < 50 & sa <= 52)

    clay_np = np.array([5.0, 45.0, 20.0, 255.0], dtype=float)
    sand_np = np.array([90.0, 40.0, 40.0, 0.0], dtype=float)
    silt_np = np.array([5.0, 15.0, 40.0, 0.0], dtype=float)

    # Eager execution
    res_eager = calc_13_category_usda_soil_type(clay_np, sand_np, silt_np)
    assert isinstance(res_eager, np.ndarray)

    # Lazy execution
    clay_da = xr.DataArray(da.from_array(clay_np, chunks=2), dims="x")
    sand_da = xr.DataArray(da.from_array(sand_np, chunks=2), dims="x")
    silt_da = xr.DataArray(da.from_array(silt_np, chunks=2), dims="x")

    res_lazy = calc_13_category_usda_soil_type(clay_da, sand_da, silt_da)

    # Ensure it is still lazy
    assert hasattr(res_lazy.data, "dask")
    assert "history" in res_lazy.attrs

    # Verify results match
    res_lazy_computed = res_lazy.compute()
    np.testing.assert_allclose(res_eager, res_lazy_computed.values)

    # Specific value checks
    assert res_eager[0] == 1.0  # SAND
    assert res_eager[1] == 12.0  # CLAY
    assert res_eager[2] == 6.0  # LOAM
    assert res_eager[3] == 0.0  # FillValue 255 -> 0.0


def test_calc_soil_type_dask_identity():
    """Verify that results are identical when using Dask vs NumPy."""
    clay = np.random.rand(10, 10) * 100
    sand = np.random.rand(10, 10) * 100
    silt = np.random.rand(10, 10) * 100

    res_np = calc_13_category_usda_soil_type(clay, sand, silt)

    clay_da = xr.DataArray(clay, dims=("y", "x")).chunk({"x": 5, "y": 5})
    sand_da = xr.DataArray(sand, dims=("y", "x")).chunk({"x": 5, "y": 5})
    silt_da = xr.DataArray(silt, dims=("y", "x")).chunk({"x": 5, "y": 5})

    res_da = calc_13_category_usda_soil_type(clay_da, sand_da, silt_da)

    xr.testing.assert_allclose(
        xr.DataArray(res_np, dims=("y", "x")), res_da.drop_vars("history", errors="ignore")
    )
