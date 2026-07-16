import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.sat_utils import apply_lazy_conversion, lazy_index_along_axis


def test_lazy_index_along_axis():
    # Setup data: (z=10, y=3, x=4)
    data_np = np.random.rand(10, 3, 4).astype(np.float32)
    data = xr.DataArray(data_np, dims=["z", "y", "x"], name="test")

    # Indices: (y=3, x=4) - picking a z for each (y,x)
    idx_np = np.random.randint(0, 10, size=(3, 4))
    idx = xr.DataArray(idx_np, dims=["y", "x"])

    # 1. Eager
    res_eager = lazy_index_along_axis(data, idx, "z")
    assert res_eager.dims == ("y", "x")

    # 2. Lazy
    data_lazy = data.chunk({"z": -1, "y": 2, "x": 2})
    idx_lazy = idx.chunk({"y": 2, "x": 2})
    res_lazy = lazy_index_along_axis(data_lazy, idx_lazy, "z")

    assert hasattr(res_lazy.data, "dask")

    # Verify values
    np.testing.assert_allclose(res_eager.values, res_lazy.compute().values)

    # Manual verification of one point
    y, x = 1, 2
    expected = data_np[idx_np[y, x], y, x]
    assert res_eager.values[y, x] == expected


def test_apply_lazy_conversion():
    data = xr.DataArray(np.array([1, 2, 3], dtype=np.float32), dims=["x"])

    def func(x):
        return x * 2

    res_eager = apply_lazy_conversion(data, func, np.float32)
    assert np.all(res_eager.values == np.array([2, 4, 6]))

    data_lazy = data.chunk({"x": 1})
    res_lazy = apply_lazy_conversion(data_lazy, func, np.float32)
    assert hasattr(res_lazy.data, "dask")
    assert np.all(res_lazy.compute().values == np.array([2, 4, 6]))


def test_time_conversion_lazy():
    # Typical satellite time: seconds since ref
    ref_date = pd.Timestamp("1993-01-01")
    times_raw = np.array([0, 3600, 7200], dtype=np.float64)
    data = xr.DataArray(times_raw, dims=["time_raw"])

    def _convert_time(t):
        return pd.to_datetime(t, unit="s", origin=ref_date)

    res_eager = apply_lazy_conversion(data, _convert_time, "datetime64[ns]")

    data_lazy = data.chunk({"time_raw": 1})
    res_lazy = apply_lazy_conversion(data_lazy, _convert_time, "datetime64[ns]")

    assert hasattr(res_lazy.data, "dask")
    pd.testing.assert_index_equal(
        pd.DatetimeIndex(res_eager.values), pd.DatetimeIndex(res_lazy.compute().values)
    )
