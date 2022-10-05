import numpy as np
import pytest
import xarray as xr

from monetio.util import _try_merge_exact


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
