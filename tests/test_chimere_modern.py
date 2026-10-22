import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.chimere import chimere_preprocess


def mock_chimere_dataset():
    """Create a mock Chimere dataset."""
    ntime = 2
    nlay = 3
    nrow = 4
    ncol = 5

    ds = xr.Dataset(
        {
            "O3": (
                ("time_counter", "bottom_top", "y", "x"),
                np.random.rand(ntime, nlay, nrow, ncol),
            ),
            "nav_lat": (("y", "x"), np.random.rand(nrow, ncol)),
            "nav_lon": (("y", "x"), np.random.rand(nrow, ncol)),
        }
    )
    ds.coords["time_counter"] = pd.date_range("2023-01-01", periods=ntime, freq="h")
    ds.O3.attrs["units"] = "ppb"
    ds.attrs["history"] = "Original"

    return ds


def test_chimere_preprocess_consistency():
    ds_eager = mock_chimere_dataset()
    ds_lazy = ds_eager.chunk({"time_counter": 1})

    # Run preprocess on both
    ds_eager_out = chimere_preprocess(ds_eager, surf_only=False)
    ds_lazy_out = chimere_preprocess(ds_lazy, surf_only=False)

    # 1. Verify Lazy remains lazy
    assert hasattr(ds_lazy_out.O3.data, "dask")

    # 2. Verify consistency
    xr.testing.assert_allclose(
        ds_eager_out.drop_vars("history", errors="ignore"),
        ds_lazy_out.compute().drop_vars("history", errors="ignore"),
    )

    # 3. Verify coordinates
    assert "time" in ds_eager_out.coords
    assert "latitude" in ds_eager_out.coords
    assert "longitude" in ds_eager_out.coords
    assert "z" in ds_eager_out.dims

    # 4. Verify dimensions order
    assert ds_eager_out.O3.dims == ("time", "z", "y", "x")


def test_chimere_preprocess_surf_only():
    ds_eager = mock_chimere_dataset()

    ds_out = chimere_preprocess(ds_eager, surf_only=True)

    assert ds_out.sizes["z"] == 1
    assert "time" in ds_out.coords


if __name__ == "__main__":
    pytest.main([__file__])
