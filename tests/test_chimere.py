import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.chimere import ChimereReader


def mock_chimere_dataset():
    """Create a mock Chimere dataset."""
    ntime = 2
    nlay = 1
    nrow = 3
    ncol = 4

    ds = xr.Dataset(
        {
            "O3": (
                ("time_counter", "bottom_top", "y", "x"),
                np.random.default_rng(42).random((ntime, nlay, nrow, ncol)),
            ),
            "nav_lat": (("y", "x"), np.random.default_rng(43).random((nrow, ncol))),
            "nav_lon": (("y", "x"), np.random.default_rng(44).random((nrow, ncol))),
        }
    )
    ds.coords["time_counter"] = pd.date_range("2023-01-01", periods=ntime, freq="h")
    ds.O3.attrs["units"] = "ppb"

    return ds


def test_chimere_eager_lazy(tmp_path):
    ds_mock = mock_chimere_dataset()
    fname = tmp_path / "test_chimere.nc"
    ds_mock.to_netcdf(fname, engine="h5netcdf")

    reader = ChimereReader()

    # Eager Mode
    ds_eager = reader.open_dataset(files=str(fname), lazy=False, engine="h5netcdf")
    assert isinstance(ds_eager, xr.Dataset)
    assert "time" in ds_eager.coords
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords
    assert "z" in ds_eager.dims

    # Lazy Mode
    ds_lazy = reader.open_dataset(files=str(fname), use_dask=True, chunks={"time": 1})
    assert isinstance(ds_lazy, xr.Dataset)
    assert hasattr(ds_lazy.O3.data, "dask")

    # Verify values
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )


def test_chimere_var_list_excludes_unselected_dask_payload(tmp_path):
    ds = mock_chimere_dataset()
    ds["UNUSED"] = (("time_counter", "y", "x"), np.full((2, 3, 4), 99.0))
    path = tmp_path / "chimere-selective.nc"
    ds.to_netcdf(path, engine="h5netcdf")

    selected = ChimereReader().open_dataset(
        str(path), use_dask=True, chunks={"time": 1}, var_list=["O3"]
    )
    try:
        assert set(selected.data_vars) == {"O3"}
        assert hasattr(selected["O3"].data, "dask")
        np.testing.assert_allclose(
            selected.O3.compute(), ds.O3.rename({"time_counter": "time", "bottom_top": "z"})
        )
    finally:
        selected.close()


if __name__ == "__main__":
    pytest.main([__file__])
