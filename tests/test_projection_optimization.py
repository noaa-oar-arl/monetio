import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.cmaq import CMAQReader


def create_mock_cmaq_dataset(n_rows=50, n_cols=50, use_dask=False):
    """Creates a mock CMAQ dataset for testing."""
    n_times = 1
    n_lay = 1
    times = pd.date_range("2023-01-01", periods=n_times, freq="h")

    tflag = np.zeros((n_times, 1, 2), dtype=np.int32)
    for i, t in enumerate(times):
        tflag[i, 0, 0] = int(t.strftime("%Y%j"))
        tflag[i, 0, 1] = int(t.strftime("%H%M%S"))

    ds = xr.Dataset(
        coords={
            "TSTEP": np.arange(n_times),
            "LAY": np.arange(n_lay),
            "ROW": np.arange(n_rows),
            "COL": np.arange(n_cols),
            "VAR": [b"O3"],
            "DATE_TIME": np.arange(2),
        },
        data_vars={
            "TFLAG": (("TSTEP", "VAR", "DATE_TIME"), tflag),
            "O3": (
                ("TSTEP", "LAY", "ROW", "COL"),
                np.random.rand(n_times, n_lay, n_rows, n_cols).astype(np.float32),
            ),
        },
    )

    ds.attrs.update(
        {
            "GDTYP": 2,
            "P_ALP": 33.0,
            "P_BET": 45.0,
            "YCENT": 40.0,
            "P_GAM": -97.0,
            "XCENT": -97.0,
            "XORIG": -100000.0,
            "YORIG": -100000.0,
            "XCELL": 12000.0,
            "YCELL": 12000.0,
            "NCOLS": n_cols,
            "NROWS": n_rows,
        }
    )
    ds.O3.attrs["units"] = "ppmV"

    if use_dask:
        ds = ds.chunk({"TSTEP": 1})

    return ds


def test_optimized_projection_eager_lazy(tmp_path):
    """Verify that optimized projection works for both Eager and Lazy data."""
    ds_mock = create_mock_cmaq_dataset()
    file_path = str(tmp_path / "test_proj.nc")
    ds_mock.to_netcdf(file_path, engine="h5netcdf")

    reader = CMAQReader()

    # 1. Test Eager
    ds_eager = reader.open_dataset(file_path, lazy=False, engine="h5netcdf")
    assert isinstance(ds_eager.latitude.data, np.ndarray)
    assert "Generated Latitude/Longitude coordinates" in ds_eager.attrs["history"]

    # 2. Test Lazy
    ds_lazy = reader.open_dataset(file_path, lazy=True, engine="h5netcdf")
    assert hasattr(ds_lazy.latitude.data, "dask")

    # 3. Assert identity
    xr.testing.assert_allclose(ds_eager.compute(), ds_lazy.compute())

    # Check coordinate attributes
    assert ds_eager.latitude.attrs["units"] == "degree_north"
    assert ds_eager.longitude.attrs["units"] == "degree_east"
