import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.camx import CAMxReader
from monetio.readers.cmaq import CMAQReader


def create_mock_ioapi_dataset(n_times=2, n_lay=1, n_rows=10, n_cols=10, overlap=False):
    """Creates a mock IOAPI-like dataset (shared by CMAQ and CAMx tests)."""
    if overlap:
        # 0, 1, 1, 2
        times = pd.to_datetime(
            [
                "2023-01-01 00:00:00",
                "2023-01-01 01:00:00",
                "2023-01-01 01:00:00",
                "2023-01-01 02:00:00",
            ]
        )
        n_times = 4
    else:
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
            "IOAPI_VERSION": "mock",
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
    return ds


@pytest.mark.parametrize("reader_class", [CMAQReader, CAMxReader])
def test_duplicate_handling_laziness(tmp_path, reader_class):
    """Verify that drop_duplicates=True is lazy and correct."""
    ds = create_mock_ioapi_dataset(overlap=True)
    f1 = str(tmp_path / "test.nc")
    ds.to_netcdf(f1, engine="h5netcdf")

    reader = reader_class()

    # 1. Test Eager
    ds_eager = reader.open_dataset(f1, drop_duplicates=True, engine="h5netcdf")
    assert ds_eager.time.size == 3

    # Support both case-sensitive and case-insensitive readers
    o3_var = "O3" if "O3" in ds_eager.data_vars else "o3"

    # If the driver/engine defaults to dask, we compute it for the "Eager" check
    o3_data = ds_eager[o3_var].data
    if hasattr(o3_data, "compute"):
        o3_data = o3_data.compute()
    assert isinstance(o3_data, np.ndarray)

    # 2. Test Lazy
    # Use chunks to ensure it stays dask-backed
    ds_lazy = reader.open_dataset(f1, drop_duplicates=True, chunks={"TSTEP": 1}, engine="h5netcdf")

    # The coordinate 'time' will be computed for drop_duplicates (unavoidable)
    # but the data variables should remain dask-backed.
    assert hasattr(ds_lazy[o3_var].data, "dask")
    assert ds_lazy.time.size == 3

    # 3. Verify results are identical
    xr.testing.assert_allclose(ds_eager.compute(), ds_lazy.compute())


@pytest.mark.parametrize("reader_class", [CMAQReader, CAMxReader])
def test_no_duplicates_laziness(tmp_path, reader_class):
    """Verify that by default (False) no unnecessary computes happen."""
    ds = create_mock_ioapi_dataset(n_times=2)
    f1 = str(tmp_path / "test_no_dup.nc")
    ds.to_netcdf(f1, engine="h5netcdf")

    reader = reader_class()

    # Open with Dask
    # Note: We must ensure TFLAG itself is chunked so that _get_times remains lazy
    ds_lazy = reader.open_dataset(f1, drop_duplicates=False, chunks={"TSTEP": 1}, engine="h5netcdf")

    # Support both case-sensitive and case-insensitive readers
    o3_var = "O3" if "O3" in ds_lazy.data_vars else "o3"

    # In the new implementation, the O3 data variable must stay lazy.
    # Xarray may compute coordinates during alignment or concatenation,
    # but we have removed the explicit .compute() from our readers.
    assert hasattr(ds_lazy[o3_var].data, "dask")
