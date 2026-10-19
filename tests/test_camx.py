from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from dask_audit_support import write_camx_pseudonetcdf_fixture

from monetio.readers.base import _get_ioapi_times
from monetio.readers.camx import CAMxReader


def mock_camx_dataset():
    """Create a mock CAMx dataset."""
    # Dimensions
    ntime = 2
    nlay = 1
    nrow = 3
    ncol = 4

    # Time flags (YYYYDDD, HHMMSS)
    # 2023-01-01 00:00:00 and 01:00:00
    tflag_data = [
        [[2023001, 0], [2023001, 10000]],  # For Var 1
        [[2023001, 0], [2023001, 10000]],  # For Var 2
    ]
    # In some CAMx formats it is (TSTEP, VAR, DATE_TIME)
    tflag = xr.DataArray(
        np.array(tflag_data).transpose(1, 0, 2), dims=("TSTEP", "VAR", "DATE_TIME"), name="TFLAG"
    )

    # Coordinates and data variables
    ds = xr.Dataset(
        {
            "O3": (("TSTEP", "LAY", "ROW", "COL"), np.random.rand(ntime, nlay, nrow, ncol)),
            "NO": (("TSTEP", "LAY", "ROW", "COL"), np.random.rand(ntime, nlay, nrow, ncol)),
            "NO2": (("TSTEP", "LAY", "ROW", "COL"), np.random.rand(ntime, nlay, nrow, ncol)),
            "TFLAG": tflag,
        }
    )

    # Grid attributes (LCC example)
    ds.attrs["GDTYP"] = 2
    ds.attrs["P_ALP"] = 30.0
    ds.attrs["P_BET"] = 60.0
    ds.attrs["P_GAM"] = -100.0
    ds.attrs["XCENT"] = -100.0
    ds.attrs["YCENT"] = 40.0
    ds.attrs["XORIG"] = -1000.0
    ds.attrs["YORIG"] = -1000.0
    ds.attrs["XCELL"] = 500.0
    ds.attrs["YCELL"] = 500.0
    ds.attrs["NCOLS"] = ncol
    ds.attrs["NROWS"] = nrow
    ds.attrs["NLAYS"] = nlay

    # Add units
    ds.O3.attrs["units"] = "ppm"
    ds.NO.attrs["units"] = "ppm"
    ds.NO2.attrs["units"] = "ppm"

    return ds


def test_camx_eager_lazy(tmp_path):
    """
    Verify that CAMx data can be loaded both eagerly and lazily with consistent results.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Temporary path for test file creation.

    Examples
    --------
    >>> test_camx_eager_lazy(tmp_path)
    """
    ds_mock = mock_camx_dataset()
    ds_mock["UNUSED"] = xr.full_like(ds_mock["O3"], -1)
    fname = tmp_path / "test_camx.nc"
    ds_mock.to_netcdf(fname, engine="h5netcdf")

    reader = CAMxReader()

    # Eager Mode
    ds_eager = reader.open_dataset(
        files=str(fname), lazy=False, engine="h5netcdf", drop_variables=["UNUSED"]
    )
    assert isinstance(ds_eager, xr.Dataset)
    assert "time" in ds_eager.coords
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords
    assert "o3" in ds_eager.data_vars
    assert ds_eager.o3.attrs["units"] == "ppbV"  # Converted from ppm

    # Lazy Mode
    # Note: Using chunks={} in open_dataset kwargs triggers dask
    ds_lazy = reader.open_dataset(
        files=str(fname), chunks={"time": 1}, engine="h5netcdf", drop_variables=["UNUSED"]
    )
    assert isinstance(ds_lazy, xr.Dataset)
    assert hasattr(ds_lazy.o3.data, "dask")
    assert ds_lazy.o3.chunksizes["time"] == (1, 1)
    assert "UNUSED" not in ds_eager
    assert "UNUSED" not in ds_lazy

    # Verify values and coordinates consistency
    # This ensures that both Eager (NumPy) and Lazy (Dask) paths produce identical results.
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )

    # Check diagnostics
    assert "nox" in ds_eager.data_vars
    # NOX = NO + NO2 (now that we added NO2 to the mock)
    expected_nox = ds_eager.no + ds_eager.no2
    xr.testing.assert_allclose(ds_eager.nox, expected_nox)

    # Verify history tracking
    assert "Added lazy diagnostic: NOx" in ds_eager.attrs["history"]
    assert "Generated Latitude/Longitude coordinates" in ds_eager.attrs["history"]

    # Verify coordinate laziness/chunking (Step 3 fix)
    assert hasattr(ds_lazy.latitude.data, "dask")
    assert hasattr(ds_lazy.longitude.data, "dask")


def test_camx_pseudonetcdf_default_backend_dask_parity(tmp_path):
    pytest.importorskip("PseudoNetCDF")
    path = write_camx_pseudonetcdf_fixture(tmp_path / "projected.uamiv")
    reader = CAMxReader()
    eager = reader.open_dataset(str(path), use_dask=False)
    lazy = reader.open_dataset(str(path), use_dask=True, chunks={"time": 1})
    try:
        assert lazy.o3.chunksizes["time"] == (1,)
        xr.testing.assert_allclose(
            eager.drop_vars("history", errors="ignore"),
            lazy.compute().drop_vars("history", errors="ignore"),
        )
    finally:
        eager.close()
        lazy.close()


def test_ioapi_time_uses_header_start_when_first_tflag_is_missing():
    ds = mock_camx_dataset()
    ds.attrs["SDATE"] = 2023001
    ds.attrs["STIME"] = 0
    ds["TFLAG"][0, 0, 0] = 0

    actual = _get_ioapi_times(ds).time.values

    np.testing.assert_array_equal(
        actual,
        np.array(["2023-01-01T00:00:00", "2023-01-01T01:00:00"], dtype="datetime64[ns]"),
    )


@pytest.mark.parametrize("reference_backend", ["kerchunk", "icechunk"])
def test_camx_virtualizarr_dask_backend_parity(tmp_path, reference_backend):
    pytest.importorskip("virtualizarr")
    if reference_backend == "icechunk":
        pytest.importorskip("icechunk")

    sample = (Path(__file__).parent / "data" / "synthetic" / "camx" / "camx_synthetic.nc").resolve()
    reader = CAMxReader()
    eager = reader.open_dataset(str(sample), engine="h5netcdf", use_dask=False)
    options = {
        "use_virtualizarr": True,
        "virtualizarr_parser": "hdf5",
        "use_dask": True,
        "chunks": {"time": 1},
    }
    if reference_backend == "kerchunk":
        options["virtualizarr_file"] = str(tmp_path / "camx-refs.json")
    else:
        options.update(
            use_icechunk=True,
            icechunk_url=str(tmp_path / "camx-icechunk"),
        )

    lazy = reader.open_dataset(str(sample), **options)
    try:
        assert lazy.o3.chunksizes["time"] == (1, 1)
        xr.testing.assert_allclose(
            eager.drop_vars("history", errors="ignore"),
            lazy.compute().drop_vars("history", errors="ignore"),
        )
        if reference_backend == "kerchunk":
            assert (tmp_path / "camx-refs.json").is_file()
    finally:
        eager.close()
        lazy.close()


if __name__ == "__main__":
    pytest.main([__file__])
