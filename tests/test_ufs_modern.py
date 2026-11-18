from pathlib import Path

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from monetio.readers.ufs import UFSReader


def create_mock_ufs_ds(lazy=False):
    """Creates a mock UFS-AQM dataset."""
    nx, ny, nz = 10, 10, 5
    nt = 2

    # Use fixed seed for data generation inside here too if needed,
    # but we handle it in the test.
    data = {
        "tmp": (("time", "z", "y", "x"), np.random.rand(nt, nz, ny, nx) + 273.15),
        "pressfc": (("time", "y", "x"), np.random.rand(nt, ny, nx) * 1000 + 100000),
        "ak": (("z_i",), np.linspace(0, 100, nz + 1)),
        "bk": (("z_i",), np.linspace(1, 0, nz + 1)),
        "lat": (("y", "x"), np.random.rand(ny, nx)),
        "lon": (("y", "x"), np.random.rand(ny, nx)),
        "delz": (("time", "z", "y", "x"), np.random.rand(nt, nz, ny, nx) * 100),
        "hgtsfc": (("y", "x"), np.random.rand(ny, nx) * 10),
        "o3": (("time", "z", "y", "x"), np.random.rand(nt, nz, ny, nx) * 1e-6),
        "aso4j": (("time", "z", "y", "x"), np.random.rand(nt, nz, ny, nx)),
    }

    # Add units
    attrs = {
        "tmp": {"units": "K"},
        "pressfc": {"units": "Pa"},
        "o3": {"units": "ppmV"},
        "aso4j": {"units": "ug/kg"},
    }

    ds = xr.Dataset(
        data_vars=data,
        coords={
            "time": (
                ("time",),
                [np.datetime64("2023-01-01T00:00"), np.datetime64("2023-01-01T01:00")],
            ),
            "pfull": (("z",), np.arange(nz)[::-1]),  # Use descending to avoid flip or expect it
            "phalf": (("z_i",), np.arange(nz + 1)[::-1]),
        },
    )

    for var, v_attrs in attrs.items():
        ds[var].attrs.update(v_attrs)

    if lazy:
        ds = ds.chunk({"time": 1, "z": -1, "y": -1, "x": -1})

    return ds


def test_ufs_protocol_compliance():
    """Verify UFS processing is backend-agnostic and lazy-friendly."""
    np.random.seed(42)
    ds_base = create_mock_ufs_ds(lazy=False)
    ds_eager_in = ds_base.copy(deep=True)
    ds_lazy_in = ds_base.chunk({"time": 1, "z": -1, "y": -1, "x": -1})

    reader = UFSReader()

    # Mock the driver.open to return our mock datasets
    class MockDriver:
        def __init__(self, ds):
            self.ds = ds

        def open(self, *args, **kwargs):
            return self.ds

    # Test Eager
    reader.driver = MockDriver(ds_eager_in)
    res_eager = reader.open_dataset("dummy", convert_to_ppb=True)

    # Test Lazy
    reader.driver = MockDriver(ds_lazy_in)
    res_lazy = reader.open_dataset("dummy", convert_to_ppb=True)

    # Check laziness
    assert isinstance(res_lazy.o3.data, da.Array)
    assert isinstance(res_lazy.PM25.data, da.Array)

    # Check correctness (ppmV -> ppbv)
    # Since we used descending z in mock, no flip should happen in open_dataset
    # (because ds.z[0] > ds.z[-1])
    np.testing.assert_allclose(res_eager.o3.values, ds_base.o3.values * 1000.0)

    # Check consistency between eager and lazy
    xr.testing.assert_allclose(res_eager, res_lazy.compute())

    # Check history
    assert "history" in res_eager.attrs
    assert "Read UFS-AQM data." in res_eager.attrs["history"]

    # Check coordinates
    assert "latitude" in res_eager.coords
    assert "longitude" in res_eager.coords
    assert "time" in res_eager.coords


def test_ufs_multifile_use_dask_eager_parity():
    files = sorted(Path("tests/data/ufs").glob("aqm.t12z.dyn.f00*.nc"))
    assert len(files) == 2
    reader = UFSReader()
    eager = reader.open_dataset([str(path) for path in files])
    lazy = reader.open_dataset([str(path) for path in files], use_dask=True)

    assert hasattr(lazy.o3_ave.data, "dask")
    xr.testing.assert_allclose(eager, lazy.compute())


@pytest.mark.parametrize("reference_backend", ["kerchunk", "icechunk"])
def test_ufs_multifile_virtualizarr_dask_parity(tmp_path, reference_backend):
    pytest.importorskip("virtualizarr")
    if reference_backend == "icechunk":
        pytest.importorskip("icechunk")

    files = sorted(Path("tests/data/ufs").glob("aqm.t12z.dyn.f00*.nc"))
    assert len(files) == 2
    reader = UFSReader()
    eager = reader.open_dataset([str(path) for path in files], use_dask=False)
    options = {
        "use_virtualizarr": True,
        "use_dask": True,
        "chunks": {"time": 1},
    }
    if reference_backend == "kerchunk":
        options["virtualizarr_file"] = str(tmp_path / "ufs-refs.json")
    else:
        options.update(use_icechunk=True, icechunk_url=str(tmp_path / "ufs-icechunk"))

    lazy = reader.open_dataset([str(path) for path in files], **options)
    try:
        assert hasattr(lazy.o3_ave.data, "dask")
        assert lazy.o3_ave.chunksizes["time"] == (1, 1)
        xr.testing.assert_allclose(eager, lazy.compute())
        if reference_backend == "kerchunk":
            assert (tmp_path / "ufs-refs.json").is_file()
    finally:
        eager.close()
        lazy.close()


def test_ufs_var_list_excludes_unselected_dask_payload(tmp_path):
    np.random.seed(42)
    source = create_mock_ufs_ds()
    source["UNUSED"] = (("time", "y", "x"), np.full((2, 10, 10), -99.0))
    path = tmp_path / "ufs-selective.nc"
    source.to_netcdf(path, engine="h5netcdf")

    reader = UFSReader()
    eager = reader.open_dataset(str(path), engine="h5netcdf", var_list=["o3"])
    lazy = reader.open_dataset(
        str(path), engine="h5netcdf", use_dask=True, chunks={"time": 1}, var_list=["o3"]
    )
    try:
        assert "UNUSED" not in lazy.data_vars
        assert "aso4j" not in lazy.data_vars
        assert hasattr(lazy.o3.data, "dask")
        xr.testing.assert_allclose(eager, lazy.compute())
    finally:
        lazy.close()


def test_ufs_diagnostic_pm25():
    """Verify PM25 calculation logic."""
    np.random.seed(42)
    ds = create_mock_ufs_ds(lazy=False)
    # Add another component
    ds["aso4i"] = (("time", "z", "y", "x"), np.random.rand(*ds.aso4j.shape))
    ds["aso4i"].attrs["units"] = "ug/kg"

    reader = UFSReader()

    class MockDriver:
        def open(self, *args, **kwargs):
            return ds

    reader.driver = MockDriver()

    res = reader.open_dataset("dummy")

    # The reader will convert units from ug/kg to ug/m3 before summing for diagnostics
    assert res.aso4j.attrs["units"] == r"$\mu g m^{-3}$"
    assert res.aso4i.attrs["units"] == r"$\mu g m^{-3}$"

    # PM25 = aso4j * 1.0 + aso4i * 1.0 + ...
    expected_pm25 = res.aso4j + res.aso4i
    xr.testing.assert_allclose(res.PM25, expected_pm25)


if __name__ == "__main__":
    pytest.main([__file__])
