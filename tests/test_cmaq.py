import numpy as np
import pytest
import xarray as xr

from monetio.readers.cmaq import CMAQReader, cmaq_preprocess


def create_mock_cmaq_dataset(lazy=False, seed=42):
    """Creates a mock CMAQ-compliant dataset."""
    rng = np.random.default_rng(seed)

    # LCC Projection example

    # Grid metadata
    ncols = 10
    nrows = 10
    nlays = 1
    ntime = 2

    # IOAPI attributes
    attrs = {
        "NCOLS": ncols,
        "NROWS": nrows,
        "NLAYS": nlays,
        "XORIG": -1000000.0,
        "YORIG": -1000000.0,
        "XCELL": 200000.0,
        "YCELL": 200000.0,
        "GDTYP": 2,
        "P_ALP": 33.0,
        "P_BET": 45.0,
        "P_GAM": -97.0,
        "XCENT": -97.0,
        "YCENT": 40.0,
        "IOAPI_VERSION": "3.2",
    }

    # Data variables
    # CMAQ usually has TFLAG as (TSTEP, VAR, DATE_TIME)
    tflag = np.zeros((ntime, 1, 2), dtype=np.int32)
    tflag[0, 0, :] = [2023152, 120000]  # June 1, 2023 12:00
    tflag[1, 0, :] = [2023152, 130000]  # June 1, 2023 13:00

    data = {
        "O3": (
            ("TSTEP", "LAY", "ROW", "COL"),
            rng.random((ntime, nlays, nrows, ncols)).astype(np.float32),
        ),
        "NO2": (
            ("TSTEP", "LAY", "ROW", "COL"),
            rng.random((ntime, nlays, nrows, ncols)).astype(np.float32),
        ),
        "TFLAG": (("TSTEP", "VAR", "DATE_TIME"), tflag),
    }

    ds = xr.Dataset(data_vars=data, attrs=attrs)

    # Add units to DataArrays
    ds["O3"].attrs = {"units": "ppmV"}
    ds["NO2"].attrs = {"units": "ppmV"}

    if lazy:
        ds = ds.chunk({"TSTEP": 1, "COL": 5, "ROW": 5})

    return ds


def test_cmaq_preprocess_consistency():
    """Verify that cmaq_preprocess works identically for Eager and Lazy backends."""
    # 1. Create datasets with SAME SEED
    ds_eager = create_mock_cmaq_dataset(lazy=False, seed=42)
    ds_lazy = create_mock_cmaq_dataset(lazy=True, seed=42)

    # 2. Apply preprocess
    ds_eager_res = cmaq_preprocess(ds_eager)
    ds_lazy_res = cmaq_preprocess(ds_lazy)

    # 3. Basic checks
    assert "latitude" in ds_eager_res.coords
    assert "longitude" in ds_eager_res.coords
    assert "time" in ds_eager_res.coords

    # Check dimensions were renamed
    assert "x" in ds_eager_res.dims
    assert "y" in ds_eager_res.dims
    assert "z" in ds_eager_res.dims

    # 4. Consistency check
    # Check units conversion (ppmV -> ppbV)
    assert ds_eager_res.o3.attrs["units"] == "ppbV"
    xr.testing.assert_allclose(ds_eager_res.o3, ds_lazy_res.o3.compute())

    # 5. Laziness check
    assert hasattr(ds_lazy_res.o3.data, "dask")
    assert hasattr(ds_lazy_res.latitude.data, "dask")


def test_cmaq_reader_harmonize():
    """Verify that CMAQReader.harmonize works identically for Eager and Lazy."""
    reader = CMAQReader()
    ds_eager = create_mock_cmaq_dataset(lazy=False, seed=42)
    ds_lazy = create_mock_cmaq_dataset(lazy=True, seed=42)

    # Preprocess first to get standard dims/coords
    ds_eager = cmaq_preprocess(ds_eager)
    ds_lazy = cmaq_preprocess(ds_lazy)

    ds_eager_h = reader.harmonize(ds_eager)
    ds_lazy_h = reader.harmonize(ds_lazy)

    assert "o3" in ds_eager_h.data_vars
    assert "no2" in ds_eager_h.data_vars
    assert "O3" not in ds_eager_h.data_vars

    xr.testing.assert_allclose(ds_eager_h.o3, ds_lazy_h.o3.compute())
    assert hasattr(ds_lazy_h.o3.data, "dask")


def test_cmaq_diagnostics():
    """Verify that diagnostics are added correctly and lazily."""
    # Create dataset with enough variables for a diagnostic (e.g., NOx)
    ds = create_mock_cmaq_dataset(lazy=True, seed=42)

    # NOx spec is ['NO', 'NO2']
    # Our mock has NO2. Let's add NO.
    ds["NO"] = ds["NO2"] * 0.1
    ds["NO"].attrs = {"units": "ppmV"}

    ds_res = cmaq_preprocess(ds)

    assert "nox" in ds_res.data_vars
    assert hasattr(ds_res.nox.data, "dask")
    assert ds_res.nox.attrs["units"] == "ppbV"  # Should be synced to ppbV


if __name__ == "__main__":
    pytest.main([__file__])


def test_add_ioapi_latlon_aero_protocol():
    """Verify _add_ioapi_latlon follows the Aero Protocol (Eager vs Lazy identity)."""
    from monetio.readers.base import _add_ioapi_latlon

    # 1. Setup mock data
    ds = xr.Dataset({"O3": (("y", "x"), np.random.rand(5, 5))})
    ds.attrs.update({"NCOLS": 5, "NROWS": 5, "XORIG": 0, "YORIG": 0, "XCELL": 1000, "YCELL": 1000})
    proj4 = "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"

    # 2. Eager execution
    ds_eager = _add_ioapi_latlon(ds.copy(), proj4)

    # 3. Lazy execution
    ds_lazy = ds.chunk({"x": 3, "y": 3})
    ds_lazy_out = _add_ioapi_latlon(ds_lazy, proj4)

    # 4. Assertions
    from dask.array import Array

    assert isinstance(ds_lazy_out.latitude.data, Array)

    xr.testing.assert_allclose(ds_eager.latitude, ds_lazy_out.latitude.compute())
    assert "Generated Latitude/Longitude coordinates" in ds_eager.attrs["history"]


def test_cmaq_reader_use_dask_keeps_processed_species_lazy(tmp_path):
    import dask.array as da

    source = tmp_path / "cmaq-lazy.nc"
    ds_source = create_mock_cmaq_dataset(lazy=False)
    ds_source["UNUSED"] = ("TSTEP", np.full(2, -1, dtype="f4"))
    ds_source.to_netcdf(source)
    reader = CMAQReader()

    eager = reader.open_dataset(str(source), use_dask=False, drop_variables=["UNUSED"])
    lazy = reader.open_dataset(str(source), use_dask=True, drop_variables=["UNUSED"])

    assert "UNUSED" not in eager
    assert "UNUSED" not in lazy
    assert isinstance(lazy.o3.data, da.Array)
    assert isinstance(lazy.no2.data, da.Array)
    assert lazy.o3.chunks is not None
    xr.testing.assert_allclose(eager.o3, lazy.o3.compute())
    xr.testing.assert_allclose(eager.no2, lazy.no2.compute())

    deferred = reader.open_dataset(
        str(source), defer=True, chunks={"TSTEP": 1}, drop_variables=["UNUSED"]
    )
    assert isinstance(deferred.o3.data, da.Array)
    time_dim = next(dim for dim in deferred.o3.dims if dim.lower() in {"time", "tstep"})
    assert deferred.o3.chunksizes[time_dim] == (1, 1)
    xr.testing.assert_allclose(eager.o3, deferred.o3.compute())
