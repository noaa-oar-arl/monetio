import dask.array as da
import numpy as np
import xarray as xr

from monetio.readers.ncep_grib import NCEPGribReader, ncep_grib_preprocess


def test_ncep_grib_preprocess():
    # Create dummy dataset with 1D coordinates
    ny, nx = 4, 5
    ds = xr.Dataset(
        {
            "VAR1": (("lat_0", "lon_0"), np.random.rand(ny, nx)),
        },
        coords={
            "lat_0": np.linspace(-90, 90, ny),
            "lon_0": np.linspace(-180, 180, nx),
        },
    )

    ds_out = ncep_grib_preprocess(ds)

    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert ds_out.latitude.ndim == 2
    assert ds_out.longitude.ndim == 2
    assert ds_out.latitude.shape == (ny, nx)
    assert ds_out.y.size == ny
    assert ds_out.x.size == nx


def test_ncep_grib_eager_lazy_consistency():
    ny, nx = 10, 12
    data = np.random.rand(ny, nx).astype("f4")
    ds_eager = xr.Dataset(
        {
            "VAR1": (("lat_0", "lon_0"), data),
        },
        coords={
            "lat_0": np.linspace(-90, 90, ny),
            "lon_0": np.linspace(-180, 180, nx),
        },
    )

    ds_lazy = ds_eager.chunk({"lat_0": 5, "lon_0": 6})

    out_eager = ncep_grib_preprocess(ds_eager)
    out_lazy = ncep_grib_preprocess(ds_lazy)

    xr.testing.assert_allclose(out_eager, out_lazy.compute())
    assert isinstance(out_lazy.latitude.data, da.Array)


def test_ncep_grib_reader_open(monkeypatch):
    class MockDriver:
        def open(self, files, **kwargs):
            # Just return a simple dataset
            return xr.Dataset({"test": 1})

    reader = NCEPGribReader()
    monkeypatch.setattr(reader, "driver", MockDriver())

    ds = reader.open_dataset("dummy.grib2")
    assert "test" in ds.data_vars
    assert "history" in ds.attrs


def _grib2io_like_dataset(valid_date):
    """A single-file dataset shaped like grib2io output: y/x dims, scalar validDate/refDate."""
    ny, nx = 3, 4
    return xr.Dataset(
        {"VAR1": (("y", "x"), np.zeros((ny, nx), dtype="f4"))},
        coords={
            "validDate": np.datetime64(valid_date, "us"),
            "refDate": np.datetime64(valid_date, "us"),
            "leadTime": np.timedelta64(0, "us"),
        },
    )


def test_ncep_grib_preprocess_promotes_scalar_valid_date():
    """A scalar validDate becomes a size-1 'time' dimension so concatenation keeps the label."""
    out = ncep_grib_preprocess(_grib2io_like_dataset("2025-03-24T00"))

    assert "time" in out.dims
    assert out.sizes["time"] == 1
    assert "time" in out.coords
    assert out["time"].ndim == 1
    assert out["time"].values[0] == np.datetime64("2025-03-24T00", "us")


def test_ncep_grib_preprocess_concat_preserves_distinct_times():
    """Distinct per-file valid dates survive nested/minimal concatenation as a 1-D time index.

    Reproduces the multi-file GRIB2 scan where ``coords='minimal'`` would otherwise
    collapse the scalar time coords, leaving an unindexed ``time`` dimension that
    cannot align against observations.
    """
    ds_a = ncep_grib_preprocess(_grib2io_like_dataset("2025-03-24T00"))
    ds_b = ncep_grib_preprocess(_grib2io_like_dataset("2025-03-24T03"))

    combined = xr.concat([ds_a, ds_b], dim="time", coords="minimal", compat="override")

    assert combined.sizes["time"] == 2
    assert "time" in combined.coords
    np.testing.assert_array_equal(
        combined["time"].values,
        np.array(["2025-03-24T00", "2025-03-24T03"], dtype="datetime64[us]"),
    )


def test_ncep_grib_preprocess_leaves_existing_time_untouched():
    """A file that already carries a time dimension is not expanded again."""
    ds = xr.Dataset(
        {"VAR1": (("time", "y", "x"), np.zeros((2, 3, 4), dtype="f4"))},
        coords={
            "time": np.array(["2025-03-24T00", "2025-03-24T03"], dtype="datetime64[us]"),
            "validDate": np.datetime64("2025-03-24T00", "us"),
        },
    )

    out = ncep_grib_preprocess(ds)

    assert out.sizes["time"] == 2
    assert out["time"].ndim == 1


def test_ncep_grib_preprocess_falls_back_to_ref_date():
    """Without validDate, the reference time (refDate) names the cycle."""
    ds = _grib2io_like_dataset("2025-03-24T00").drop_vars("validDate")

    out = ncep_grib_preprocess(ds)

    assert out["time"].values[0] == np.datetime64("2025-03-24T00", "us")


def test_ncep_grib_preprocess_aero_protocol():
    """Verify ncep_grib_preprocess follows the Aero Protocol (Eager vs Lazy identity)."""
    # 1. Setup mock data
    lon = np.linspace(0, 359, 10)
    lat = np.linspace(-90, 90, 5)
    ds = xr.Dataset(
        {"TMP_2maboveground": (("lat", "lon"), np.random.rand(5, 10))},
        coords={"latitude": (("lat",), lat), "longitude": (("lon",), lon)},
    )

    # 2. Eager execution
    ds_eager = ncep_grib_preprocess(ds.copy())

    # 3. Lazy execution
    ds_lazy = ds.chunk({"lat": 3, "lon": 5})
    ds_lazy_out = ncep_grib_preprocess(ds_lazy)

    # 4. Assertions
    from dask.array import Array

    assert isinstance(ds_lazy_out.latitude.data, Array)

    xr.testing.assert_allclose(ds_eager.latitude, ds_lazy_out.latitude.compute())
    assert "Generated 2D latitude/longitude coordinates lazily" in ds_eager.attrs["history"]
