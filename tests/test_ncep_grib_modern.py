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
