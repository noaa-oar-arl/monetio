import numpy as np
import xarray as xr

from monetio.readers.grib2 import Grib2Reader


def test_grib2_reader_open():
    ds = xr.Dataset(
        {"tmp": (("lat_0", "lon_0"), np.random.rand(10, 20))},
        coords={"lat_0": np.arange(10), "lon_0": np.arange(20)},
    )

    reader = Grib2Reader()

    import unittest.mock as mock

    with mock.patch("monetio.readers.drivers.XarrayDriver.open", return_value=ds):
        res = reader.open_dataset("dummy.grib2")

    assert "latitude" in res.coords
    assert "longitude" in res.coords
    assert "history" in res.attrs
    assert "grib2io" in res.attrs["history"]


def test_grib2_reader_harmonize():
    ds = xr.Dataset(
        {"tmp": (("lat", "lon"), np.random.rand(10, 20))},
        coords={
            "lat": np.arange(10),
            "lon": np.arange(20),
            "valid_time": [np.datetime64("2023-01-01")],
        },
    )

    reader = Grib2Reader()
    res = reader.harmonize(ds)

    assert "latitude" in res.coords
    assert "longitude" in res.coords
    # valid_time should be renamed to time
    assert "time" in res.variables


def test_ncep_grib_eager_lazy(tmp_path):
    """Test NCEPGribReader with both eager and lazy backends."""
    from monetio.readers.ncep_grib import NCEPGribReader

    nlat = 10
    nlon = 20

    ds_mock = xr.Dataset(
        {
            "TMP_P0_L1_GLL0": (("lat_0", "lon_0"), np.random.rand(nlat, nlon).astype(np.float32)),
        }
    )
    ds_mock.coords["lat_0"] = np.linspace(90, -90, nlat).astype(np.float32)
    ds_mock.coords["lon_0"] = np.linspace(0, 359, nlon).astype(np.float32)

    # Add some attributes with whitespace to test hygiene
    ds_mock.attrs["TITLE"] = "  Mock NCEP GRIB Data  "
    ds_mock.TMP_P0_L1_GLL0.attrs["units"] = " K "

    fname = tmp_path / "test_ncep_grib.nc"
    ds_mock.to_netcdf(fname, engine="h5netcdf")

    reader = NCEPGribReader()

    # 1. Eager Mode
    ds_eager = reader.open_dataset(files=str(fname), lazy=False, engine="h5netcdf")

    assert isinstance(ds_eager, xr.Dataset)
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords
    assert ds_eager.latitude.ndim == 2
    assert ds_eager.longitude.ndim == 2
    assert ds_eager.attrs["TITLE"] == "Mock NCEP GRIB Data"
    assert ds_eager.TMP_P0_L1_GLL0.attrs["units"] == "K"

    # 2. Lazy Mode
    ds_lazy = reader.open_dataset(
        files=str(fname), chunks={"lat_0": 5, "lon_0": 10}, engine="h5netcdf"
    )

    assert isinstance(ds_lazy, xr.Dataset)
    # Check if data is dask-backed
    assert hasattr(ds_lazy.TMP_P0_L1_GLL0.data, "dask")
    assert hasattr(ds_lazy.latitude.data, "dask")
    assert hasattr(ds_lazy.longitude.data, "dask")

    # 3. Consistency Check
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )

    # Verify provenance
    assert "history" in ds_eager.attrs
    assert "Generated 2D latitude/longitude coordinates lazily" in ds_eager.attrs["history"]


def test_ncep_grib_variable_promotion(tmp_path):
    """Test that lat_0/lon_0 are promoted to coords if they are only variables."""
    from monetio.readers.ncep_grib import NCEPGribReader

    nlat = 5
    nlon = 10
    ds = xr.Dataset(
        {
            "TMP": (("lat_0", "lon_0"), np.random.rand(nlat, nlon).astype(np.float32)),
            "lat_0": (("lat_0",), np.linspace(90, -90, nlat).astype(np.float32)),
            "lon_0": (("lon_0",), np.linspace(0, 350, nlon).astype(np.float32)),
        }
    )
    # Note: we don't set them as coords
    fname = tmp_path / "test_promotion.nc"
    ds.to_netcdf(fname, engine="h5netcdf")

    reader = NCEPGribReader()
    ds_out = reader.open_dataset(files=str(fname), engine="h5netcdf")

    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert ds_out.latitude.ndim == 2
