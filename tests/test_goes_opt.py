import numpy as np
import xarray as xr

from monetio.readers.goes import goes_preprocess


def _get_mock_goes():
    """Returns a mock GOES dataset for testing."""
    x = np.linspace(-0.1, 0.1, 10).astype(np.float32)
    y = np.linspace(0.1, -0.1, 8).astype(np.float32)

    ds = xr.Dataset(
        {"AOD": (("y", "x"), np.random.rand(8, 10).astype(np.float32))}, coords={"x": x, "y": y}
    )

    # Add fake projection info
    proj = xr.DataArray(
        np.int32(0),
        attrs={
            "perspective_point_height": 35786023.0,
            "semi_major_axis": 6378137.0,
            "semi_minor_axis": 6356752.31414,
            "inverse_flattening": 298.257222103,
            "latitude_of_projection_origin": 0.0,
            "longitude_of_projection_origin": -75.0,
            "sweep_angle_axis": "x",
            "grid_mapping_name": "geostationary",
        },
    )
    ds["goes_imager_projection"] = proj
    ds.attrs["time_coverage_start"] = "2023-01-01T00:00:00Z"
    return ds


def test_goes_opt_eager_vs_lazy():
    """Verify that Eager (NumPy) and Lazy (Dask) outputs are identical."""
    ds = _get_mock_goes()

    # 1. Eager execution
    ds_eager = goes_preprocess(ds.copy())

    # 2. Lazy execution
    ds_lazy = ds.chunk({"x": 5, "y": 4})
    ds_lazy_out = goes_preprocess(ds_lazy)

    # Check that it's still lazy
    # Note: Xarray might sometimes compute coordinates eagerly if they are used for alignment
    # or if the dataset is very small. We check the data variables if they were chunked,
    # or we check if the underlying array is a dask array.
    from dask.array import Array

    assert isinstance(ds_lazy_out.latitude.data, Array)
    assert isinstance(ds_lazy_out.longitude.data, Array)

    # Compute and compare
    ds_lazy_computed = ds_lazy_out.compute()

    xr.testing.assert_allclose(ds_eager.latitude, ds_lazy_computed.latitude)
    xr.testing.assert_allclose(ds_eager.longitude, ds_lazy_computed.longitude)

    # Check history
    assert (
        "Optimized GOES coordinate generation using standardized preprocessing."
        in ds_eager.attrs["history"]
    )
    assert "Preprocessed GOES data." in ds_eager.attrs["history"]


def test_goes_opt_values():
    """Check that results are scientifically reasonable."""
    ds = _get_mock_goes()
    ds_out = goes_preprocess(ds)

    # Results should not be all NaN
    assert not np.isnan(ds_out.latitude.values).all()
    assert not np.isnan(ds_out.longitude.values).all()

    # Latitude should be within reasonable bounds
    assert np.nanmin(ds_out.latitude.values) > -90
    assert np.nanmax(ds_out.latitude.values) < 90

    # Longitude should be centered around -75 (based on mock)
    assert np.nanmean(ds_out.longitude.values) < 0


def test_add_goes_latlon_aero_protocol():
    """Verify _add_goes_latlon follows the Aero Protocol (Eager vs Lazy identity)."""
    from monetio.readers.goes import _add_goes_latlon

    # 1. Setup mock data
    x = np.linspace(-0.1, 0.1, 10).astype(np.float32)
    y = np.linspace(0.1, -0.1, 8).astype(np.float32)
    ds = xr.Dataset(
        {"AOD": (("y", "x"), np.random.rand(8, 10).astype(np.float32))}, coords={"x": x, "y": y}
    )
    ds["goes_imager_projection"] = (
        (),
        np.int32(0),
        {
            "perspective_point_height": 35786023.0,
            "semi_major_axis": 6378137.0,
            "semi_minor_axis": 6356752.31414,
            "inverse_flattening": 298.257222103,
            "latitude_of_projection_origin": 0.0,
            "longitude_of_projection_origin": -75.0,
            "sweep_angle_axis": "x",
            "grid_mapping_name": "geostationary",
        },
    )

    # 2. Eager execution
    ds_eager = _add_goes_latlon(ds.copy())

    # 3. Lazy execution
    ds_lazy = ds.chunk({"x": 5, "y": 4})
    ds_lazy_out = _add_goes_latlon(ds_lazy)

    # 4. Assertions
    from dask.array import Array

    assert isinstance(ds_lazy_out.latitude.data, Array)
    assert isinstance(ds_lazy_out.longitude.data, Array)

    xr.testing.assert_allclose(ds_eager.latitude, ds_lazy_out.latitude.compute())
    xr.testing.assert_allclose(ds_eager.longitude, ds_lazy_out.longitude.compute())
    assert (
        "Generated Latitude/Longitude coordinates" in ds_eager.attrs["history"]
        or "Optimized GOES coordinate generation" in ds_eager.attrs["history"]
    )
