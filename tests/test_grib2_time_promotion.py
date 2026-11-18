"""Regression tests: per-file scalar valid_time promotion for GRIB2 concat.

Without promotion, concatenating grib2io per-file datasets along a new
``time`` dimension keeps only the first file's scalar valid_time, leaving
the result with a time dimension but no time values.
"""

import numpy as np
import xarray as xr

from monetio.readers.drivers import _promote_grib2_scalar_time


def _file_ds(valid_time, ref_time, lead_hours):
    ds = xr.Dataset({"aod": (("y", "x"), np.ones((2, 2), dtype=np.float32))})
    return ds.assign_coords(
        valid_time=np.datetime64(valid_time),
        forecast_reference_time=np.datetime64(ref_time),
        lead_time=np.timedelta64(lead_hours, "h"),
    )


def test_promotion_adds_time_dimension_and_values():
    ds = _promote_grib2_scalar_time(_file_ds("2025-08-01T03", "2025-08-01T00", 3))
    assert ds.dims["time"] == 1
    assert ds["aod"].dims == ("time", "y", "x")
    assert ds["time"].values[0] == np.datetime64("2025-08-01T03")
    assert ds["lead_time"].dims == ("time",)
    assert ds["forecast_reference_time"].dims == ("time",)


def test_concatenation_keeps_per_sample_times():
    files = [
        _file_ds("2025-08-01T00", "2025-08-01T00", 0),
        _file_ds("2025-08-01T03", "2025-08-01T00", 3),
        _file_ds("2025-08-02T00", "2025-08-02T00", 0),
    ]
    combined = xr.concat([_promote_grib2_scalar_time(ds) for ds in files], dim="time")
    assert combined.sizes["time"] == 3
    np.testing.assert_array_equal(
        combined["time"].values,
        np.array(["2025-08-01T00", "2025-08-01T03", "2025-08-02T00"], dtype="datetime64[ns]"),
    )
    np.testing.assert_array_equal(
        combined["lead_time"].values,
        np.array([0, 3, 0], dtype="timedelta64[h]").astype("timedelta64[ns]"),
    )


def test_promotion_noop_when_time_dim_exists():
    ds = _file_ds("2025-08-01T00", "2025-08-01T00", 0).expand_dims("time")
    out = _promote_grib2_scalar_time(ds)
    assert out is ds


def test_promotion_noop_without_valid_time():
    ds = xr.Dataset({"aod": (("y", "x"), np.ones((2, 2)))})
    out = _promote_grib2_scalar_time(ds)
    assert out is ds