import dask.array as da
import numpy as np
import xarray as xr

from monetio.readers.sat_utils import tai93_to_datetime
from monetio.readers.time_utils import (
    parse_ioapi_times,
    parse_wrf_times,
    parse_yyyymmdd_hhmm,
)


def test_parse_ioapi_times_eager_lazy():
    """Verify parse_ioapi_times logic for both Eager and Lazy backends."""
    dates = np.array([2023001, 2023001])
    times = np.array([120000, 130000])

    # 1. Eager
    res_eager = parse_ioapi_times(dates, times)
    assert res_eager[0] == np.datetime64("2023-01-01T12:00:00")

    # 2. Lazy (Dask)
    ds = xr.Dataset(
        {
            "dates": (("time"), da.from_array(dates, chunks=1)),
            "times": (("time"), da.from_array(times, chunks=1)),
        }
    )
    res_lazy = xr.apply_ufunc(
        parse_ioapi_times,
        ds.dates,
        ds.times,
        dask="parallelized",
        output_dtypes=[np.dtype("datetime64[ns]")],
    ).compute()

    np.testing.assert_array_equal(res_eager, res_lazy.values)


def test_parse_wrf_times_eager_lazy():
    """Verify parse_wrf_times logic for both Eager and Lazy backends."""
    times_bytes = np.array([b"2023-01-01_12:00:00", b"2023-01-01_13:00:00"])

    # 1. Eager
    res_eager = parse_wrf_times(times_bytes)
    assert res_eager[0] == np.datetime64("2023-01-01T12:00:00")

    # 2. Lazy
    ds = xr.Dataset({"times": (("time"), da.from_array(times_bytes, chunks=1))})
    res_lazy = xr.apply_ufunc(
        parse_wrf_times, ds.times, dask="parallelized", output_dtypes=[np.dtype("datetime64[ns]")]
    ).compute()

    np.testing.assert_array_equal(res_eager, res_lazy.values)


def test_parse_yyyymmdd_hhmm_eager_lazy():
    """Verify parse_yyyymmdd_hhmm logic for both Eager and Lazy backends."""
    yyyymmdd = np.array([20230101, 20230101])
    hhmm = np.array([1200, 1300])

    # 1. Eager
    res_eager = parse_yyyymmdd_hhmm(yyyymmdd, hhmm)
    assert res_eager[0] == np.datetime64("2023-01-01T12:00:00")

    # 2. Lazy
    ds = xr.Dataset(
        {
            "y": (("time"), da.from_array(yyyymmdd, chunks=1)),
            "h": (("time"), da.from_array(hhmm, chunks=1)),
        }
    )
    res_lazy = xr.apply_ufunc(
        parse_yyyymmdd_hhmm,
        ds.y,
        ds.h,
        dask="parallelized",
        output_dtypes=[np.dtype("datetime64[ns]")],
    ).compute()

    np.testing.assert_array_equal(res_eager, res_lazy.values)


def test_tai93_to_datetime_eager_lazy():
    """Verify tai93_to_datetime logic for both Eager and Lazy backends."""
    # 1993-01-01 00:00:00 + 0s = 1993-01-01
    times = xr.DataArray([0.0, 3600.0], dims=("time",))

    # 1. Eager
    res_eager = tai93_to_datetime(times)
    assert res_eager[0].values == np.datetime64("1993-01-01T00:00:00")

    # 2. Lazy
    res_lazy = tai93_to_datetime(times.chunk(1))
    assert res_lazy.chunks is not None

    xr.testing.assert_allclose(res_eager, res_lazy.compute())
