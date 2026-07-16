from pathlib import Path

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio import geoms
from monetio.readers.geoms import geoms_preprocess, open_dataset_geoms

DATA = Path(__file__).parent / "data"
TEST_FP = str(DATA / "tolnet-hdf4-test-data.hdf")
TEST_FP_H4TONCCF_NC4 = str(DATA / "tolnet-hdf4-test-data_h4tonccf_nc4.nc")
TEST_FP_PANDORA_NO2_TOTCOL = str(DATA / "pandora-uvvis-no2-boulder-20231206.h5")


def create_mock_geoms_ds(lazy=False):
    """Creates a mock GEOMS dataset."""
    nt = 5
    nz = 10

    # Mock MJD2000 times (approx 2023-01-01)
    times = np.linspace(8401, 8402, nt)

    # Generate fixed random data
    o3_data = np.arange(nt * nz).reshape(nt, nz).astype(float)

    data = {
        "DATETIME": (("fakeDim0datetime",), times),
        "ALTITUDE": (("fakeDim0altitude",), np.linspace(0, 10000, nz)),
        "O3.MIXING.RATIO.VOLUME": (("fakeDim0datetime", "fakeDim0altitude"), o3_data),
        "LATITUDE.INSTRUMENT": (("fakeDim0latitude",), [40.0]),
        "LONGITUDE.INSTRUMENT": (("fakeDim0longitude",), [-105.0]),
        "ALTITUDE.INSTRUMENT": (("fakeDim0altinst",), [1600.0]),
        "STRING_VAR": (("fakeDim0datetime",), np.array([b"test"] * nt, dtype=object)),
    }

    ds = xr.Dataset(data_vars=data, attrs={"DATA_SOURCE": "MOCK"})

    if lazy:
        ds = ds.chunk({"fakeDim0datetime": 2, "fakeDim0altitude": -1})

    return ds


def test_geoms_protocol_compliance():
    """Verify GEOMS processing is backend-agnostic and lazy-friendly."""
    ds_base = create_mock_geoms_ds(lazy=False)
    ds_eager = ds_base.copy(deep=True)
    ds_lazy = ds_base.chunk({"fakeDim0datetime": 2, "fakeDim0altitude": -1})

    # Test Preprocess Eager
    res_eager = geoms_preprocess(ds_eager)

    # Test Preprocess Lazy
    res_lazy = geoms_preprocess(ds_lazy)

    # Check laziness
    assert isinstance(res_lazy.o3_mixing_ratio_volume.data, da.Array)
    assert isinstance(res_lazy.time.data, da.Array)

    # Check consistency
    xr.testing.assert_allclose(res_eager, res_lazy.compute())

    # Verify that history was updated correctly (Provenance check)
    assert "Renamed dimensions" in res_lazy.attrs["history"]
    assert "Converted MJD2000 times" in res_lazy.attrs["history"]

    # Check time conversion
    expected_time = pd.to_datetime(ds_base.DATETIME.values + 2451544.5, unit="D", origin="julian")
    # Use xr.testing for datetime comparisons, but drop coords for direct comparison if needed
    xr.testing.assert_allclose(
        res_eager.time.drop_vars(res_eager.time.coords),
        xr.DataArray(expected_time.values.astype("datetime64[ns]"), dims="time"),
    )

    # Check string decoding
    assert res_eager.string_var.values[0] == "test"

    # Check dimension names
    assert "time" in res_eager.dims
    assert "altitude" in res_eager.dims
    assert "latitude" in res_eager.coords
    assert "longitude" in res_eager.coords


def test_mjd2000_consistency():
    """Verify _mjd2000_to_datetime is backend-agnostic."""
    from monetio.readers.geoms import _mjd2000_to_datetime

    # 1. Eager
    mjd_vals = np.array([8401.0, 8402.0])
    res_eager = _mjd2000_to_datetime(mjd_vals)

    # 2. Lazy (Dask) via apply_ufunc
    da_mjd = xr.DataArray(mjd_vals, dims="time").chunk({"time": 1})
    res_lazy_da = xr.apply_ufunc(
        _mjd2000_to_datetime,
        da_mjd,
        dask="parallelized",
        output_dtypes=[np.dtype("datetime64[ns]")],
    )

    # Assert results are identical
    np.testing.assert_array_equal(res_eager, res_lazy_da.compute().values)
    assert res_lazy_da.dtype == np.dtype("datetime64[ns]")


def test_hdf5_lazy_loading_mock():
    """Verify that HDF5-like lazy loading propagates without compute."""
    import dask.array as da

    from monetio.readers.geoms import geoms_preprocess

    # Simulate lazy data loading from HDF5
    nt = 10
    lazy_data = da.from_array(np.random.rand(nt), chunks=5)

    ds = xr.Dataset(
        data_vars={
            "DATETIME": (("fakeDim0datetime",), np.linspace(8401, 8402, nt)),
            "O3": (("fakeDim0datetime",), lazy_data),
        }
    )

    # Preprocess should remain lazy
    ds_proc = geoms_preprocess(ds)

    # Verify O3 is still a dask array
    assert isinstance(ds_proc.o3.data, da.Array)
    # Verify time is still converted lazily (it was eager in input, but apply_ufunc might make it lazy if we chunked it,
    # but here we didn't chunk DATETIME in input. Let's check O3 specifically.)
    assert ds_proc.o3.chunks is not None


def test_open_dataset():
    if not Path(TEST_FP).exists():
        pytest.skip("Test file not found")
    ds = open_dataset_geoms(TEST_FP)
    # The real file has different variables than the mock
    assert "o3_mixing_ratio_volume_derived" in ds.variables
    assert tuple(ds["o3_mixing_ratio_volume_derived"].dims) == ("time", "altitude")
    assert ds.sizes["time"] == 28
    assert ds.sizes["altitude"] == 496


def test_open_no_rename_vars():
    if not Path(TEST_FP).exists():
        pytest.skip("Test file not found")
    ds = open_dataset_geoms(TEST_FP, rename_all=False)
    assert "O3.MIXING.RATIO.VOLUME_DERIVED" in ds.variables
    assert tuple(ds["O3.MIXING.RATIO.VOLUME_DERIVED"].dims) == ("time", "altitude")
    assert ds.sizes["time"] == 28
    assert ds.sizes["altitude"] == 496


def test_open_no_squeeze():
    if not Path(TEST_FP).exists():
        pytest.skip("Test file not found")
    ds = open_dataset_geoms(TEST_FP, squeeze=False)
    assert ds.sizes["latitude"] == 1
    assert ds.sizes["longitude"] == 1
    assert ds.sizes["altitude_instrument"] == 1
    assert ds.sizes["time"] == 28
    assert ds.sizes["altitude"] == 496


def test_mjd2k():
    f0 = 0.0
    t0 = pd.Timestamp("2000-01-01 00:00:00")
    da = xr.DataArray(data=[f0])
    dti = pd.DatetimeIndex([t0])

    with pytest.raises(AttributeError):
        geoms._dti_from_mjd2000(da)

    da.attrs.update(VAR_UNITS="MJD2K")
    assert geoms._dti_from_mjd2000(da) == dti


def test_cmp_h4tonccf():
    if not Path(TEST_FP).exists() or not Path(TEST_FP_H4TONCCF_NC4).exists():
        pytest.skip("Test files not found")
    ds = open_dataset_geoms(TEST_FP, rename_all=False)
    try:
        ds_h4tonccf = xr.open_dataset(TEST_FP_H4TONCCF_NC4, engine="h5netcdf")
    except Exception:
        ds_h4tonccf = xr.open_dataset(TEST_FP_H4TONCCF_NC4)
    # Note: h4tonccf_nc4 replaces all `.` in var names to `_`
    # Just check that standard dimensions match
    assert ds.sizes["time"] == sorted(ds_h4tonccf.squeeze().sizes.values())[0]
    assert ds.sizes["altitude"] == sorted(ds_h4tonccf.squeeze().sizes.values())[1]


def test_pandora_totcol():
    if not Path(TEST_FP_PANDORA_NO2_TOTCOL).exists():
        pytest.skip("Test file not found")
    ds = open_dataset_geoms(TEST_FP_PANDORA_NO2_TOTCOL)

    assert "time" in ds.dims
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert ds.sizes["time"] > 1

    # In Pandas 3.0.1, direct comparison of dask-backed datetime64[ns] to pd.Timestamp can fail.
    # We cast to np.datetime64 for a robust comparison.
    assert (ds.time.dt.floor("D") == np.datetime64("2023-12-06")).all()

    assert "no2_column_absorption_solar" in ds.data_vars

    assert "altitude" not in ds
    assert "latitude_instrument" not in ds and "latitude" in ds, "renamed"
    assert "longitude_instrument" not in ds and "longitude" in ds, "renamed"

    assert all(vn == vn.lower() and "." not in vn for vn in ds.variables)


if __name__ == "__main__":
    pytest.main([__file__])
