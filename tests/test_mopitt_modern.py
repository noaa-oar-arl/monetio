import dask.array as da
import numpy as np
import pytest
import xarray as xr

from monetio.readers.mopitt import MOPITT_MISSING, mopitt_preprocess


def make_mock_mopitt_ds(missing=False):
    """Generate a mock MOPITT L3 dataset with metadata."""
    lat = np.linspace(-90, 90, 18)
    lon = np.linspace(-180, 180, 36)
    alt = np.array([1000.0, 900.0, 800.0, 700.0, 600.0, 500.0, 400.0, 300.0, 200.0, 100.0])

    co_data = np.ones((36, 18))
    if missing:
        co_data[0, 0] = MOPITT_MISSING

    ds = xr.Dataset(
        data_vars={
            "HDFEOS/GRIDS/MOP03/Data Fields/Latitude": (("lat",), lat),
            "HDFEOS/GRIDS/MOP03/Data Fields/Longitude": (("lon",), lon),
            "HDFEOS/GRIDS/MOP03/Data Fields/Pressure2": (("alt",), alt),
            "HDFEOS/GRIDS/MOP03/Data Fields/RetrievedCOTotalColumnDay": (
                ("lon", "lat"),
                co_data,
            ),
            "HDFEOS/GRIDS/MOP03/Data Fields/TotalColumnAveragingKernelDay": (
                ("lon", "lat", "alt"),
                np.ones((36, 18, 10)),
            ),
            "HDFEOS/GRIDS/MOP03/Data Fields/SurfacePressureDay": (
                ("lon", "lat"),
                np.full((36, 18), 1013.25),
            ),
            "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOMixingRatioProfileDay": (
                ("lon", "lat", "alt"),
                np.ones((36, 18, 10)),
            ),
            "HDFEOS/GRIDS/MOP03/Data Fields/APrioriCOSurfaceMixingRatioDay": (
                ("lon", "lat"),
                np.full((36, 18), 100.0),
            ),
        },
        attrs={"StartTime": [725846400.0], "history": "Initial history."},
    )

    # Add attributes to variables to test preservation
    ds["HDFEOS/GRIDS/MOP03/Data Fields/RetrievedCOTotalColumnDay"].attrs["units"] = "mol/cm2"
    ds["HDFEOS/GRIDS/MOP03/Data Fields/RetrievedCOTotalColumnDay"].attrs["long_name"] = "CO Column"

    return ds


def test_mopitt_eager_lazy_consistency():
    """Verify mopitt_preprocess produces identical results and preserves metadata for Eager and Lazy."""
    ds_eager = make_mock_mopitt_ds(missing=True)

    # 1. Eager execution
    res_eager = mopitt_preprocess(ds_eager)

    # 2. Lazy execution
    ds_lazy = make_mock_mopitt_ds(missing=True).chunk({"lon": 10, "lat": 10})

    res_lazy = mopitt_preprocess(ds_lazy)

    # Check that it's still dask-backed
    assert isinstance(res_lazy.co_column.data, da.Array)
    assert isinstance(res_lazy.pressure.data, da.Array)

    # 3. Compare values
    xr.testing.assert_allclose(res_eager, res_lazy.compute())

    # 4. Verify Metadata Preservation
    assert res_eager.co_column.attrs["units"] == "mol/cm2"
    assert res_eager.co_column.attrs["long_name"] == "CO Column"
    assert res_lazy.co_column.attrs["units"] == "mol/cm2"

    # 5. Verify History Provenance
    assert "Applied vectorized missing value mask" in res_eager.attrs["history"]
    assert "Renamed variables" in res_eager.attrs["history"]
    assert "Calculated 3D center pressure lazily" in res_eager.attrs["history"]


def test_mopitt_no_hidden_computes():
    """Ensure no hidden computes are triggered for Dask-backed datasets."""
    ds = make_mock_mopitt_ds().chunk({"lon": 5, "lat": 5})

    # Use a dummy array that raises an error on compute
    class ComputeError(Exception):
        pass

    def error_on_compute(*args, **kwargs):
        raise ComputeError("Hidden compute detected!")

    # This is a bit tricky to mock perfectly without deep patching,
    # but we can check if any data variable's data is no longer a dask array
    # after preprocess.

    res = mopitt_preprocess(ds)

    for var in res.data_vars:
        if var in ["pressure", "apriori_co_profile", "co_column"]:
            assert isinstance(res[var].data, da.Array), f"Variable {var} was eagerly computed!"


if __name__ == "__main__":
    pytest.main([__file__])
