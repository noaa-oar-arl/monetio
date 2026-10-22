import numpy as np
import pytest
import xarray as xr

from monetio.readers.base import _add_ioapi_latlon


def create_mock_ioapi_dataset(lazy=False):
    """Creates a mock IOAPI-compliant dataset."""
    # LCC Projection example
    proj4 = "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

    # Grid metadata
    ncols = 10
    nrows = 10
    xorig = -1000000.0
    yorig = -1000000.0
    xcell = 200000.0
    ycell = 200000.0

    ds = xr.Dataset(
        data_vars={"test_var": (("y", "x"), np.random.rand(nrows, ncols))},
        attrs={
            "NCOLS": ncols,
            "NROWS": nrows,
            "XORIG": xorig,
            "YORIG": yorig,
            "XCELL": xcell,
            "YCELL": ycell,
            "proj4_srs": proj4,
        },
    )

    if lazy:
        ds = ds.chunk({"x": 5, "y": 5})

    return ds, proj4


def test_add_ioapi_latlon_consistency():
    """Verify that _add_ioapi_latlon works identically for Eager and Lazy backends."""
    # 1. Create datasets
    ds_eager, proj4 = create_mock_ioapi_dataset(lazy=False)
    ds_lazy, _ = create_mock_ioapi_dataset(lazy=True)

    # 2. Apply function
    ds_eager_res = _add_ioapi_latlon(ds_eager, proj4)
    ds_lazy_res = _add_ioapi_latlon(ds_lazy, proj4)

    # 3. Basic checks
    assert "latitude" in ds_eager_res.coords
    assert "longitude" in ds_eager_res.coords
    assert "latitude" in ds_lazy_res.coords
    assert "longitude" in ds_lazy_res.coords

    # 4. Consistency check
    # We must compute the lazy one for comparison
    xr.testing.assert_allclose(ds_eager_res.latitude, ds_lazy_res.latitude.compute())
    xr.testing.assert_allclose(ds_eager_res.longitude, ds_lazy_res.longitude.compute())

    # 5. Laziness check
    assert hasattr(ds_lazy_res.latitude.data, "dask")
    assert hasattr(ds_lazy_res.longitude.data, "dask")

    # 6. Attributes check
    assert ds_eager_res.latitude.attrs["units"] == "degree_north"
    assert "PROJ inversion" in ds_eager_res.attrs["history"]


if __name__ == "__main__":
    pytest.main([__file__])
