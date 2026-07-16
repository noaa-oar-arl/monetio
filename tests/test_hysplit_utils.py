import numpy as np
import pytest
import xarray as xr

from monetio import hysplit
from monetio.readers.hysplit import check_grid_continuity, fix_grid_continuity


def test_003():
    """
    tests for getlatlon and  get_latlongrid functions.
    tests global grids.
    """
    # Longitude
    # center = 1
    # spacing = 0.5
    # span = 360
    # --------------------
    # Latitude
    # center = 1
    # spacing = 0.5
    # span = 180.0
    # ------------
    attrs = {
        "llcrnr latitude": -90.0,
        "llcrnr longitude": -180.0,
        "Latitude Spacing": 0.5,
        "Longitude Spacing": 0.5,
        "Number Lat Points": 361,
        "Number Lon Points": 721,
    }
    xxx = [1, 5, 93]
    xanswers = [165.0, 167.0, -149.0]
    yyy = [1, 17, 93]
    yanswers = [0.0, 8.0, 46.0]
    grid = hysplit.get_latlongrid(attrs, xxx, yyy)  # noqa: F841
    latlist, lonlist = hysplit.getlatlon(attrs)
    gridanswer = np.meshgrid(xanswers, yanswers)  # noqa: F841

    assert len(latlist) == 361
    assert len(lonlist) == 721
    # check that lat lon begin and end in correct place.
    assert latlist[0] == -90.0
    assert latlist[-1] == 90.0
    assert lonlist[0] == -180.0
    # should go -180 to 180 or -180 to -180?
    assert lonlist[-1] == 180.0
    assert lonlist[-2] == 179.5


def test_002():
    """
    tests for getlatlon and  get_latlongrid functions.
    tests medium grid with 90 degree span which crosses date line.
    """
    # Longitude
    # center  = -150
    # spacing =  0.5
    # span    = 90.0
    # ------------
    # crnr    = 165
    # nlon    = 181
    # x=1  longitude = 165
    # x=5  longitude = 167.0
    # x=93 longitude = -149.0

    # Latitude
    # center = 45
    # spacing = 0.5
    # span = 90.0
    # ------------
    # crnr = 0.0
    # nlat = 181
    # y=1  latitude=0.0
    # y=17 latitude=8
    # y=93 latitude=46.0

    attrs = {
        "llcrnr latitude": 0.0,
        "llcrnr longitude": 165,
        "Latitude Spacing": 0.5,
        "Longitude Spacing": 0.5,
        "Number Lat Points": 181.0,
        "Number Lon Points": 181.0,
    }
    xxx = [1, 5, 93]
    xanswers = [165.0, 167.0, -149.0]
    yyy = [1, 17, 93]
    yanswers = [0.0, 8.0, 46.0]
    grid = hysplit.get_latlongrid(attrs, xxx, yyy)
    latlist, lonlist = hysplit.getlatlon(attrs)

    gridanswer = np.meshgrid(xanswers, yanswers)
    assert np.array_equal(grid, gridanswer)

    assert len(latlist) == 181
    assert len(lonlist) == 181
    # check that lat lon begin and end in correct place.
    assert latlist[0] == 0.0
    assert latlist[-1] == 90.0
    assert lonlist[0] == 165
    assert lonlist[-1] == -105.0

    for x in zip(xxx, xanswers):
        assert lonlist[x[0] - 1] == x[1]

    for y in zip(yyy, yanswers):
        assert latlist[y[0] - 1] == y[1]


def test_001():
    """
    tests for getlatlon and  get_latlongrid functions.
    tests simple small grid with 10 degree span.
    """
    # Longitude
    # center  = -150
    # spacing =  0.1
    # span    = 10.0
    # ------------
    # crnr    = -155
    # nlon    = 101
    # x=1  longitude = -155
    # x=33 longitude = -151.8
    # x=69 longitude = -148.2

    # Latitude
    # center = 45
    # spacing = 0.1
    # span = 10.0
    # ------------
    # crnr = 40.0
    # nlat = 101
    # y=1  latitude=40.0
    # y=50 latitude=44.9
    # y=73 latitude=47.2

    attrs = {
        "llcrnr latitude": 40.0,
        "llcrnr longitude": -155,
        "Latitude Spacing": 0.1,
        "Longitude Spacing": 0.1,
        "Number Lat Points": 101.0,
        "Number Lon Points": 101.0,
    }

    xxx = [1, 33, 69]
    yyy = [1, 50, 73]
    xanswers = [-155, -151.8, -148.2]
    yanswers = [40.0, 44.9, 47.2]
    grid = hysplit.get_latlongrid(attrs, xxx, yyy)
    gridanswer = np.meshgrid(xanswers, yanswers)
    assert np.array_equal(grid, gridanswer)

    latlist, lonlist = hysplit.getlatlon(attrs)
    assert len(latlist) == 101
    assert len(lonlist) == 101
    assert latlist[0] == 40.0
    assert latlist[-1] == 50.0
    assert lonlist[0] == -155.0
    assert lonlist[-1] == -145.0

    for x in zip(xxx, xanswers):
        assert lonlist[x[0] - 1] == x[1]

    for y in zip(yyy, yanswers):
        assert latlist[y[0] - 1] == y[1]


@pytest.fixture
def mock_hysplit_ds():
    """Create a mock HYSPLIT dataset with a gap in the grid."""
    attrs = {
        "llcrnr latitude": 30.0,
        "llcrnr longitude": -100.0,
        "Number Lat Points": 10,
        "Number Lon Points": 10,
        "Latitude Spacing": 1.0,
        "Longitude Spacing": 1.0,
    }

    # Grid with a gap: x=1,2, 4,5 (missing 3)
    x = np.array([1, 2, 4, 5])
    y = np.array([1, 2, 3])

    data = np.random.rand(1, 1, 3, 4)  # time, z, y, x

    ds = xr.Dataset(
        {"CONC": (("time", "z", "y", "x"), data)},
        coords={
            "time": [np.datetime64("2020-01-01")],
            "z": [100],
            "y": y,
            "x": x,
        },
        attrs=attrs,
    )
    return ds


def test_check_grid_continuity(mock_hysplit_ds):
    # Should be False due to gap in x
    assert check_grid_continuity(mock_hysplit_ds) is False

    # Fix it
    ds_fixed = mock_hysplit_ds.reindex(x=[1, 2, 3, 4, 5], fill_value=0)
    assert check_grid_continuity(ds_fixed) is True


def test_fix_grid_continuity_eager_lazy_consistency(mock_hysplit_ds):
    """Verify fix_grid_continuity works identically for NumPy and Dask backends."""
    # Eager (NumPy)
    ds_eager = fix_grid_continuity(mock_hysplit_ds)

    # Lazy (Dask)
    ds_lazy_input = mock_hysplit_ds.chunk({"x": 2})
    ds_lazy = fix_grid_continuity(ds_lazy_input)

    # Check that it's still dask-backed
    assert ds_lazy.CONC.chunks is not None

    # Verify results are identical
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())

    # Verify grid is continuous
    assert ds_eager.x.size == 5
    assert (ds_eager.x.values == np.arange(1, 6)).all()
    assert ds_eager.CONC.sel(x=3).isnull().all() or (ds_eager.CONC.sel(x=3) == 0).all()


def test_fix_grid_continuity_provenance(mock_hysplit_ds):
    ds_fixed = fix_grid_continuity(mock_hysplit_ds)
    assert "Fixed grid continuity" in ds_fixed.attrs["history"]


def test_mass_loading_lazy_consistency():
    from monetio.readers.hysplit import mass_loading

    # Create a 3D dataset (z, y, x)
    z = np.array([100, 200, 300])
    y = np.arange(5)
    x = np.arange(5)
    data = np.random.rand(3, 5, 5)

    da = xr.DataArray(
        data,
        dims=("z", "y", "x"),
        coords={"z": z, "y": y, "x": x},
        name="CONC",
        attrs={"history": "Initial data."},
    )

    # delta = [100, 100, 100]
    delta = np.array([100, 100, 100])

    # Eager
    ml_eager = mass_loading(da, delta=delta)

    # Lazy
    da_lazy = da.chunk({"z": 1})
    ml_lazy = mass_loading(da_lazy, delta=delta)

    assert ml_lazy.chunks is not None
    xr.testing.assert_allclose(ml_eager, ml_lazy.compute())
    assert "Calculated mass loading" in ml_lazy.attrs["history"]


def test_mass_loading_with_deposition():
    from monetio.readers.hysplit import mass_loading

    # z=0 is deposition layer
    z = np.array([0, 100, 200])
    data = np.ones((3, 2, 2))
    da = xr.DataArray(data, dims=("z", "y", "x"), coords={"z": z}, name="CONC")

    # thickness of deposition layer is 0
    delta = np.array([0, 100, 100])

    ml = mass_loading(da, delta=delta)

    # Should only sum z=100 and z=200
    # Expected: 1*100 + 1*100 = 200
    assert (ml == 200).all()
