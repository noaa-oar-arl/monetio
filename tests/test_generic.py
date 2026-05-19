import numpy as np
import pandas as pd
import xarray as xr

from monetio.models.generic import open_dataset, open_mfdataset


def _make_ds(*, nz=None):
    """Build a minimal in-memory dataset in raw (pre-MONET) form."""
    nt, ny, nx = 2, 3, 4
    time = pd.date_range("2020-01-01", periods=nt, freq="h")
    lat = np.linspace(20.0, 30.0, ny)
    lon = np.linspace(180.0, 200.0, nx)  # > 180

    time_dim = "tim"
    lev_dim = "lev"
    lat_dim = "lat"
    lon_dim = "lon"

    coords = {"Time": (time_dim, time), "lat": (lat_dim, lat), "lon": (lon_dim, lon)}

    dims_2d = (time_dim, lat_dim, lon_dim)
    dims_3d = (time_dim, lev_dim, lat_dim, lon_dim)

    if nz is not None:
        coords["lev"] = (lev_dim, np.linspace(1000.0, 100.0, nz), {"units": "hPa"})
        dims = dims_3d
        data = np.arange(nt * nz * ny * nx, dtype=float).reshape(nt, nz, ny, nx)
    else:
        dims = dims_2d
        data = np.arange(nt * ny * nx, dtype=float).reshape(nt, ny, nx)

    return xr.Dataset({"field": (dims, data)}, coords=coords)


_OPEN_KWARGS = dict(
    x_dim="lon",
    y_dim="lat",
    time_dim="tim",
    lon_var="lon",
    lat_var="lat",
    time_var="Time",
)

_OPEN_KWARGS_Z = dict(
    **_OPEN_KWARGS,
    z_dim="lev",
    pres_var="lev",
)


def test_open_dataset_2d(tmp_path):
    """2-D lat/lon (no z) dataset."""
    path = tmp_path / "test_2d.nc"
    _make_ds().to_netcdf(path)

    ds = open_dataset(path, **_OPEN_KWARGS)

    assert set(ds.dims) == {"time", "z", "y", "x"}
    assert ds.sizes["z"] == 1
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "time" in ds.coords
    # longitude wrapping: all values in [-180, 180)
    assert float(ds["longitude"].max()) < 180.0
    assert float(ds["longitude"].min()) >= -180.0


def test_open_dataset_2d_cftime(tmp_path):
    """2-D lat/lon dataset with cftime time coord."""
    path = tmp_path / "test_2d_cftime.nc"
    raw = _make_ds()

    time_dim = _OPEN_KWARGS["time_dim"]
    time_var = _OPEN_KWARGS["time_var"]

    assert time_var not in raw.indexes

    # The arg is `dim` but it really wants the time coord (ideally dim coord)
    raw_cftime = (
        raw.rename_vars({time_var: time_dim})
        .convert_calendar("noleap", dim=time_dim, use_cftime=True)
        .rename_vars({time_dim: time_var})
    )
    assert time_var in raw_cftime.indexes
    assert not isinstance(raw_cftime.indexes[time_var], pd.DatetimeIndex)
    raw_cftime.to_netcdf(path)

    ds = open_dataset(path, **_OPEN_KWARGS)
    assert isinstance(ds.indexes["time"], pd.DatetimeIndex)


def test_open_dataset_3d(tmp_path):
    """3-D dataset (with z) has correct dims and shape."""
    nz = 5
    path = tmp_path / "test_3d.nc"
    _make_ds(nz=nz).to_netcdf(path)

    ds = open_dataset(path, **_OPEN_KWARGS_Z)

    assert set(ds.dims) == {"time", "z", "y", "x"}
    assert ds.sizes["z"] == nz
    assert "pres_pa_mid" in ds.coords


def test_open_dataset_surf_only_default_lev(tmp_path):
    """surf_only=True selects level 0 by default."""
    nz = 5
    path = tmp_path / "test_surf.nc"
    _make_ds(nz=nz).to_netcdf(path)

    ds_full = open_dataset(path, **_OPEN_KWARGS_Z)
    ds_surf = open_dataset(path, **_OPEN_KWARGS_Z, surf_only=True)

    assert ds_surf.sizes["z"] == 1
    assert set(ds_surf.dims) == set(ds_full.dims)
    np.testing.assert_array_equal(
        ds_surf["field"].isel(z=0).values,
        ds_full["field"].isel(z=0).values,
    )


def test_open_dataset_surf_only_explicit_lev(tmp_path):
    """surf_only=True with surf_lev=n selects the correct level."""
    nz = 5
    surf_lev = 3
    path = tmp_path / "test_surf_lev.nc"
    _make_ds(nz=nz).to_netcdf(path)

    ds_full = open_dataset(path, **_OPEN_KWARGS_Z)
    ds_surf = open_dataset(path, **_OPEN_KWARGS_Z, surf_only=True, surf_lev=surf_lev)

    assert ds_surf.sizes["z"] == 1
    np.testing.assert_array_equal(
        ds_surf["field"].isel(z=0).values,
        ds_full["field"].isel(z=surf_lev).values,
    )


def test_open_dataset_surf_only_false_unchanged(tmp_path):
    """surf_only=False (default) leaves z untouched."""
    nz = 5
    path = tmp_path / "test_no_surf.nc"
    _make_ds(nz=nz).to_netcdf(path)

    ds = open_dataset(path, **_OPEN_KWARGS_Z, surf_only=False)
    assert ds.sizes["z"] == nz


def test_open_mfdataset_basic(tmp_path):
    """open_mfdataset concatenates multiple files along time."""
    nz = 3
    raw = _make_ds(nz=nz)
    time_dim = _OPEN_KWARGS["time_dim"]
    paths = []
    for i in range(raw.sizes[time_dim]):
        p = tmp_path / f"slice_{i}.nc"
        raw.isel({time_dim: [i]}).to_netcdf(p)
        paths.append(p)

    ds = open_mfdataset(paths, **_OPEN_KWARGS_Z, combine="nested", concat_dim=time_dim)

    assert set(ds.dims) == {"time", "z", "y", "x"}
    assert ds.sizes["time"] == len(paths)
    assert ds.sizes["z"] == nz


def test_open_mfdataset_surf_only(tmp_path):
    """open_mfdataset with surf_only=True yields z size 1."""
    nz = 4
    raw = _make_ds(nz=nz)
    time_dim = _OPEN_KWARGS["time_dim"]
    paths = []
    for i in range(raw.sizes[time_dim]):
        p = tmp_path / f"slice_{i}.nc"
        raw.isel({time_dim: [i]}).to_netcdf(p)
        paths.append(p)

    ds = open_mfdataset(
        paths, **_OPEN_KWARGS_Z, combine="nested", concat_dim=time_dim, surf_only=True
    )

    assert ds.sizes["z"] == 1
    assert set(ds.dims) == {"time", "z", "y", "x"}
