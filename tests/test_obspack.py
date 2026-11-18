import numpy as np
import pandas as pd
import xarray as xr

from monetio import obspack


def _write_obspack_file(path, times, values, obspack_ids):
    ds = xr.Dataset(
        {
            "value": ("obs", values, {"units": "ppm"}),
            "latitude": ("obs", np.full(len(times), 19.5), {"units": "degrees_north"}),
            "longitude": ("obs", np.full(len(times), -155.6), {"units": "degrees_east"}),
            "obspack_id": ("obs", obspack_ids),
        },
        coords={"time": ("obs", times)},
    )
    ds.to_netcdf(path)


def test_open_dataset(tmp_path):
    path = tmp_path / "co2_mlo_surface-flask_426_representative.nc"
    times = pd.date_range("2020-01-01", periods=2, freq="D")
    ids = ["obspack-a", "obspack-b"]
    _write_obspack_file(path, times, [410.2, 410.5], ids)

    ds = obspack.open_dataset(path)

    assert ds.value.values.tolist() == [410.2, 410.5]
    assert ds.time.dims == ("obs",)
    assert ds.latitude.dims == ("obs",)
    assert ds.longitude.dims == ("obs",)
    assert ds.obspack_id.dims == ("obs",)
    assert ds.obspack_id.values.tolist() == ids
    assert "obspack_id" in ds.coords
    assert np.issubdtype(ds.time.dtype, np.datetime64)


def test_open_mfdataset(tmp_path):
    first = tmp_path / "first.nc"
    second = tmp_path / "second.nc"
    _write_obspack_file(
        first,
        pd.date_range("2020-01-01", periods=1),
        [410.2],
        ["obspack-a"],
    )
    _write_obspack_file(
        second,
        pd.date_range("2020-01-02", periods=1),
        [410.5],
        ["obspack-b"],
    )

    ds = obspack.open_mfdataset([first, second])

    assert ds.sizes["obs"] == 2
    assert ds.value.values.tolist() == [410.2, 410.5]
    assert np.array_equal(ds.time.values, pd.date_range("2020-01-01", periods=2).values)
    assert ds.obspack_id.values.tolist() == ["obspack-a", "obspack-b"]
