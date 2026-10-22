"""
Test CALIPSO Reader
"""

import numpy as np
import pytest
import xarray as xr

from monetio.readers.calipso import CALIPSOReader


def create_mock_calipso_dataset(is_dask=False):
    """Create a mock CALIOP L2 aerosol profile dataset."""
    nray = 10
    nbin = 30

    ds = xr.Dataset(
        data_vars={
            "Extinction_Coefficient_532": (
                ("nray", "nbin"),
                np.random.default_rng(42).random((nray, nbin)),
            ),
            "Latitude": (("nray",), np.linspace(-90, 90, nray)),
            "Longitude": (("nray",), np.linspace(-180, 180, nray)),
            "Profile_Time": (("nray",), np.linspace(0, 6000, nray)),
        }
    )

    if is_dask:
        ds = ds.chunk({"nray": 5, "nbin": 15})

    return ds


@pytest.mark.parametrize("lazy", [False, True])
def test_calipso_reader_logic(lazy, monkeypatch):
    """Test CALIPSO reader preprocessing logic."""
    mock_ds = create_mock_calipso_dataset(is_dask=lazy)

    def mock_open(*args, **kwargs):
        return mock_ds

    monkeypatch.setattr("monetio.readers.drivers.XarrayDriver.open", mock_open)

    reader = CALIPSOReader()
    ds = reader.open_dataset(files="dummy.hdf")

    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "time" in ds.coords
    assert ds.dims == {"y": 10, "z": 30}

    if lazy:
        assert ds.Extinction_Coefficient_532.chunks is not None


def test_calipso_reader_use_dask_local_netcdf_parity(tmp_path):
    source = create_mock_calipso_dataset()
    source["unrelated"] = (("nray", "nbin"), np.full((10, 30), -1.0))
    path = tmp_path / "calipso-synthetic.nc"
    source.to_netcdf(path, engine="h5netcdf")
    reader = CALIPSOReader()
    eager = reader.open_dataset(files=str(path), engine="h5netcdf", drop_variables=["unrelated"])
    lazy = reader.open_dataset(
        files=str(path),
        use_dask=True,
        engine="h5netcdf",
        chunks={"nray": 5, "nbin": 15},
        drop_variables=["unrelated"],
    )
    try:
        assert "unrelated" not in lazy
        assert hasattr(lazy.Extinction_Coefficient_532.data, "dask")
        assert lazy.Extinction_Coefficient_532.chunks == ((5, 5), (15, 15))
        xr.testing.assert_allclose(eager, lazy.compute())
    finally:
        lazy.close()
