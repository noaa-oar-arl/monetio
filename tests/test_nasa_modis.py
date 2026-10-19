import numpy as np
import pytest
import xarray as xr

from monetio.readers.nasa_modis import NASAMODISReader


def mock_nasa_modis_dataset():
    """Create a mock NASA MODIS dataset."""
    ny = 10
    nx = 10

    data = np.random.default_rng(42).random((ny, nx))
    ds = xr.Dataset(
        {
            "Data_Field": (("y", "x"), data),
        },
        attrs={
            "HORIZONTALTILENUMBER": 10,
            "VERTICALTILENUMBER": 5,
            "RANGEBEGINNINGDATE": "2023-01-01",
            "RANGEBEGINNINGTIME": "12:00:00",
        },
    )
    # The reader expects certain dimension names which it will standardize
    # or it expects them to be already standardized if passed to standardize_satellite_coords
    # In nasa_modis_preprocess:
    # ds = standardize_satellite_coords(ds, y_dim=["YDim:MOD_Grid_BRDF", "y"], x_dim=["XDim:MOD_Grid_BRDF", "x"])

    return ds


def test_nasa_modis_eager_lazy(tmp_path):
    ds_mock = mock_nasa_modis_dataset()
    fname = tmp_path / "test_modis.nc"
    # Use h5netcdf to simulate HDF-like structure if needed, or just netcdf4
    ds_mock.to_netcdf(fname, engine="h5netcdf")

    reader = NASAMODISReader()

    # Eager Mode
    ds_eager = reader.open_dataset(files=str(fname), lazy=False, engine="h5netcdf")
    assert isinstance(ds_eager, xr.Dataset)
    assert "time" in ds_eager.coords
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords
    assert "x" in ds_eager.dims
    assert "y" in ds_eager.dims
    assert not hasattr(ds_eager.Data_Field.data, "dask")

    # Lazy Mode
    ds_lazy = reader.open_dataset(
        files=str(fname), use_dask=True, chunks={"x": 5, "y": 5}, engine="h5netcdf"
    )
    assert isinstance(ds_lazy, xr.Dataset)
    assert hasattr(ds_lazy.Data_Field.data, "dask")

    # Verify values (ignoring attributes like history)
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())

    # Verify history tracking
    assert "Read NASA MODIS data." in ds_eager.attrs["history"]
    assert "Preprocessed NASA MODIS data." in ds_eager.attrs["history"]
    assert "Assigned coordinates for tile h10v5." in ds_eager.attrs["history"]


def test_nasa_modis_drop_variables_keeps_selected_dask_payload(tmp_path):
    source = mock_nasa_modis_dataset()
    source["unrelated"] = (("y", "x"), np.full((10, 10), -1.0))
    path = tmp_path / "nasa-modis-selective.nc"
    source.to_netcdf(path, engine="h5netcdf")
    reader = NASAMODISReader()
    eager = reader.open_dataset(str(path), engine="h5netcdf", drop_variables=["unrelated"])
    lazy = reader.open_dataset(
        str(path),
        engine="h5netcdf",
        use_dask=True,
        chunks={"y": 5, "x": 5},
        drop_variables=["unrelated"],
    )
    try:
        assert "unrelated" not in lazy
        assert hasattr(lazy.Data_Field.data, "dask")
        xr.testing.assert_allclose(eager, lazy.compute())
    finally:
        lazy.close()


if __name__ == "__main__":
    pytest.main([__file__])
