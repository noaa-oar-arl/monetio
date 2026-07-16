import numpy as np
import pytest
import xarray as xr

from monetio.readers.umbc_aerosol import UMBCAerosolReader


@pytest.fixture
def mock_umbc_hdf5(tmp_path):
    """Create a mock UMBC Aerosol HDF5 structure."""
    import h5py

    fn = tmp_path / "UMBC_CL51_TEST.h5"
    with h5py.File(fn, "w") as f:
        # DATA group
        data = f.create_group("DATA")
        ntime = 10
        nz = 50
        data.create_dataset("Altitude_m", data=np.linspace(0, 5000, nz))
        data.create_dataset("UnixTime_UTC", data=np.linspace(1600000000, 1600003600, ntime))
        data.create_dataset("Profile_bsc", data=np.random.rand(ntime, nz))

        # Instrument_Attributes group
        atts = f.create_group("Instrument_Attributes")
        atts.attrs["Location_lat"] = 39.25
        atts.attrs["Location_lon"] = -76.71

    return str(fn)


def test_umbc_aerosol_eager_lazy(mock_umbc_hdf5):
    """Verify UMBCAerosolReader works with both Eager and Lazy backends."""
    reader = UMBCAerosolReader()

    # 1. Eager (NumPy)
    ds_eager = reader.open_dataset(mock_umbc_hdf5, lazy=False)
    assert isinstance(ds_eager.bsc.data, np.ndarray)
    assert "time" in ds_eager.coords
    assert "altitude" in ds_eager.coords
    assert ds_eager.latitude.values.item() == 39.25
    assert ds_eager.longitude.values.item() == -76.71
    assert ds_eager.time.dtype == "datetime64[ns]"

    # 2. Lazy (Dask)
    ds_lazy = reader.open_dataset(mock_umbc_hdf5, lazy=True)
    try:
        import dask.array as da

        assert isinstance(ds_lazy.bsc.data, da.Array)
    except ImportError:
        pytest.skip("Dask not installed")

    # 3. Equality Check
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())

    # 4. Provenance Check
    assert "history" in ds_eager.attrs
    assert "Preprocessed UMBC Aerosol data" in ds_eager.attrs["history"]
    assert "Read UMBC Aerosol data" in ds_eager.attrs["history"]


def test_umbc_aerosol_multi_file(tmp_path):
    """Verify UMBCAerosolReader handles multiple files via XarrayDriver."""
    import h5py

    files = []
    for i in range(2):
        fn = tmp_path / f"UMBC_CL51_{i}.h5"
        with h5py.File(fn, "w") as f:
            data = f.create_group("DATA")
            data.create_dataset("Altitude_m", data=np.linspace(0, 100, 5))
            data.create_dataset("UnixTime_UTC", data=[1600000000 + i * 3600])
            data.create_dataset("Profile_bsc", data=np.random.rand(1, 5))
            f.create_group("Instrument_Attributes").attrs["Location_lat"] = 40.0
        files.append(str(fn))

    reader = UMBCAerosolReader()
    ds = reader.open_dataset(files, concat_dim="time")
    assert ds.sizes["time"] == 2
    assert "latitude" in ds.coords
