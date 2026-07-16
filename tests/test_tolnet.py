import h5py
import numpy as np
import pytest
import xarray as xr

from monetio.readers.tolnet import TOLNetReader


@pytest.fixture
def mock_tolnet_file(tmp_path):
    fname = tmp_path / "TOLNet_test.hdf5"
    with h5py.File(fname, "w") as f:
        g_atts = f.create_group("INSTRUMENT_ATTRIBUTES")
        g_atts.attrs["Location_Latitude"] = b"39.0 N"
        g_atts.attrs["Location_Longitude"] = b"76.5 W"

        g_data = f.create_group("DATA")
        # ALT: (10,)
        g_data.create_dataset("ALT", data=np.linspace(0, 10000, 10))
        # TIME_MID_UT_UNIX: (2,)
        g_data.create_dataset("TIME_MID_UT_UNIX", data=np.array([1600000000000, 1600003600000]))
        # O3MR: (10, 2)
        g_data.create_dataset("O3MR", data=np.random.rand(10, 2))
        # O3ND: (10, 2)
        g_data.create_dataset("O3ND", data=np.random.rand(10, 2))
        # Add some missing values
        g_data["O3MR"][0, 0] = -999.0

    return str(fname)


def test_tolnet_reader_eager(mock_tolnet_file):
    reader = TOLNetReader()
    ds = reader.open_dataset(files=mock_tolnet_file, lazy=False)

    assert isinstance(ds, xr.Dataset)
    assert "ozone_mixing_ratio" in ds.data_vars
    assert "ozone_number_density" in ds.data_vars
    assert "altitude" in ds.coords
    assert "time" in ds.coords
    assert ds.sizes["z"] == 10
    assert ds.sizes["time"] == 2

    # Check spatial coords
    assert float(ds.latitude.values.flatten()[0]) == 39.0
    assert float(ds.longitude.values.flatten()[0]) == -76.5

    # Check masking
    assert np.isnan(ds.ozone_mixing_ratio.isel(z=0, time=0).values)

    # Check history
    assert "Read TOLNet data using standardized preprocessing." in ds.attrs["history"]


def test_tolnet_reader_lazy(mock_tolnet_file):
    pytest.importorskip("dask")
    reader = TOLNetReader()
    ds = reader.open_dataset(files=mock_tolnet_file, lazy=True, chunks={"z": 5})

    assert ds.ozone_mixing_ratio.chunks is not None

    ds_eager = reader.open_dataset(files=mock_tolnet_file, lazy=False)

    xr.testing.assert_allclose(ds.compute(), ds_eager)


def test_tolnet_multi_file(tmp_path):
    # Create two files
    f1 = tmp_path / "TOLNet_1.hdf5"
    f2 = tmp_path / "TOLNet_2.hdf5"

    for fpath, tstart in [(f1, 1600000000000), (f2, 1600007200000)]:
        with h5py.File(fpath, "w") as f:
            g_atts = f.create_group("INSTRUMENT_ATTRIBUTES")
            g_atts.attrs["Location_Latitude"] = b"39.0 N"
            g_atts.attrs["Location_Longitude"] = b"76.5 W"
            g_data = f.create_group("DATA")
            g_data.create_dataset("ALT", data=np.linspace(0, 10000, 10))
            g_data.create_dataset("TIME_MID_UT_UNIX", data=np.array([tstart, tstart + 3600000]))
            g_data.create_dataset("O3MR", data=np.random.rand(10, 2))

    reader = TOLNetReader()
    ds = reader.open_dataset(files=str(tmp_path / "TOLNet_*.hdf5"), lazy=True)

    assert ds.sizes["time"] == 4
    assert ds.ozone_mixing_ratio.chunks is not None
