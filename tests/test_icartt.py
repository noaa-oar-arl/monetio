import numpy as np
import xarray as xr

from monetio.readers.icartt import ICARTTReader


def create_mock_icartt(filename, multiple_vars=False):
    """Create a mock ICARTT file."""
    if multiple_vars:
        header = [
            "17, 1001",
            "Aero PI",
            "Aero Org",
            "Aero Source",
            "Aero Mission",
            "1",
            "2023, 05, 15, 2023, 05, 15",
            "1",
            "Time_Start, seconds",
            "4",
            "1.0, 1.0, 10.0, 1.0",
            "-999, -999, -99, N/A",
            "Latitude, deg",
            "Longitude, deg",
            "Ozone, ppb",
            "CO, ppm",
            "Time_Start, Latitude, Longitude, Ozone, CO",
        ]
        data = [
            "0, 40.0, -100.0, 50.0, 0.1",
            "1, 41.0, -101.0, -99, 0.2",
            "2, -999, -102.0, 60.0, N/A",
        ]
    else:
        header = [
            "15, 1001",
            "Aero PI",
            "Aero Org",
            "Aero Source",
            "Aero Mission",
            "1",
            "2023, 05, 15, 2023, 05, 15",
            "1",
            "Time_Start, seconds",
            "2",
            "1.0, 1.0",
            "-999, -999",
            "Latitude, deg",
            "Longitude, deg",
            "Time_Start, Latitude, Longitude",
        ]
        data = ["0, 40.0, -100.0", "1, 41.0, -101.0", "2, -999, -102.0"]

    with open(filename, "w") as f:
        f.write("\n".join(header + data))


def test_icartt_reader_lazy(tmp_path):
    fn = str(tmp_path / "test.ict")
    create_mock_icartt(fn)

    reader = ICARTTReader()

    # 1. Eager
    ds_eager = reader.open_dataset(fn, lazy=False)
    # If expanded 2D, it should have time and node dims.
    # For a single file with one platform, node=1.
    if "time" in ds_eager.dims:
        lat0 = ds_eager.latitude.isel(time=0).values
        lon0 = ds_eager.longitude.isel(time=0).values
        lat2 = ds_eager.latitude.isel(time=2).values
    else:
        lat0 = ds_eager.latitude.isel(node=0).values
        lon0 = ds_eager.longitude.isel(node=0).values
        lat2 = ds_eager.latitude.isel(node=2).values

    assert lat0 == 40.0
    assert lon0 == -100.0
    assert np.isnan(lat2)

    # 2. Lazy
    ds_lazy = reader.open_dataset(fn, lazy=True)
    # PointReader currently returns Dask-backed variables if lazy=True
    # But wait, PointReader.to_xarray uses to_dask_array(lengths=True)
    assert ds_lazy.latitude.chunks is not None

    # 3. Compare
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())


def test_icartt_complex_scaling(tmp_path):
    fn = str(tmp_path / "test_complex.ict")
    create_mock_icartt(fn, multiple_vars=True)

    reader = ICARTTReader()

    # 1. Eager
    ds_eager = reader.open_dataset(fn, lazy=False)

    # Check Scaling
    # Ozone scale is 10.0, value 50.0 -> 500.0
    # CO scale is 1.0, value 0.1 -> 0.1
    assert ds_eager.Ozone.isel(time=0) == 500.0
    assert np.isclose(ds_eager.CO.isel(time=0), 0.1)

    # Check Missing Values
    # Ozone missing is -99. Row 1: -99 -> NaN
    # CO missing is 'N/A'. Row 2: 'N/A' -> NaN
    assert np.isnan(ds_eager.Ozone.isel(time=1))
    assert np.isnan(ds_eager.CO.isel(time=2))

    # 2. Lazy
    ds_lazy = reader.open_dataset(fn, lazy=True)
    assert ds_lazy.Ozone.chunks is not None

    # 3. Compare
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())


def test_icartt_metadata(tmp_path):
    fn = str(tmp_path / "test.ict")
    create_mock_icartt(fn)

    reader = ICARTTReader()
    ds = reader.open_dataset(fn)
    assert ds.attrs["PI"] == "Aero PI"
    assert ds.attrs["mission"] == "Aero Mission"
