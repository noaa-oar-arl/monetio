import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.omps_nadir import OMPSNadirReader, omps_nadir_preprocess


@pytest.mark.parametrize("lazy", [True, False])
def test_omps_v8toz_preprocess(lazy):
    # Create dummy V8TOZ dataset
    # nTimes, nIFOV
    n_times = 5
    n_ifov = 10

    # Microseconds since 1958-01-01
    origin = pd.Timestamp("1958-01-01")
    target_time = pd.Timestamp("2024-01-01 18:45:10")
    time_val = (target_time - origin).total_seconds() * 1e6

    scan_times = np.full(n_times, time_val) + np.arange(n_times) * 1e6  # add 1s each

    ds = xr.Dataset(
        {
            "ColumnAmountO3": (
                ("nTimes", "nIFOV"),
                np.random.rand(n_times, n_ifov).astype(np.float32),
            ),
            "ScanTime": (("nTimes",), scan_times),
            "Latitude": (("nTimes", "nIFOV"), np.random.rand(n_times, n_ifov).astype(np.float32)),
            "Longitude": (("nTimes", "nIFOV"), np.random.rand(n_times, n_ifov).astype(np.float32)),
        }
    )

    if lazy:
        ds = ds.chunk({"nTimes": 2})

    ds_out = omps_nadir_preprocess(ds, product="v8toz")

    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert "time" in ds_out.coords
    assert ds_out.y.size == n_times
    assert ds_out.x.size == n_ifov

    # Check time
    assert ds_out.time.values[0] == np.datetime64(target_time)

    if lazy:
        assert ds_out.ozone_column.chunks is not None


def test_omps_v8toz_masking():
    # Create dummy V8TOZ dataset with quality flags
    ds = xr.Dataset(
        {
            "ColumnAmountO3": (("nTimes", "nIFOV"), np.ones((2, 2), dtype=np.float32)),
            "QualityFlag": (("nTimes", "nIFOV"), [[0, 1], [2, 0]]),
            "ScanTime": (("nTimes",), [0.0, 1.0]),
            "Latitude": (("nTimes", "nIFOV"), np.zeros((2, 2))),
            "Longitude": (("nTimes", "nIFOV"), np.zeros((2, 2))),
        }
    )

    ds_out = omps_nadir_preprocess(ds, product="v8toz")

    # Values where QualityFlag != 0 should be masked (NaN)
    ozone = ds_out.ozone_column.values
    assert ozone[0, 0] == 1.0
    assert np.isnan(ozone[0, 1])
    assert np.isnan(ozone[1, 0])
    assert ozone[1, 1] == 1.0


def test_omps_sdr_multi_group(monkeypatch):
    # Mock XarrayDriver.open to return different groups
    class MockDriver:
        def open(self, files, **kwargs):
            group = kwargs.get("group")
            if group == "All_Data/OMPS-TC-SDR_All":
                return xr.Dataset({"Radiance": (("y", "x"), np.ones((5, 10)))})
            elif group == "All_Data/OMPS-TC-GEO_All":
                return xr.Dataset(
                    {
                        "Latitude": (("y", "x"), np.zeros((5, 10))),
                        "Longitude": (("y", "x"), np.zeros((5, 10))),
                    }
                )
            return xr.Dataset()

    reader = OMPSNadirReader()
    monkeypatch.setattr(reader, "driver", MockDriver())

    # Mock standardize_satellite_coords to avoid needing real satellite data
    # (Actually it's better to let it run to verify it handles merged names)

    ds = reader.open_dataset(files=["test.nc"], product="tc_sdr")

    assert "radiance" in ds.data_vars
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    # If not expanding dims, it will be (y, x).
    # But OMPSNadirReader.open_dataset sets concat_dim="time" and combine="nested"
    # Wait, my mock driver returned (y, x). xr.merge doesn't add a time dim.
    # The reader's open_dataset loop uses super().open_dataset(files, **g_kwargs)
    # If files is a list of 1, XarrayDriver returns (y, x) if not preprocessed.
    # My mock driver returned (y, x).
    assert "y" in ds.radiance.dims
    assert "x" in ds.radiance.dims


def test_omps_nadir_reader_inference():
    reader = OMPSNadirReader()
    # Basic check that reader can be instantiated
    assert isinstance(reader, OMPSNadirReader)


@pytest.mark.parametrize("product", ["nmto3_l2", "nmto3_l3"])
def test_omps_nadir_nasa_fallback(product):
    # Create dummy NASA style dataset
    ds = xr.Dataset(
        {
            "ColumnAmountO3": (("scanline", "ground_pixel"), np.ones((3, 4), dtype=np.float32)),
            "Time": (("scanline",), np.zeros(3)),
        },
        coords={
            "Latitude": (("scanline", "ground_pixel"), np.zeros((3, 4))),
            "Longitude": (("scanline", "ground_pixel"), np.zeros((3, 4))),
        },
    )

    # For NASA products, it should still work
    ds_out = omps_nadir_preprocess(ds, product=product)

    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert "ozone_column" in ds_out.data_vars
    assert "time" in ds_out.coords or "time" in ds_out.dims


def test_omps_nadir_build_urls(monkeypatch):
    class MockFS:
        def glob(self, pattern):
            if "OMPS_V8TOZ/2024/01/01/" in pattern:
                return [
                    "s3://bucket/OMPS_V8TOZ/2024/01/01/file1.nc",
                    "s3://bucket/OMPS_V8TOZ/2024/01/01/file2.nc",
                ]
            return []

    from monetio.readers.drivers import FileUtility

    monkeypatch.setattr(FileUtility, "get_fs", lambda path, **kwargs: MockFS())
    monkeypatch.setattr(FileUtility, "expand_paths", lambda path, **kwargs: MockFS().glob(path))

    reader = OMPSNadirReader()
    urls = reader.build_urls("2024-01-01", satellite="snpp", product="v8toz")

    assert len(urls) == 2
    assert "s3://bucket/OMPS_V8TOZ/2024/01/01/file1.nc" in urls


def test_omps_nadir_stitching(monkeypatch):
    # Mock XarrayDriver to return a combined dataset
    class MockDriver:
        def open(self, files, **kwargs):
            # Verify concat_dim and combine are passed
            assert kwargs.get("concat_dim") == "time"
            assert kwargs.get("combine") == "nested"
            return xr.Dataset({"ozone": (("time", "y", "x"), np.ones((len(files), 5, 10)))})

    reader = OMPSNadirReader()
    monkeypatch.setattr(reader, "driver", MockDriver())

    ds = reader.open_dataset(files=["file1.nc", "file2.nc"], product="v8toz")
    assert ds.ozone.sizes["time"] == 2
