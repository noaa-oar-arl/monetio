import numpy as np
import pytest
import xarray as xr

from monetio.readers.nesdis_viirs_jrr import VIIRSJRRReader, viirs_jrr_preprocess


@pytest.mark.parametrize("product", ["AOD", "ADP"])
def test_viirs_jrr_preprocess(product):
    # Create dummy dataset
    ds = xr.Dataset(
        {
            "Latitude": (("Rows", "Columns"), np.zeros((3, 4))),
            "Longitude": (("Rows", "Columns"), np.zeros((3, 4))),
        },
        attrs={"time_coverage_start": "2024-01-01T00:00:00Z"},
    )

    if product == "AOD":
        ds["AOD550"] = (("Rows", "Columns"), np.ones((3, 4)))
    elif product == "ADP":
        ds["Smoke"] = (("Rows", "Columns"), np.ones((3, 4)))

    ds_out = viirs_jrr_preprocess(ds, product=product)

    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert "time" in ds_out.coords
    assert ds_out.y.size == 3
    assert ds_out.x.size == 4

    if product == "AOD":
        assert "aod_550" in ds_out.data_vars
        assert ds_out["aod_550"].attrs["units"] == "1"
    elif product == "ADP":
        assert "Smoke" in ds_out.data_vars
        assert "long_name" in ds_out["Smoke"].attrs


def test_viirs_jrr_preprocess_qa():
    # Test quality masking
    ds = xr.Dataset(
        {
            "AOD550": (("Rows", "Columns"), [[1.0, 2.0], [3.0, 4.0]]),
            "AOD_Quality_Flag": (("Rows", "Columns"), [[0, 1], [2, 3]]),
            "Latitude": (("Rows", "Columns"), np.zeros((2, 2))),
            "Longitude": (("Rows", "Columns"), np.zeros((2, 2))),
        },
        attrs={"time_coverage_start": "2024-01-01T00:00:00Z"},
    )

    # Mask with threshold 2 (should keep 3.0 and 4.0)
    ds_out = viirs_jrr_preprocess(ds, product="AOD", qa_threshold=2)

    assert "aod_550" in ds_out.data_vars
    # Use .item() or flat access for 0D/1D checks to be safe, but here they are 2D
    assert np.isnan(ds_out.aod_550.values.flatten()[0])
    assert np.isnan(ds_out.aod_550.values.flatten()[1])
    assert ds_out.aod_550.values.flatten()[2] == 3.0
    assert ds_out.aod_550.values.flatten()[3] == 4.0
    # Quality flag itself should remain unmasked
    assert ds_out.AOD_Quality_Flag.values.flatten()[0] == 0


def test_viirs_jrr_eager_lazy_consistency():
    import dask.array as da

    # Create dummy dataset
    data = np.random.rand(10, 10)
    qa = np.random.randint(0, 4, (10, 10))

    ds_eager = xr.Dataset(
        {
            "AOD550": (("y", "x"), data),
            "AOD_Quality_Flag": (("y", "x"), qa),
            "Latitude": (("y", "x"), np.zeros((10, 10))),
            "Longitude": (("y", "x"), np.zeros((10, 10))),
        },
        attrs={"time_coverage_start": "2024-01-01T00:00:00Z"},
    )

    ds_lazy = ds_eager.chunk({"y": 5, "x": 5})

    # Run preprocess
    out_eager = viirs_jrr_preprocess(ds_eager, product="AOD", qa_threshold=2)
    out_lazy = viirs_jrr_preprocess(ds_lazy, product="AOD", qa_threshold=2)

    # Check consistency
    xr.testing.assert_allclose(out_eager, out_lazy.compute())
    assert isinstance(out_lazy.aod_550.data, da.Array)


def test_viirs_jrr_build_urls(monkeypatch):
    class MockFS:
        def glob(self, pattern):
            if "VIIRS-JRR-AOD" in pattern:
                return ["bucket/VIIRS-JRR-AOD/2024/01/01/file1.nc"]
            if "VIIRS-JRR-ADP" in pattern:
                return ["bucket/VIIRS-JRR-ADP/2024/01/01/file2.nc"]
            return []

    from monetio.readers.drivers import FileUtility

    monkeypatch.setattr(FileUtility, "get_fs", lambda path, **kwargs: MockFS())
    monkeypatch.setattr(FileUtility, "expand_paths", lambda path, **kwargs: MockFS().glob(path))

    reader = VIIRSJRRReader()
    urls_aod = reader.build_urls("2024-01-01", satellite="snpp", product="AOD")
    urls_adp = reader.build_urls("2024-01-01", satellite="snpp", product="ADP")

    assert len(urls_aod) == 1
    assert "file1.nc" in urls_aod[0]
    assert len(urls_adp) == 1
    assert "file2.nc" in urls_adp[0]


def test_viirs_jrr_stitching(monkeypatch):
    class MockDriver:
        def open(self, files, **kwargs):
            return xr.Dataset({"aod": (("time", "y", "x"), np.ones((len(files), 3, 4)))})

    reader = VIIRSJRRReader()
    monkeypatch.setattr(reader, "driver", MockDriver())

    ds = reader.open_dataset(files=["f1.nc", "f2.nc"], product="AOD")
    assert ds.aod.sizes["time"] == 2
