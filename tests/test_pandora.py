from pathlib import Path

import dask.array as da
import pytest
import xarray as xr

from monetio.readers.pandora import PandoraReader

DATA = Path(__file__).parent / "data"
TEST_FP_PANDORA_NO2 = str(DATA / "pandora-uvvis-no2-boulder-20231206.h5")


def test_pandora_reader_eager():
    if not Path(TEST_FP_PANDORA_NO2).exists():
        pytest.skip("Test file not found")

    reader = PandoraReader()
    ds = reader.open_dataset(files=TEST_FP_PANDORA_NO2)

    assert isinstance(ds, xr.Dataset)
    assert "time" in ds.dims
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "nitrogen_dioxide" in ds.data_vars
    assert ds.nitrogen_dioxide.attrs.get("units") is not None

    # Check harmonization
    assert "no2_column_absorption_solar" not in ds.data_vars
    assert "nitrogen_dioxide" in ds.data_vars


def test_pandora_reader_lazy():
    if not Path(TEST_FP_PANDORA_NO2).exists():
        pytest.skip("Test file not found")

    reader = PandoraReader()
    # PandoraReader (via GEOMSReader) currently opens files in a loop but uses dask.array.from_array
    # for HDF5 variables if opened via GEOMSReader.
    ds = reader.open_dataset(files=TEST_FP_PANDORA_NO2)

    # In GEOMSReader, HDF5 variables are wrapped in dask arrays.
    assert isinstance(ds.nitrogen_dioxide.data, da.Array)


def test_pandora_build_urls_mock(monkeypatch):
    reader = PandoraReader()

    class MockFs:
        def ls(self, url):
            return [
                "groundbased_uvvis.doas.directsun.no2_noaa.esrl057_rd.rnvs3.1.8_boulder.co_20230101t000000z_20230101t235959z_001.h5",
                "groundbased_uvvis.doas.directsun.o3_noaa.esrl057_rd.rout2.1.8_boulder.co_20230101t000000z_20230101t235959z_001.h5",
            ]

    import fsspec

    monkeypatch.setattr(fsspec, "filesystem", lambda x: MockFs())

    urls = reader.build_urls(
        dates="2023-01-01", siteid="BoulderCO", instrument="Pandora57s1", product="no2"
    )
    assert len(urls) == 1
    assert "no2" in urls[0]

    urls_o3 = reader.build_urls(
        dates="2023-01-01", siteid="BoulderCO", instrument="Pandora57s1", product="o3"
    )
    assert len(urls_o3) == 1
    assert "o3" in urls_o3[0]


def test_pandora_add_data_redirection():
    from monetio.obs import pandora

    # Just check if the function exists and is callable
    assert hasattr(pandora, "add_data")
    assert hasattr(pandora, "add_local")
