import numpy as np
import pytest
import xarray as xr

from monetio.readers.actris import ACTRISReader


@pytest.fixture
def mock_actris_file(tmp_path):
    """Create a mock EBAS NASA-Ames 1001 file."""
    file_path = tmp_path / "test_actris.nas"
    content = """68 1001
Originator, Name
Organization
Submitter, Name
Project
1 1
2023 01 01 2023 01 01
0.041667
start_time
2
1.0 1.0
9999.99 9999.99
Ozone
Carbon_Monoxide
Station name: Test Station
Station code: TS0001
Station latitude: 45.0
Station longitude: 10.0
Station altitude: 100.0
Station land use: Grassland
Station setting: Rural
... more metadata lines until 68 ...
"""
    # Add dummy metadata lines to reach 68
    header_lines = content.count("\n")
    for i in range(68 - header_lines):
        content += f"Metadata line {i}\n"

    # Add data lines
    # time, ozone, co
    content += "0.0 10.0 100.0\n"
    content += "0.5 20.0 200.0\n"
    content += "1.0 9999.99 300.0\n"

    file_path.write_text(content)
    return str(file_path)


def test_actris_reader_eager(mock_actris_file):
    reader = ACTRISReader()
    ds = reader.open_dataset(mock_actris_file, as_xarray=True, lazy=False)

    assert isinstance(ds, xr.Dataset)
    assert "Ozone" in ds.data_vars
    assert "Carbon_Monoxide" in ds.data_vars
    assert ds.sizes["time"] == 3

    # Check coordinates
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "elevation" in ds.coords
    assert "time" in ds.coords

    # Check values (scaling and missing values)
    # 9999.99 should be NaN
    assert np.isnan(ds["Ozone"].values[2])
    assert ds["Ozone"].values[0] == 10.0
    assert ds["Carbon_Monoxide"].values[2] == 300.0

    # Check site metadata
    assert ds["latitude"].values[0] == 45.0
    assert ds["longitude"].values[0] == 10.0
    assert ds["elevation"].values[0] == 100.0
    assert ds["siteid"].values[0] == "Test Station"


def test_actris_reader_lazy(mock_actris_file):
    reader = ACTRISReader()
    ds = reader.open_dataset(mock_actris_file, as_xarray=True, lazy=True)

    assert isinstance(ds, xr.Dataset)
    # Check if it's dask-backed
    assert ds["Ozone"].chunks is not None

    # Trigger compute
    ds_computed = ds.compute()
    assert np.isnan(ds_computed["Ozone"].values[2])
    assert ds_computed["Ozone"].values[0] == 10.0


def test_actris_redirection(mock_actris_file):
    from monetio.obs.actris import add_data

    ds = add_data(mock_actris_file)
    assert isinstance(ds, xr.Dataset)
    assert "Ozone" in ds.data_vars


def test_actris_build_urls(monkeypatch):
    """Test URL construction from THREDDS catalog."""

    class MockResponse:
        def __init__(self):
            self.text = """
            <catalog>
              <dataset name="NO0042G.20230101000000.20241021102319.uv_abs.ozone.air.1y.1h.nc" ID="EBAS/NO0042G..." urlPath="ebas/NO0042G.20230101000000.nc" />
              <dataset name="DE0001R.20230601000000.20241021102319.uv_abs.ozone.air.1y.1h.nc" ID="EBAS/DE0001R..." urlPath="ebas/DE0001R.20230601000000.nc" />
              <dataset name="FR0001R.20230101000000.20241021102319.earlinet.ozone.air.1y.1h.nc" ID="EBAS/FR0001R..." urlPath="ebas/FR0001R.20230101000000.nc" />
            </catalog>
            """
            self.status_code = 200

        def raise_for_status(self):
            pass

    import requests

    # Clear cache if it exists to ensure monkeypatch works
    from monetio.readers.actris import get_ebas_catalog

    get_ebas_catalog.cache_clear()

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: MockResponse())

    reader = ACTRISReader()
    # Request range including both non-earlinet files
    urls = reader.build_urls(dates=["2023-01-01", "2023-06-01"])
    assert len(urls) == 2
    # Check that OPeNDAP base is used
    assert "thredds/dodsC/" in urls[0]
    assert "ebas/NO0042G.20230101000000.nc" in urls[0]

    # Filter by siteid
    urls = reader.build_urls(dates=["2023-01-01", "2023-06-01"], siteid="NO0042G")
    assert len(urls) == 1
    assert "NO0042G" in urls[0]

    # Verify overlap logic (1y file starting before range)
    urls = reader.build_urls(dates=["2023-12-31"])
    assert len(urls) == 2  # Both 2023-01 and 2023-06 files overlap with 2023-12-31

    # Verify EARLINET exclusion
    urls = reader.build_urls(dates=["2023-01-01"], siteid="FR0001R")
    assert len(urls) == 0
