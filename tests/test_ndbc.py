import pandas as pd
import pytest
import xarray as xr

from monetio.readers.ndbc import NDBCReader, build_urls


def test_build_urls():
    # Real-time
    urls = build_urls("41001", realtime=True)
    assert urls == ["https://www.ndbc.noaa.gov/data/realtime2/41001.txt"]

    # Historical
    urls = build_urls("41001", years=2020, realtime=False)
    assert urls == ["https://www.ndbc.noaa.gov/data/historical/stdmet/41001h2020.txt.gz"]

    urls = build_urls(["41001", "41002"], years=[2020, 2021], realtime=False)
    assert len(urls) == 4
    assert "41001h2020" in urls[0]


@pytest.fixture
def mock_ndbc_file(tmp_path):
    fn = tmp_path / "41002.txt"
    # Real-time format with 2 header rows
    content = "#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE\n"
    content += "#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft\n"
    content += "2026 03 24 10 50  40 13.0 17.0   2.9     7   5.8   0 1018.2  18.9  23.4  16.7   MM   MM    MM\n"
    content += "2026 03 24 10 40  40 13.0 16.0    MM    MM    MM  MM 1018.1  18.9  23.4  16.7   MM   MM    MM\n"
    fn.write_text(content)
    return str(fn)


@pytest.fixture
def mock_ndbc_hist_file(tmp_path):
    fn = tmp_path / "41002h2020.txt"
    # Historical format often has YYYY and no mm
    content = (
        "#YYYY MM DD hh WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS TIDE\n"
    )
    content += (
        "#yr  mo dy hr degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  ft\n"
    )
    content += (
        "2020 01 01 00  40 10.0 12.0   2.0     6   5.0  10 1015.0  15.0  20.0  12.0   MM   MM\n"
    )
    fn.write_text(content)
    return str(fn)


def test_ndbc_eager_vs_lazy(mock_ndbc_file, monkeypatch):
    def mock_get_station_table():
        return pd.DataFrame(
            {
                "siteid": ["41002"],
                "name": ["South Hatteras"],
                "latitude": [31.73],
                "longitude": [-74.95],
            }
        )

    import monetio.readers.ndbc as ndbc

    monkeypatch.setattr(ndbc, "get_station_table", mock_get_station_table)

    reader = NDBCReader()

    # Eager
    ds_eager = reader.open_dataset(files=mock_ndbc_file, lazy=False, as_xarray=True)
    assert isinstance(ds_eager, xr.Dataset)
    assert "wind_speed" in ds_eager.data_vars
    assert ds_eager.siteid.values[0] == "41002"
    assert ds_eager.latitude.values[0] == 31.73

    # Lazy
    ds_lazy = reader.open_dataset(files=mock_ndbc_file, lazy=True, as_xarray=True)
    assert ds_lazy.wind_speed.chunks is not None

    ds_lazy_comp = ds_lazy.compute()

    # Compare (drop history)
    ds_eager.attrs.pop("history", None)
    ds_lazy_comp.attrs.pop("history", None)

    xr.testing.assert_allclose(ds_eager, ds_lazy_comp)


def test_ndbc_historical(mock_ndbc_hist_file, monkeypatch):
    def mock_get_station_table():
        return pd.DataFrame(
            {
                "siteid": ["41002"],
                "name": ["South Hatteras"],
                "latitude": [31.73],
                "longitude": [-74.95],
            }
        )

    import monetio.readers.ndbc as ndbc

    monkeypatch.setattr(ndbc, "get_station_table", mock_get_station_table)

    reader = NDBCReader()
    ds = reader.open_dataset(files=mock_ndbc_hist_file, as_xarray=True)
    assert ds.time.dt.year[0] == 2020
    assert "wind_speed" in ds.data_vars


@pytest.mark.network
def test_ndbc_network():
    # Test a real file from NDBC
    reader = NDBCReader()
    try:
        # 41002 is usually active
        ds = reader.open_dataset(stations="41002", realtime=True)
        assert "wind_speed" in ds.data_vars
        assert ds.sizes["node"] > 0
    except Exception as e:
        pytest.skip(f"NDBC network call failed: {e}")
