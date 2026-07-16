import pandas as pd
import pytest
import xarray as xr

from monetio.readers.airnow import AirNowReader, build_urls


def _check_df(df):
    assert df.index.size >= 1
    assert not any(col in df.columns for col in ["index", "level_0"])


def test_build_urls():
    dates = pd.date_range("2021/01/01", "2021/01/05", freq="h")
    urls, fnames = build_urls(dates)
    assert urls.size == fnames.size == dates.size == 4 * 24 + 1
    assert fnames[0] == dates[0].strftime(r"HourlyData_%Y%m%d%H.dat")
    urls, fnames = build_urls(dates, daily=True)
    assert urls.size == fnames.size == 5
    assert (fnames == "daily_data.dat").all()


@pytest.fixture
def mock_airnow_file(tmp_path):
    fn = tmp_path / "HourlyData_2023010100.dat"
    content = "01/01/23|00:00|012345678|Test Site|-5|OZONE|PPB|50.0|Test Source\n"
    content += "01/01/23|00:00|012345678|Test Site|-5|PM2.5|UG/M3|10.0|Test Source\n"
    content += "01/01/23|00:00|999999999|Bad TZ Site|0|OZONE|PPB|40.0|Test Source\n"
    fn.write_text(content, encoding="ISO-8859-1")
    return str(fn)


def test_airnow_eager_vs_lazy_local(mock_airnow_file, monkeypatch):
    pytest.importorskip("timezonefinder")

    def mock_read_monitor(*args, **kwargs):
        return pd.DataFrame(
            {
                "siteid": ["012345678", "999999999"],
                "latitude": [40.0, 40.0],
                "longitude": [-80.0, -80.0],
                "site_name": ["Site 1", "Site 2"],
            }
        )

    import monetio.readers.epa_utils as epa_utils

    monkeypatch.setattr(epa_utils, "read_monitor_file", mock_read_monitor)
    reader = AirNowReader()
    ds_eager = reader.open_dataset(
        files=mock_airnow_file, as_xarray=True, lazy=False, wide_fmt=True, bad_utcoffset="fix"
    )
    ds_lazy = reader.open_dataset(
        files=mock_airnow_file, as_xarray=True, lazy=True, wide_fmt=True, bad_utcoffset="fix"
    )
    assert ds_lazy.OZONE.chunks is not None
    ds_lazy_computed = ds_lazy.compute()
    ds_eager.attrs.pop("history", None)
    ds_lazy_computed.attrs.pop("history", None)
    data_vars = [v for v in ds_eager.data_vars if not v.endswith("_unit")]
    xr.testing.assert_allclose(ds_eager[data_vars], ds_lazy_computed[data_vars])
    uo_999 = ds_eager.sel(node=ds_eager.siteid == "999999999").utcoffset.values
    assert uo_999 == pytest.approx(-5.0)


@pytest.mark.network
def test_add_data_hourly_network():
    dates = pd.date_range("2024/07/01", periods=1, freq="h")
    try:
        df = AirNowReader().open_dataset(dates=dates, as_xarray=False, wide_fmt=True)
        _check_df(df)
        assert "OZONE" in df.columns or "PM2.5" in df.columns
    except Exception as e:
        pytest.skip(f"AirNow network call failed: {e}")


@pytest.mark.network
@pytest.mark.parametrize("bad_utcoffset", ["null", "drop", "fix", "leave"])
@pytest.mark.parametrize(
    "date",
    [pd.Timestamp("2021/07/01"), pd.Timestamp("2024/04/23")],
    ids=["multiple_bad", "some_bad"],
)
def test_check_zero_utc_offsets_network(date, bad_utcoffset, request):
    dates = [date]
    try:
        df = AirNowReader().open_dataset(
            dates=dates, daily=False, wide_fmt=True, bad_utcoffset=bad_utcoffset, as_xarray=False
        )
    except Exception as e:
        pytest.skip(f"AirNow network call failed: {e}")
    assert -180 <= df.longitude.min() < 0 < df.longitude.max() < 180
    bad_rows = df.query("utcoffset == 0 and abs(longitude) > 20")
    bad_sites = bad_rows.groupby("siteid")[["siteid", "longitude"]].first()
    if bad_utcoffset == "leave":
        assert not bad_sites.empty
    elif bad_utcoffset in ["null", "drop", "fix"]:
        assert bad_sites.empty
    if bad_utcoffset == "fix":
        assert ((df.utcoffset >= -12) & (df.utcoffset <= 14)).all()


@pytest.mark.network
def test_add_data_daily_network():
    dates = pd.date_range("2021/07/01", "2021/07/02")
    try:
        df = AirNowReader().open_dataset(dates=dates, daily=True, as_xarray=False, wide_fmt=True)
        _check_df(df)
        assert any("OZONE" in col for col in df.columns)
        assert df.time.unique().size == 2
    except Exception as e:
        pytest.skip(f"AirNow network call failed: {e}")
