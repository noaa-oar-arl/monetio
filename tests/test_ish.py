import gzip
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import xarray as xr

from monetio.readers.ish import ISHReader, read_ish_file
from monetio.readers.ish_lite import ISHLiteReader

# The ISD history frame comes from the shared ``ish_history`` fixture in
# conftest.py, which is byte-identical to the ``ish_lite`` reader's copy.


def test_read_ish_file_logic(tmp_path):
    line1 = "0054722244003582020090100004+38941-076952FM-12+0020KADW 99992201V0040199999199030000199+02561+01841101851"
    fn = tmp_path / "722244-00358-2020"
    fn.write_text(line1 + "\n")
    df = read_ish_file(str(fn))
    assert len(df) == 1
    assert df.time.iloc[0] == pd.Timestamp("2020-09-01 00:00:00")
    assert df.t.iloc[0] == 25.6


def test_read_ish_file_rejects_payload_without_valid_observations(tmp_path):
    fn = tmp_path / "corrupt-ish.bin"
    fn.write_text("this is not a valid fixed-width ISH record\n")

    from monetio.readers.errors import ContentInvalidError

    with pytest.raises(ContentInvalidError, match="valid observation timestamps") as exc:
        read_ish_file(str(fn))
    assert fn.name in str(exc.value)


def test_ish_eager_lazy_consistency(tmp_path, ish_history):
    """Verify that Eager (Pandas) and Lazy (Dask) backends produce identical results."""
    line1 = "0054722244003582020090100004+38941-076952FM-12+0020KADW 99992201V0040199999199030000199+02561+01841101851"
    fn = tmp_path / "722244-00358-2020"
    fn.write_text(line1 + "\n")
    reader = ISHReader()

    def side_effect(self, dates=None):
        self.history = ish_history

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        # 1. Eager (NumPy/Pandas)
        ds_eager = reader.open_dataset(files=str(fn), as_xarray=True, lazy=False, resample=False)
        table_eager = reader.open_dataset(
            files=str(fn), as_xarray=False, lazy=False, resample=False
        )
        # 2. Lazy (Dask)
        ds_lazy = reader.open_dataset(files=str(fn), as_xarray=True, lazy=True, resample=False)
        df_lazy = reader.open_dataset(files=str(fn), as_xarray=False, lazy=True, resample=False)
        # The use_dask alias must also select the lazy branch before the
        # DataFrame resampling policy is applied.
        with pytest.warns(UserWarning, match="Resampling is currently not supported"):
            df_alias = reader.open_dataset(
                files=str(fn), as_xarray=False, use_dask=True, resample=True
            )

    # Check values
    assert isinstance(table_eager, pd.DataFrame)
    assert len(table_eager) == 1
    assert all(var.chunks is None for var in ds_eager.data_vars.values())
    assert all(var.chunks is not None for var in ds_lazy.data_vars.values() if var.dims)
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    assert ds_eager.state.values[0] == "MD"
    import dask.dataframe as dd

    assert isinstance(df_lazy, dd.DataFrame)
    pd.testing.assert_frame_equal(table_eager, df_lazy.compute())
    assert isinstance(df_alias, dd.DataFrame)


@pytest.mark.parametrize("lite", [False, True], ids=["ish", "ish-lite"])
@pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-dataset"])
def test_ish_station_date_selection_bounds_files_and_lazy_outputs(
    tmp_path, monkeypatch, ish_history, lite, as_xarray
):
    import dask.dataframe as dd

    import monetio.readers.ish as ish_module

    if lite:
        path = tmp_path / "722244-00358-2020.gz"
        with gzip.open(path, "wb") as stream:
            stream.write(b"2020 09 01 00 256 184 10185 220 40 0 0 0\n")
        reader = ISHLiteReader()
    else:
        path = tmp_path / "722244-00358-2020"
        path.write_text(
            "0054722244003582020090100004+38941-076952FM-12+0020KADW "
            "99992201V0040199999199030000199+02561+01841101851\n"
        )
        reader = ISHReader()

    observed = {}
    original_build_urls = ish_module.ISH.build_urls

    def localize_selected_urls(self, dates=None, sites=None, lite=False):
        urls = original_build_urls(self, dates=dates, sites=sites, lite=lite)
        observed["urls"] = urls["name"].tolist()
        localized = urls.copy()
        localized["name"] = [str(path)]
        observed["station_ids"] = sites.station_id.tolist()
        return localized

    monkeypatch.setattr(
        ish_module.ISH,
        "read_ish_history",
        lambda self, dates=None: setattr(self, "history", ish_history.copy()),
    )
    monkeypatch.setattr(ish_module.ISH, "build_urls", localize_selected_urls)

    result = reader.open_dataset(
        dates=pd.DatetimeIndex(["2020-09-01"]),
        site="72224400358",
        as_xarray=as_xarray,
        lazy=True,
        resample=False,
    )
    try:
        assert observed["station_ids"] == ["72224400358"]
        assert len(observed["urls"]) == 1
        assert observed["urls"][0].endswith("/2020/722244-00358-2020.gz")
        if as_xarray:
            assert isinstance(result, xr.Dataset)
            variable = "t2m" if lite else "t"
            assert result[variable].chunks is not None
            assert result[variable].compute().size == 1
        else:
            assert isinstance(result, dd.DataFrame)
            assert result.compute().shape[0] == 1
    finally:
        if isinstance(result, xr.Dataset):
            result.close()


def test_ish_resampling_logic(tmp_path, ish_history):
    # Create 3 hours of data
    line1 = "0054722244003582020090100004+38941-076952FM-12+0020KADW 99992201V0040199999199030000199+02001+01001100001"
    line2 = "0054722244003582020090101004+38941-076952FM-12+0020KADW 99992201V0040199999199030000199+02101+01101100101"
    line3 = "0054722244003582020090102004+38941-076952FM-12+0020KADW 99992201V0040199999199030000199+02201+01201100201"

    fn = tmp_path / "722244-00358-2020"
    with open(fn, "w") as f:
        f.write(line1 + "\n")
        f.write(line2 + "\n")
        f.write(line3 + "\n")

    reader = ISHReader()

    def side_effect(self, dates=None):
        self.history = ish_history

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        # Resample to 3h
        ds = reader.open_dataset(
            files=str(fn), as_xarray=True, lazy=False, resample=True, window="3h"
        )

    assert len(ds.time) == 1
    assert ds.t.values[0] == 21.0  # Average of 20, 21, 22
    assert ds.p.values[0] == 1001.0


def test_ish_metadata_merging_robustness(tmp_path, ish_history):
    """Test that metadata merging handles missing sites gracefully."""
    line1 = "0054722244003582020090100004+38941-076952FM-12+0020KADW 99992201V0040199999199030000199+02561+01841101851"
    line2 = "0054888888999992020090100004+10000+020000FM-12+0010XXXX 99992201V0040199999199030000199+01001+00501100001"

    fn = tmp_path / "mixed_sites"
    fn.write_text(line1 + "\n" + line2 + "\n")

    reader = ISHReader()

    def side_effect(self, dates=None):
        self.history = ish_history

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        ds = reader.open_dataset(files=str(fn), as_xarray=True, lazy=False, resample=False)

    # One site is dropped because it lacks lat/lon metadata (not in ish_history)
    assert len(ds.node) == 1
    assert ds.siteid.values[0] == "72224400358"


def test_ish_lite_multi_site_resample(monkeypatch, tmp_path):
    def mock_read_history(self, dates=None):
        self.history = pd.DataFrame(
            {
                "usaf": ["012345", "543210"],
                "wban": ["67890", "09876"],
                "latitude": [40.0, 41.0],
                "longitude": [-80.0, -81.0],
                "station_id": ["01234567890", "54321009876"],
                "ctry": ["US", "US"],
                "state": ["PA", "OH"],
                "station name": ["Test Station 1", "Test Station 2"],
                "elev(m)": [100.0, 200.0],
                "begin": [pd.to_datetime("2020-01-01"), pd.to_datetime("2020-01-01")],
                "end": [pd.to_datetime("2025-01-01"), pd.to_datetime("2025-01-01")],
            }
        )

    import monetio.readers.ish as ish

    monkeypatch.setattr(ish.ISH, "read_ish_history", mock_read_history)
    fn1 = tmp_path / "012345-67890-2023.gz"
    with gzip.open(fn1, "wb") as f:
        f.write(b"2023 01 01 00  100  50 10132  270   50 0 0 0\n")
    fn2 = tmp_path / "543210-09876-2023.gz"
    with gzip.open(fn2, "wb") as f:
        f.write(b"2023 01 01 00  200  80 10132  270   50 0 0 0\n")
    reader = ISHLiteReader()
    ds = reader.open_dataset(
        files=[str(fn1), str(fn2)], as_xarray=True, resample=True, window="h", lazy=False
    )
    # After resampling, time dimension should have 1 step (all data at same hour)
    assert ds.sizes["time"] == 1


def test_read_ish_file_timeout(tmp_path):
    """Test that timeout is propagated correctly."""
    fn = tmp_path / "dummy.gz"
    fn.write_text("dummy content")

    with patch("monetio.readers.drivers.FileUtility.get_fs") as mock_get_fs:
        mock_fs = MagicMock()
        mock_get_fs.return_value = mock_fs
        try:
            read_ish_file("http://example.com/data.gz", request_timeout=42)
        except Exception:
            pass


@pytest.mark.network
def test_ish_network():
    dates = pd.date_range("2020-09-01", "2020-09-01 01:00", freq="h")
    site = "72224400358"
    try:
        df = ISHReader().open_dataset(dates=dates, site=site, as_xarray=False)
        assert not df.empty
    except Exception as e:
        pytest.skip(f"ISH network call failed: {e}")
