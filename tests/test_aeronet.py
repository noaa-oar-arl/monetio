from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from monetio import aeronet

DATA = Path(__file__).parent / "data"

try:
    import pytspack

    # Check if actually usable (fixes Windows CI issue where symbols are missing)
    # Some versions/platforms might have the module but not the shared library symbols
    try:
        pytspack.TsPack()
    except (RuntimeError, AttributeError):
        # Fallback check for older versions
        pytspack.tspsi([0.0, 1.0], [0.0, 1.0])
except (ImportError, RuntimeError, AttributeError, TypeError):
    has_pytspack = False
else:
    has_pytspack = True


def is_connection_error(e):
    """Check if an exception is a connection error."""
    import requests

    msg = str(e)
    return (
        isinstance(e, requests.exceptions.ConnectionError | requests.exceptions.Timeout)
        or "Connection refused" in msg
        or "Max retries exceeded" in msg
        or "PandasDriver failed to open files" in msg
        or "timed out" in msg.lower()
    )


@pytest.fixture
def mock_valid_sites():
    """Mock get_valid_sites to avoid network calls during tests."""
    with patch("monetio.readers.aeronet.get_valid_sites") as mock:
        mock.return_value = pd.DataFrame(
            {
                "siteid": ["Mauna_Loa", "SERC", "Cart_Site", "Chilbolton", "Banana_River"],
                "longitude": [-155.6, -76.5, -97.5, -1.4, -80.6],
                "latitude": [19.5, 38.9, 36.6, 51.1, 28.4],
                "elevation": [3397.0, 10.0, 315.0, 84.0, 2.0],
            }
        )
        yield mock


def test_build_url_required_param_checks(mock_valid_sites):
    # Default (nothing set; `dates`, `prod``, `daily` required)
    a = aeronet.AERONET()
    with pytest.raises(AssertionError):
        a.build_url()

    # Adding dates
    a.dates = pd.date_range("2021/08/01", "2021/08/03")
    with pytest.raises(AssertionError):
        a.build_url()

    # Adding prod
    a.prod = "AOD15"
    with pytest.raises(AssertionError):
        a.build_url()

    # Adding daily (now should work)
    a.daily = 20
    a.build_url()


def test_build_url_bad_prod(mock_valid_sites):
    dates = pd.date_range("2021/08/01", "2021/08/02")
    a = aeronet.AERONET()
    a.dates = dates
    a.daily = 10

    # Invalid non-inv product
    a.prod = "asdf"
    with pytest.raises(ValueError, match="invalid product"):
        a.build_url()

    # Good non-inv prod but inv_type set
    a.prod = "AOD15"
    a.inv_type = "ALM15"
    with pytest.raises(ValueError, match="invalid product"):
        a.build_url()

    # Bad inv_type
    a.inv_type = "asdf"
    with pytest.raises(ValueError, match="invalid inv type"):
        a.build_url()

    # Good inv type but prod isn't
    a.inv_type = "ALM15"
    with pytest.raises(ValueError, match="invalid product"):
        a.build_url()

    # Both good
    a.prod = "SIZ"
    a.build_url()


def test_valid_sites_col_rename():
    try:
        # Use low retries for test
        df = aeronet.get_valid_sites(retries=1)
        if df.empty:
            pytest.skip("AERONET locations file could not be fetched (empty)")
        assert (df.columns == ["siteid", "longitude", "latitude", "elevation"]).all()
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        raise


def test_add_data_bad_siteid(mock_valid_sites):
    with pytest.raises(ValueError, match="invalid site"):
        aeronet.add_data(siteid="Rivendell", retries=0)


def test_add_data_one_site():
    dates = pd.date_range("2021/08/01", "2021/08/03")
    try:
        df = aeronet.add_data(dates, siteid="SERC", as_xarray=False, retries=5)
        assert df.index.size > 0
        assert (df.siteid == "SERC").all()
        assert df.attrs["info"].startswith("AERONET Data Download")
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        if "valid query but no data found" in str(e):
            pytest.skip("No data found for the given query")
        raise


def test_aeronet_aero_protocol():
    import xarray as xr

    from monetio.readers.aeronet import AERONETReader

    # Use local file to avoid network issues
    fp = DATA / "aeronet-AOD15-example.txt"
    if not fp.exists():
        pytest.skip(f"Local data file not found at {fp}")

    reader = AERONETReader()

    # 1. Eager Load
    df_eager = reader.open_dataset(files=str(fp), as_xarray=False, lazy=False)
    assert isinstance(df_eager, pd.DataFrame)
    assert not df_eager.empty
    assert "time" in df_eager.columns
    assert "siteid" in df_eager.columns

    # 2. Lazy Load
    df_lazy = reader.open_dataset(files=str(fp), as_xarray=False, lazy=True)
    try:
        import dask.dataframe as dd

        assert isinstance(df_lazy, dd.DataFrame)
    except ImportError:
        pytest.skip("Dask not installed")

    # Check they match after compute
    df_eager = df_eager.reindex(sorted(df_eager.columns), axis=1)
    df_lazy_computed = df_lazy.compute().reindex(sorted(df_eager.columns), axis=1)
    df_eager["siteid"] = df_eager["siteid"].astype(object)
    pd.testing.assert_frame_equal(df_eager, df_lazy_computed, check_dtype=False)

    # 3. Xarray Eager
    ds_eager = reader.open_dataset(files=str(fp), as_xarray=True, lazy=False)
    assert isinstance(ds_eager, xr.Dataset)
    assert "time" in ds_eager.coords
    assert "node" in ds_eager.dims

    # 4. Xarray Lazy
    ds_lazy = reader.open_dataset(files=str(fp), as_xarray=True, lazy=True)
    assert isinstance(ds_lazy, xr.Dataset)
    assert any(ds_lazy[v].chunks is not None for v in ds_lazy.data_vars)

    # Match
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())


def test_aeronet_build_urls_n_chunks(mock_valid_sites):
    from monetio.readers.aeronet import build_urls

    dates = pd.date_range("2021-01-01", "2021-01-31", freq="D")
    urls = build_urls(dates, product="AOD15", siteid="SERC", n_chunks=3)
    assert len(urls) == 3
    assert "site=SERC" in urls[0]
    # Check that they cover the whole range roughly
    assert "day=01" in urls[0]
    assert "day2=31" in urls[-1]


def test_aeronet_chunking_logic():
    import pandas as pd

    from monetio.readers.aeronet import AERONETReader

    reader = AERONETReader()
    dates = pd.date_range("2021-01-01", "2021-01-02", freq="D")  # 2 days

    # All sites, short range -> should be 1 chunk
    with patch("monetio.readers.aeronet.build_urls") as mock_build:
        mock_build.return_value = ["http://test.com"]
        with patch("monetio.readers.base.PointReader.open_dataset") as mock_open:
            mock_open.return_value = pd.DataFrame()
            reader.open_dataset(dates=dates, siteid=None, lazy=True)
            mock_build.assert_called_once()
            assert mock_build.call_args[1]["n_chunks"] == 1

    # Single site, short range -> should be 2 chunks (min(n_days, 8))
    with patch("monetio.readers.aeronet.build_urls") as mock_build:
        mock_build.return_value = ["http://test.com"]
        # Mock site validation
        with patch("monetio.readers.aeronet.get_valid_sites") as mock_sites:
            mock_sites.return_value = pd.DataFrame({"siteid": ["Mauna_Loa"]})
            with patch("monetio.readers.base.PointReader.open_dataset") as mock_open:
                mock_open.return_value = pd.DataFrame()
                reader.open_dataset(dates=dates, siteid="Mauna_Loa", lazy=True)
                mock_build.assert_called_once()
                assert mock_build.call_args[1]["n_chunks"] == 2

    # All sites, long range -> should be 8 chunks
    dates_long = pd.date_range("2021-01-01", "2021-03-01", freq="D")  # ~60 days
    with patch("monetio.readers.aeronet.build_urls") as mock_build:
        mock_build.return_value = ["http://test.com"]
        with patch("monetio.readers.base.PointReader.open_dataset") as mock_open:
            mock_open.return_value = pd.DataFrame()
            reader.open_dataset(dates=dates_long, siteid=None, lazy=True)
            mock_build.assert_called_once()
            assert mock_build.call_args[1]["n_chunks"] == 8


def test_aeronet_build_urls_split_by_day(mock_valid_sites):
    from monetio.readers.aeronet import build_urls

    dates = pd.date_range("2021-08-01", "2021-08-02", freq="D")
    urls = build_urls(dates, product="AOD15", siteid="SERC", split_by_day=True)
    # 2021-08-01 to 2021-08-02 is one span (one daily averaging period or full day)
    # Wait, my logic for split_by_day produces intervals.
    # If 2021-08-01 to 2021-08-02, it's 1 chunk.
    assert len(urls) == 1

    dates_multi = pd.date_range("2021-08-01", "2021-08-03", freq="D")
    urls_multi = build_urls(dates_multi, split_by_day=True)
    assert len(urls_multi) == 2


def test_add_data_inv():
    dates = pd.date_range("2021/08/01", "2021/08/02")

    try:
        df = aeronet.add_data(dates, inv_type="ALM15", product="SIZ", as_xarray=False, retries=5)
        assert (df.inversion_data_quality_level == "lev15").all()
        assert (df.retrieval_measurement_scan_type == "Almucantar").all()

        df = aeronet.add_data(dates, inv_type="HYB15", product="SIZ", retries=5, as_xarray=False)
        assert (df.inversion_data_quality_level == "lev15").all()
        assert (df.retrieval_measurement_scan_type == "Hybrid").all()
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        if "valid query but no data found" in str(e):
            pytest.skip("No data found for the given query")
        raise


@pytest.mark.parametrize("product", ["AOD15", "SDA20"])
def test_add_data_all_noninv(product):
    dates = pd.date_range("2021/08/01", "2021/08/02")
    site = "Mauna_Loa"

    try:
        df = aeronet.add_data(dates, product=product, siteid=site, as_xarray=False, retries=5)
        assert df.index.size > 0
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        if "valid query but no data found" in str(e):
            pytest.skip("No data found for the given query")
        raise


def test_add_data_valid_empty_query():
    dates = pd.date_range("2021/08/01", "2021/08/02")
    site = "Banana_River"

    try:
        with pytest.raises(Exception, match="valid query but no data found"):
            aeronet.add_data(dates, product="AOD20", siteid=site, retries=1, as_xarray=False)
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        raise


def test_load_local():
    fp = DATA / "aeronet-AOD15-example.txt"
    assert fp.is_file()

    df = aeronet.add_local(fp, as_xarray=False)
    assert df.index.size > 0
    assert (df.siteid == "Mauna_Loa").all()
    assert df.attrs["info"].startswith("AERONET Data Download")


def test_load_local_inv():
    fp = DATA / "aeronet-inv-ALM15-SIZ-example.txt"
    assert fp.is_file()

    df = aeronet.add_local(fp, as_xarray=False)
    assert df.index.size > 0
    assert (df.siteid == "Cart_Site").all()


def test_add_data_lunar():
    dates = pd.date_range("2021/08/01", "2021/08/02")
    try:
        df = aeronet.add_data(
            dates, lunar=True, daily=True, retries=5, as_xarray=False
        )  # only daily-average data at this time
        assert len(df) > 0

        dates = pd.date_range("2022/01/20", "2022/01/21")
        df = aeronet.add_data(dates, lunar=True, siteid="Chilbolton", retries=5, as_xarray=False)
        assert len(df) > 0
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        if "valid query but no data found" in str(e):
            pytest.skip("No data found for the given query")
        raise


def test_serial_freq():
    # For MM data proc example
    dates = pd.date_range(start="2019-09-01", end="2019-09-2", freq="h")
    try:
        df = aeronet.add_data(dates, freq="2h", n_procs=1, as_xarray=False, retries=5)
        assert (
            pd.DatetimeIndex(sorted(df.time.unique()))
            == pd.date_range("2019-09-01", freq="2h", periods=12)
        ).all()
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        if "valid query but no data found" in str(e):
            pytest.skip("No data found for the given query")
        raise


@pytest.mark.skipif(has_pytspack, reason="has pytspack")
def test_interp_without_pytspack():
    import sys
    from unittest.mock import patch

    fp = DATA / "aeronet-AOD15-example.txt"
    standard_wavelengths = np.array([0.34, 0.44, 0.55, 0.66, 0.86, 1.63, 11.1]) * 1000
    # Explicitly mock pytspack as missing to avoid interference from other tests
    with patch.dict(sys.modules, {"pytspack": None}):
        with pytest.raises(RuntimeError, match="You must install pytspack"):
            aeronet.add_local(fp, interp_to_aod_values=standard_wavelengths)


@pytest.mark.skipif(not has_pytspack, reason="no pytspack")
def test_interp_with_pytspack():
    fp = DATA / "aeronet-AOD15-example.txt"
    standard_wavelengths = np.array([0.34, 0.44, 0.55, 0.66, 0.86, 1.63, 11.1]) * 1000
    with pytest.warns(UserWarning, match="Renaming duplicate AOD columns"):
        df = aeronet.add_local(
            fp,
            interp_to_aod_values=standard_wavelengths,
            as_xarray=False,
        )

    # Check for the new columns
    assert {f"aod_{int(wl)}nm" for wl in standard_wavelengths}.issubset(df.columns)

    # Check for renamed duplicate columns
    assert {c for c in df if c.startswith("aod_") and c.endswith("nm_orig")} == {
        "aod_340nm_orig",
        "aod_440nm_orig",
    }


@pytest.mark.skipif(not has_pytspack, reason="no pytspack")
def test_interp_daily_with_pytspack():
    # Use the SDA example for a different product if needed, but AOD15 is fine
    fp = DATA / "aeronet-AOD15-example.txt"
    standard_wavelengths = np.array([0.55]) * 1000
    df = aeronet.add_local(
        fp,
        interp_to_aod_values=standard_wavelengths,
        as_xarray=False,
    )

    assert {f"aod_{int(wl)}nm" for wl in standard_wavelengths}.issubset(df.columns)


@pytest.mark.parametrize("lazy", [True, False])
def test_aeronet_metadata_robustness(lazy):
    """Test that AERONET reader is robust to empty or bad files when using lazy loading."""
    import sys
    from unittest.mock import MagicMock

    import dask.dataframe as dd

    from monetio.readers.aeronet import AERONETReader

    # Mock pytspack for this test to verify interpolation logic even if not installed
    mock_pytspack = MagicMock()
    mock_pytspack.TsPack.return_value.interpolate.return_value = lambda wv: np.full(len(wv), 0.07)

    good_content = b"""AERONET Data
Version 3
AOD
Some info
Another info
Time(hh:mm:ss),Date(dd:mm:yyyy),Site,Latitude,Longitude,AOD_440nm,AOD_675nm,AOD_870nm,AOD_1020nm,440-870_Angstrom_Exponent
00:00:00,01:01:2024,TEST,0.0,0.0,0.1,0.05,0.03,0.02,1.0
"""
    empty_content = b"""AERONET Data
Version 3
AOD
Some info
Another info
Time(hh:mm:ss),Date(dd:mm:yyyy),Site,Latitude,Longitude,AOD_440nm,AOD_675nm,AOD_870nm,AOD_1020nm,440-870_Angstrom_Exponent
"""
    bad_content = b"<html><body>Error</body></html>"

    def mock_get(url, **kwargs):
        resp = MagicMock()
        if "good" in str(url):
            resp.content = good_content
        elif "empty" in str(url):
            resp.content = empty_content
        else:
            resp.content = bad_content
        resp.raise_for_status.return_value = None
        return resp

    with (
        patch("requests.Session.get", side_effect=mock_get),
        patch.dict(sys.modules, {"pytspack": mock_pytspack}),
    ):
        reader = AERONETReader()

        # Test loading a mix of good, empty and bad URLs
        files = ["http://good", "http://empty", "http://bad"]

        df = reader.open_dataset(
            files=files, detect_dust=True, interp_to_aod_values=[550.0], lazy=lazy, as_xarray=False
        )

        if lazy:
            assert isinstance(df, dd.DataFrame)
            # This should not trigger immediate compute of all partitions
            # but open_dataset now computes meta from the first file.

        res = df.compute() if hasattr(df, "compute") else df

        assert len(res) == 1
        assert "dust" in res.columns
        assert "aod_550nm" in res.columns
        assert res["siteid"].iloc[0] == "TEST"

        # Check that dtypes are consistent (not objects for numeric)
        assert res["aod_440nm"].dtype == np.float64
        assert res["latitude"].dtype == np.float64


@pytest.mark.parametrize(
    "dates",
    [
        pd.to_datetime(["2019-09-01", "2019-09-03"]),
        pd.to_datetime(["2019-09-01 00:00", "2019-09-01 12:00"]),
    ],
    ids=[
        "two days",
        "half day",
    ],
)
def test_issue100(dates, request):
    try:
        df1 = aeronet.add_data(dates, n_procs=1, as_xarray=False, retries=5)
        df2 = aeronet.add_data(dates, n_procs=2, as_xarray=False, retries=5)
        assert len(df1) == len(df2)
        if request.node.callspec.id == "two days":
            df1_ = df1.sort_values(["time", "siteid"]).reset_index(drop=True)
            df2_ = df2.sort_values(["time", "siteid"]).reset_index(drop=True)
            assert df1_.equals(df2_)
        else:
            assert df1.equals(df2)
        assert dates[0] <= df1.time.min() <= df1.time.max() <= dates[-1]
    except Exception as e:
        if is_connection_error(e):
            pytest.skip(f"Network connection failed: {e}")
        if "valid query but no data found" in str(e):
            pytest.skip("No data found for the given query")
        raise
