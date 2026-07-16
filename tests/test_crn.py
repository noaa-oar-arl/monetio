from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.crn import CRNReader, read_crn
from monetio.readers.time_utils import parse_yyyymmdd_hhmm


@pytest.fixture
def mock_crn_file(tmp_path):
    d = tmp_path / "crn_hourly"
    d.mkdir()
    f = d / "CRNH0203-2023-AL_Fairhope_3_NE.txt"

    # HCOLS: WBANNO UTC_DATE UTC_TIME LST_DATE LST_TIME CRX_VN LONGITUDE LATITUDE T_CALC ...
    content = (
        "63893 20230101 1200 20230101 0600 1.000 -87.88 30.54 10.5 10.0 11.0 9.5 0.0 "
        + "0.0 " * 22
        + "\n"
        "63893 20230101 1300 20230101 0700 1.000 -87.88 30.54 10.6 10.1 11.1 9.6 0.0 "
        + "1.0 " * 22
        + "\n"
    )
    f.write_text(content)
    return str(f)


def test_parse_yyyymmdd_hhmm_logic():
    yyyymmdd = np.array([20230101, 20231231])
    hhmm = np.array([0, 2359])
    res = parse_yyyymmdd_hhmm(yyyymmdd, hhmm)
    expected = pd.to_datetime(["2023-01-01 00:00:00", "2023-12-31 23:59:00"]).values.astype(
        "datetime64[ns]"
    )
    np.testing.assert_array_equal(res, expected)

    # Dask/Xarray check
    try:
        import dask.array as da

        lazy_time = xr.apply_ufunc(
            parse_yyyymmdd_hhmm,
            xr.DataArray(da.from_array(yyyymmdd, chunks=1)),
            xr.DataArray(da.from_array(hhmm, chunks=1)),
            dask="parallelized",
            output_dtypes=[np.dtype("datetime64[ns]")],
        ).compute()
        np.testing.assert_array_equal(res, lazy_time.values)
    except ImportError:
        pass


def test_read_crn_logic(mock_crn_file):
    df = read_crn(mock_crn_file)
    assert len(df) == 2
    assert "time" in df.columns
    assert df["time"].iloc[0] == pd.Timestamp("2023-01-01 12:00")


@patch("monetio.readers.crn.CRNReader.get_monitor_df")
def test_crn_eager_lazy_consistency(mock_get_monitor, mock_crn_file):
    monitor_df = pd.DataFrame(
        {
            "WBAN": ["63893"],
            "STATE": ["AL"],
            "LOCATION": ["Fairhope"],
            "VECTOR": ["Test"],
            "LATITUDE": [30.54],
            "LONGITUDE": [-87.88],
            "NETWORK": ["USCRN"],
            "GMT_OFFSET": [-6.0],
        }
    )
    mock_get_monitor.return_value = monitor_df

    reader = CRNReader()
    ds_eager = reader.open_dataset(files=mock_crn_file, as_xarray=True, lazy=False)

    try:
        ds_lazy = reader.open_dataset(files=mock_crn_file, as_xarray=True, lazy=True)
        xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    except ImportError:
        pass

    assert "history" in ds_eager.attrs
    assert "Merged with CRN" in ds_eager.attrs["history"]


def test_crn_build_urls_optimization():
    reader = CRNReader()
    dates = pd.to_datetime(["2023-01-01"])
    monitor_df = pd.DataFrame(
        {
            "STATE": ["AL"],
            "LOCATION": ["Fairhope"],
            "VECTOR": ["Test"],
            "LATITUDE": [30.0],
            "LONGITUDE": [-80.0],
        }
    )

    with patch("monetio.readers.crn.CRNReader.get_monitor_df", return_value=monitor_df):
        with patch("monetio.readers.crn.FileUtility.get_fs") as mock_get_fs:
            mock_fs = MagicMock()
            mock_get_fs.return_value = mock_fs
            mock_fs.ls.return_value = ["CRNH0203-2023-AL_Fairhope_Test.txt"]
            urls, fnames = reader.build_urls(dates)
            assert len(urls) == 1
            assert mock_fs.ls.call_count == 1
