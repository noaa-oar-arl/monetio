from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from monetio import aeronet

DATA = Path(__file__).parent / "data"

try:
    import pytspack  # noqa: F401
except ImportError:
    has_pytspack = False
else:
    has_pytspack = True


def test_build_url_required_param_checks():
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


def test_build_url_bad_prod():
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
    assert (
        aeronet.get_valid_sites().columns == ["siteid", "longitude", "latitude", "elevation"]
    ).all()


def test_add_data_bad_siteid():
    with pytest.raises(ValueError, match="invalid site"):
        aeronet.add_data(siteid="Rivendell")


def test_add_data_one_site():
    dates = pd.date_range("2021/08/01", "2021/08/03")
    df = aeronet.add_data(dates, siteid="SERC")
    assert df.index.size > 0
    assert (df.siteid == "SERC").all()
    assert df.attrs["info"].startswith("AERONET Data Download")


def test_add_data_inv():
    dates = pd.date_range("2021/08/01", "2021/08/02")

    df = aeronet.add_data(dates, inv_type="ALM15", product="SIZ")
    assert df.inversion_data_quality_level.eq("lev15").all()
    assert df.retrieval_measurement_scan_type.eq("Almucantar").all()

    df = aeronet.add_data(dates, inv_type="HYB15", product="SIZ")
    assert df.inversion_data_quality_level.eq("lev15").all()
    assert df.retrieval_measurement_scan_type.eq("Hybrid").all()

    # TODO: find a time with Level 2.0 retrievals


@pytest.mark.parametrize("product", aeronet.AERONET._valid_prod_noninv)
def test_add_data_all_noninv(product):
    dates = pd.date_range("2021/08/01", "2021/08/02")
    site = "Mauna_Loa"

    df = aeronet.add_data(dates, product=product, siteid=site)
    assert df.index.size > 0


def test_add_data_valid_empty_query():
    dates = pd.date_range("2021/08/01", "2021/08/02")
    site = "Banana_River"

    with pytest.raises(Exception, match="loading from URL .+ failed") as ei:
        aeronet.add_data(dates, product="AOD20", siteid=site)
    assert "valid query but no data found" in str(ei.value.__cause__)


# [21.1,-131.6686,53.04,-58.775]


def test_load_local():
    # The example file is based on one of the provided examples:
    # https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?year=2000&month=6&day=1&year2=2000&month2=6&day2=14&AOD15=1&AVG=10
    # but with
    # - no-HTML mode
    # - site `Mauna_Loa` selected
    # https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_v3?year=2000&month=6&day=1&year2=2000&month2=6&day2=14&AOD15=1&AVG=10&if_no_html=1&site=Mauna_Loa

    fp = DATA / "aeronet-AOD15-example.txt"
    assert fp.is_file()

    df = aeronet.add_local(fp)
    assert df.index.size > 0
    assert (df.siteid == "Mauna_Loa").all(0)
    assert df.attrs["info"].startswith("AERONET Data Download")


def test_load_local_inv():
    # One of the provided examples:
    # https://aeronet.gsfc.nasa.gov/cgi-bin/print_web_data_inv_v3?site=Cart_Site&year=2002&month=6&day=1&year2=2003&month2=6&day2=14&product=SIZ&AVG=20&ALM15=1&if_no_html=1

    fp = DATA / "aeronet-inv-ALM15-SIZ-example.txt"
    assert fp.is_file()

    df = aeronet.add_local(fp)
    assert df.index.size > 0
    assert (df.siteid == "Cart_Site").all(0)


def test_add_data_lunar():
    dates = pd.date_range("2021/08/01", "2021/08/02")
    df = aeronet.add_data(dates, lunar=True, daily=True)  # only daily-average data at this time
    assert df.index.size > 0

    dates = pd.date_range("2022/01/20", "2022/01/21")
    df = aeronet.add_data(dates, lunar=True, siteid="Chilbolton")
    assert df.index.size > 0


def test_serial_freq():
    # For MM data proc example
    dates = pd.date_range(start="2019-09-01", end="2019-09-2", freq="H")
    df = aeronet.add_data(dates, freq="2H", n_procs=1)
    assert (
        pd.DatetimeIndex(sorted(df.time.unique()))
        == pd.date_range("2019-09-01", freq="2H", periods=12)
    ).all()


@pytest.mark.skipif(has_pytspack, reason="has pytspack")
def test_interp_without_pytspack():
    # For MM data proc example
    dates = pd.date_range(start="2019-09-01", end="2019-09-2", freq="H")
    standard_wavelengths = np.array([0.34, 0.44, 0.55, 0.66, 0.86, 1.63, 11.1]) * 1000
    with pytest.raises(RuntimeError, match="You must install pytspack"):
        aeronet.add_data(dates, n_procs=1, interp_to_aod_values=standard_wavelengths)


@pytest.mark.skipif(not has_pytspack, reason="no pytspack")
def test_interp_with_pytspack():
    # For MM data proc example
    dates = pd.date_range(start="2019-09-01", end="2019-09-2", freq="H")
    standard_wavelengths = np.array([0.34, 0.44, 0.55, 0.66, 0.86, 1.63, 11.1]) * 1000
    with pytest.warns(UserWarning, match="Renaming duplicate AOD columns"):
        df = aeronet.add_data(dates, n_procs=1, interp_to_aod_values=standard_wavelengths)
    # Note: default wls for this period:
    #
    # wls = sorted(df.columns[df.columns.str.startswith("aod")].str.slice(4, -2).astype(int).tolist())
    #
    # [340, 380, 400, 412, 440,
    #  443, 490, 500, 510, 532,
    #  551, 555, 560, 620, 667,
    #  675, 681, 709, 779, 865,
    #  870, 1020, 1640]
    #
    # Note: Some of the ones we want already are in there (340 and 440 nm)

    # Check for the new columns
    assert {f"aod_{int(wl)}nm" for wl in standard_wavelengths}.issubset(df.columns)

    # Check for renamed duplicate columns
    assert {c for c in df if c.startswith("aod_") and c.endswith("nm_orig")} == {
        "aod_340nm_orig",
        "aod_440nm_orig",
    }
    assert {
        c for c in df if c.startswith("exact_wavelengths_of_aod") and c.endswith("nm_orig")
    } == {"exact_wavelengths_of_aod(um)_340nm_orig", "exact_wavelengths_of_aod(um)_440nm_orig"}


@pytest.mark.skipif(not has_pytspack, reason="no pytspack")
def test_interp_daily_with_pytspack():
    dates = pd.date_range(start="2019-09-01", end="2019-09-2", freq="H")
    standard_wavelengths = np.array([0.55]) * 1000
    df = aeronet.add_data(dates, daily=True, n_procs=1, interp_to_aod_values=standard_wavelengths)

    assert {f"aod_{int(wl)}nm" for wl in standard_wavelengths}.issubset(df.columns)


@pytest.mark.parametrize(
    "dates",
    [
        pd.to_datetime(["2019-09-01", "2019-09-02"]),
        pd.to_datetime(["2019-09-01", "2019-09-03"]),
        pd.to_datetime(["2019-09-01", "2019-09-01 12:00"]),
    ],
    ids=[
        "one day",
        "two days",
        "half day",
    ],
)
def test_issue100(dates, request):
    df1 = aeronet.add_data(dates, n_procs=1)
    df2 = aeronet.add_data(dates, n_procs=2)
    assert len(df1) == len(df2)
    if request.node.callspec.id == "two days":
        # Sort first (can use `df1.compare(df2)` for debugging)
        # Seems the sorting is site then time, not time then site
        # which is why this is necessary
        df1_ = df1.sort_values(["time", "siteid"]).reset_index(drop=True)
        df2_ = df2.sort_values(["time", "siteid"]).reset_index(drop=True)
        assert df1_.equals(df2_)
    else:
        assert df1.equals(df2)
    assert dates[0] < df1.time.min() < df1.time.max() < dates[-1]
