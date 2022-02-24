from pathlib import Path

import pandas as pd
import pytest

from monetio import aeronet

DATA = Path(__file__).parent / "data"


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
    site = "Tucson"

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
    df = aeronet.add_data(dates, lunar=True, siteid="Tucson")
    assert df.index.size > 0
