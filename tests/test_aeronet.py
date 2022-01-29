import pandas as pd
import pytest

from monetio import aeronet


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


def test_add_data_inv():
    dates = pd.date_range("2021/08/01", "2021/08/02")

    df = aeronet.add_data(dates, inv_type="ALM15", product="SIZ")
    assert df.inversion_data_quality_level.eq("lev15").all()
    assert df.retrieval_measurement_scan_type.eq("Almucantar").all()

    df = aeronet.add_data(dates, inv_type="HYB15", product="SIZ")
    assert df.inversion_data_quality_level.eq("lev15").all()
    assert df.retrieval_measurement_scan_type.eq("Hybrid").all()

    # TODO: find a time with Level 2.0 retrievals


# [21.1,-131.6686,53.04,-58.775]
