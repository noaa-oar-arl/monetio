import pandas as pd
import pytest

from monetio import gml_ozonesonde


def test_discover_files():
    files = gml_ozonesonde.discover_files()
    assert len(files) > 0
    assert set(files["place"].unique()) == set(gml_ozonesonde.PLACES)


def test_read_100m():
    url = r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/Boulder,%20Colorado/100%20Meter%20Average%20Files/bu1043_2023_12_27_17.l100"
    df = gml_ozonesonde.read_100m(url)
    assert len(df) > 0

    assert df.attrs["ds_attrs"]["Station"] == "Boulder, CO"
    assert df.attrs["ds_attrs"]["Station Height"] == "1743 meters"
    assert df.attrs["ds_attrs"]["Flight Number"] == "BU1043"
    assert df.attrs["ds_attrs"]["O3 Sonde ID"] == "2z43312"
    assert df.attrs["ds_attrs"]["Background"] == "0.020 microamps (0.08 mPa)"
    assert df.attrs["ds_attrs"]["Flowrate"] == "29.89 sec/100ml"
    assert df.attrs["ds_attrs"]["RH Corr"] == "0.31 %"
    assert df.attrs["ds_attrs"]["Sonde Total O3"] == "329 (65) DU"
    assert df.attrs["ds_attrs"]["Sonde Total O3 (SBUV)"] == "325 (62) DU"


@pytest.mark.parametrize(
    "url",
    [
        # Missing 'O3 Uncert'
        r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/San%20Cristobal,%20Galapagos/100%20Meter%20Average%20Files/sc204_2002_02_01_03.l100",
        # Missing 'O3 Uncert' + different header blocks (only 1)
        r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/Narragansett,%20Rhode%20Island/100%20Meter%20Average%20Files/ri058_2004_08_05_18.l100",
    ],
)
def test_read_100m_nonstd(url):
    df = gml_ozonesonde.read_100m(url)
    assert len(df) > 0


def test_read_100m_bad_data_line():
    url = r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/San%20Cristobal,%20Galapagos/100%20Meter%20Average%20Files/sc204_2002_01_31_12.l100"
    # Level   Press    Alt   Pottp   Temp   FtempV   Hum  Ozone  Ozone   Ozone  Ptemp  O3 # DN O3 Res
    #  Num     hPa      km     K      C       C       %    mPa    ppmv   atmcm    C   10^11/cc   DU
    #    0 -6331.0   0.008     0.0-3323.0   999.9    999-6666.00 10.529  0.0000  -91.8 1583.081    260
    #    1   892.2   0.100   301.1   18.3    19.1    105   1.07  0.012  0.0009   32.3    2.649    259

    with pytest.raises(ValueError, match="Expected 13 columns in data block"):
        _ = gml_ozonesonde.read_100m(url)


def test_add_data():
    dates = pd.date_range("2023-01-01", "2023-01-31 23:59", freq="H")
    df = gml_ozonesonde.add_data(dates, n_procs=2)
    assert len(df) > 0

    assert df.attrs["var_attrs"]["o3"]["units"] == "ppmv"

    latlon = df["latitude"].astype(str) + "," + df["longitude"].astype(str)
    assert 1 < latlon.nunique() <= 10, "multiple sites; lat/lon doesn't change in profile"

    # NOTE: Similar to the place folder names, but not all the same
    assert df["station"].nunique() == latlon.nunique()


def test_add_data_place_sel():
    dates = pd.date_range("2023-01-01", "2023-01-31 23:59", freq="H")
    df = gml_ozonesonde.add_data(
        dates,
        place=["Boulder, Colorado", "South Pole, Antarctica"],
        n_procs=2,
    )
    assert len(df) > 0

    latlon = df["latitude"].astype(str) + "," + df["longitude"].astype(str)
    assert latlon.nunique() == 2, "selected two places"


@pytest.mark.parametrize(
    "place",
    ["asdf", ["asdf", "blah"], ("asdf", "blah")],
)
def test_add_data_invalid_place(place):
    dates = pd.date_range("2023-01-01", "2023-01-31 23:59", freq="H")
    with pytest.raises(ValueError, match="Invalid place"):
        _ = gml_ozonesonde.add_data(dates, place=place)
