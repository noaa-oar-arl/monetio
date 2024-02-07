import pandas as pd
import pytest

from monetio import gml_ozonesonde


def test_read_100m():
    url = r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/Boulder,%20Colorado/100%20Meter%20Average%20Files/bu1043_2023_12_27_17.l100"
    df = gml_ozonesonde.read_100m(url)
    assert len(df) > 0


def test_add_data():
    dates = pd.date_range("2023-01-01", "2023-01-31 23:59", freq="H")
    df = gml_ozonesonde.add_data(dates, n_procs=2)
    assert len(df) > 0

    assert df.attrs["var_attrs"]["o3"]["units"] == "ppmv"

    latlon = df["latitude"].astype(str) + "," + df["longitude"].astype(str)
    assert 1 < latlon.nunique() <= 10, "multiple sites; lat/lon doesn't change in profile"


def test_add_data_place_sel():
    dates = pd.date_range("2023-01-01", "2023-01-31 23:59", freq="H")
    df = gml_ozonesonde.add_data(
        dates,
        place=["Boulder, Colorado", "South Pole, Antartica"],
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
