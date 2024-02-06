import pandas as pd

from monetio import gml_ozonesonde


def test_read_100m():
    url = r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/Boulder,%20Colorado/100%20Meter%20Average%20Files/bu1043_2023_12_27_17.l100"
    df = gml_ozonesonde.read_100m(url)
    assert len(df) > 0


def test_add_data():
    dates = pd.date_range("2023-01-01", "2023-02-01")[:-1]
    df = gml_ozonesonde.add_data(dates, n_procs=2)
    assert len(df) > 0

    latlon = df["latitude"].astype(str) + "," + df["longitude"].astype(str)
    assert 1 < latlon.nunique() <= 10, "multiple sites; lat/lon doesn't change in profile"
