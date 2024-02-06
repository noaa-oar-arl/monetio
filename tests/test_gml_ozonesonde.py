from monetio import gml_ozonesonde


def test_read_100m():
    url = r"https://gml.noaa.gov/aftp/data/ozwv/Ozonesonde/Boulder,%20Colorado/100%20Meter%20Average%20Files/bu1043_2023_12_27_17.l100"
    df = gml_ozonesonde.read_100m(url)
    assert len(df) > 0
