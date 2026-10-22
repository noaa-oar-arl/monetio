import pandas as pd
import pytest

from monetio.readers.epa_utils import convert_epa_unit, convert_statenames_to_abv


def test_convert_statenames_to_abv():
    df = pd.DataFrame({"state_name": ["Alabama", "California", "Mexico", "Unknown"]})
    df_conv = convert_statenames_to_abv(df.copy())
    assert df_conv["state_name"].iloc[0] == "AL"
    assert df_conv["state_name"].iloc[1] == "CA"
    assert df_conv["state_name"].iloc[2] == "MM"
    assert df_conv["state_name"].iloc[3] == "Unknown"


def test_convert_epa_unit_eager():
    df = pd.DataFrame({"obs": [10.0, 20.0], "units": ["ppb", "ug/m3"]})
    # Convert ppb to ug/m3 for SO2 (factor 2.6178)
    df_conv = convert_epa_unit(df.copy(), species="SO2", to_unit="ug/m3")
    assert df_conv["obs"].iloc[0] == pytest.approx(10.0 * 2.6178)
    assert df_conv["units"].iloc[0] == "ug/m3"
    assert df_conv["obs"].iloc[1] == 20.0  # Already ug/m3, should stay
    assert df_conv["units"].iloc[1] == "ug/m3"
    assert "Converted SO2 to ug/m3" in df_conv.attrs["history"]


def test_convert_epa_unit_lazy():
    dd = pytest.importorskip("dask.dataframe")
    df = pd.DataFrame({"obs": [10.0, 20.0], "units": ["ppb", "ug/m3"]})
    ddf = dd.from_pandas(df, npartitions=2)

    ddf_conv = convert_epa_unit(ddf, species="SO2", to_unit="ug/m3")
    res = ddf_conv.compute()

    assert res["obs"].iloc[0] == pytest.approx(10.0 * 2.6178)
    assert res["units"].iloc[0] == "ug/m3"
    assert res["obs"].iloc[1] == 20.0
    if hasattr(ddf_conv, "attrs") and "history" in ddf_conv.attrs:
        assert "Converted SO2 to ug/m3" in ddf_conv.attrs["history"]
