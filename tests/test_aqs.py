import os
from pathlib import Path

import pandas as pd
import pytest

from monetio import aqs

HERE = Path(__file__).parent
DATA = HERE / "data"


def test_aqs_daily_long():
    # For MM data proc example
    dates = pd.date_range(start="2019-08-01", end="2019-08-31", freq="D")
    # Note: will retrieve full year
    network = "NCORE"  # CSN NCORE CASTNET
    with pytest.warns(UserWarning, match="Short names not available for these variables"):
        df = aqs.add_data(
            dates,
            param=["PM10SPEC"],
            network=network,
            wide_fmt=False,
            daily=True,
        )
    assert (df.variable == "").sum() == 0
    t = df.time
    assert ((t.dt.year == 2019) & (t.dt.month == 8)).all()


def test_issue263():
    # melodies-monet get-aqs -s 2021-01-01 -e 2021-01-31
    dates = pd.date_range(start="2021-01-01", end="2021-01-31", freq="h")
    df = aqs.add_data(
        dates,
        param=["O3", "PM2.5", "PM10"],
        network=None,
        wide_fmt=True,
        daily=False,
    )
    t = df.time
    assert ((t.dt.year == 2021) & (t.dt.month == 1)).all()


def test_aqs_daily_wide():
    dates = pd.date_range(start="2019-08-01", end="2019-08-31", freq="D")
    df = aqs.add_data(
        dates,
        param=["O3", "PM2.5"],
        network="IMPROVE",
        wide_fmt=True,
        daily=True,
    )
    t = df.time
    assert ((t.dt.year == 2019) & (t.dt.month == 8)).all()


def test_issue265(tmp_path):
    # Some sites may erroneously have time not on the UTC hour
    # e.g. for 2025 PM2.5, one site has times 26 min after the hour.
    # This extract has the first *local* day of the first site in the file
    # and the first *local* day of that site
    p0 = DATA / "aqs_hourly_88101_2025_issue265_extract.zip"

    # Copy to temp dir with the expected file name
    p = tmp_path / "hourly_88101_2025.zip"
    p.write_bytes(p0.read_bytes())
    os.chdir(tmp_path)

    # Open the raw file for comparison
    df_raw = pd.read_csv(p)
    assert len(df_raw) == 48
    df_raw["time"] = pd.to_datetime(df_raw["Date GMT"] + " " + df_raw["Time GMT"])
    df_raw["time_local"] = pd.to_datetime(df_raw["Date Local"] + " " + df_raw["Time Local"])
    is_off = df_raw.time.dt.minute == 26
    assert is_off.sum() == 24
    (utcoffset_raw,) = (df_raw[is_off].time_local - df_raw[is_off].time).unique()

    # Extend a bit into the next UTC day so as to not lose any records
    dates = pd.date_range(start="2025-01-01", end="2025-01-02 23:00", freq="h")

    with pytest.warns(
        UserWarning,
        match=r"24 records are not on the hour\. "
        r"Rounding down to the nearest hour\. "
        r"Affected sites include: \['060730077'\]\.",
    ):
        df = aqs.add_data(
            dates,
            param=["PM2.5"],
            network=None,
            wide_fmt=False,
            daily=False,
            local=True,
        )

    assert len(df) == len(df_raw)
    assert df.index.equals(df_raw.index)
    assert (df.time == df.time.dt.floor("h")).all()
    df_ = df.query("siteid == '060730077'")
    (utcoffset,) = (df_.time_local - df_.time).unique()
    assert utcoffset == utcoffset_raw == pd.Timedelta(hours=-8)
