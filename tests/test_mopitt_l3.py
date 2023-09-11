import warnings
from pathlib import Path

import pandas as pd

from monetio.sat._mopitt_l3_mm import get_start_time, load_variable

HERE = Path(__file__).parent


def get_test_path():
    fn = "MOP03JM-201701-L3V95.9.3.he5"
    p = HERE / "data" / fn
    if not p.is_file():
        warnings.warn(f"Downloading test file {fn} for MOPITT L3 test")
        import requests

        r = requests.get(
            "https://csl.noaa.gov/groups/csl4/modeldata/melodies-monet/data/"
            f"example_observation_data/satellite/{fn}",
            stream=True,
        )
        r.raise_for_status()
        with open(p, "wb") as f:
            f.write(r.content)

    return p


def test_get_start_time():
    t = get_start_time(get_test_path())
    assert t.floor("D") == pd.Timestamp("2017-01-01")


def test_read_da():
    ds = load_variable(get_test_path(), "column")
    assert set(ds.coords) == {"lon", "lat"}
    assert set(ds) == {"column"}
    assert ds.column.mean() > 0
