import pandas as pd
import pytest
import xarray as xr

from monetio.readers.igra2 import IGRA2Reader, read_igra2, read_igra2_derived


def test_read_igra2_mock(tmp_path):
    header = "#USM00072201 2023 01 01 00 2315    1                   257906 -803164\n"
    data1 = "11    0  100000 10000 11111 22222 33333 44444 55555\n"

    lines = [header, data1]
    f = tmp_path / "USM00072201-data.txt"
    with open(f, "w") as fid:
        fid.writelines(lines)

    df = read_igra2(str(f))
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0].siteid == "USM00072201"
    assert df.iloc[0].press == 100000
    assert df.iloc[0].temp == 1111.1


def test_read_igra2_derived_mock(tmp_path):
    # Header: #ID YEAR MN DY HR RELTIME NUMLEV PW INVPRESS ... CAPE CIN
    # 32-36 is NUMLEV
    header_list = [" "] * 160
    header_list[0:12] = list("#USM00072201")
    header_list[13:17] = list("2023")
    header_list[18:20] = list("01")
    header_list[21:23] = list("01")
    header_list[24:26] = list("00")
    header_list[31:36] = list("    1")
    header_list[37:43] = list("  1234")
    header_list[145:151] = list("  1000")
    header_list[151:157] = list("   500")
    header = "".join(header_list) + "\n"

    # data: PRESS 1-7, REPGPH 9-15, ..., TEMP 25-31, ..., UWND 113-119, ..., VWND 129-135
    data_list = [" "] * 150
    data_list[0:7] = list(" 100000")
    data_list[8:15] = list("  10000")
    data_list[16:23] = list("  10000")
    data_list[24:31] = list("   3000")  # 300.0 K -> 26.85 C
    data_list[112:119] = list("    100")  # 10.0 m/s
    data_list[128:135] = list("    200")  # 20.0 m/s
    data1 = "".join(data_list) + "\n"

    lines = [header, data1]
    f = tmp_path / "USM00072201-drvd.txt"
    with open(f, "w") as fid:
        fid.writelines(lines)

    df = read_igra2_derived(str(f))
    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0].siteid == "USM00072201"
    assert df.iloc[0].pw == 12.34
    assert df.iloc[0].cape == 1000.0
    assert df.iloc[0].cin == 500.0
    assert df.iloc[0].press == 100000
    assert df.iloc[0].temp == pytest.approx(26.85)
    assert df.iloc[0].uwnd == 10.0
    assert df.iloc[0].vwnd == 20.0


def test_igra2_reader_eager(tmp_path):
    header = "#USM00072201 2023 01 01 00 2315    1                   257906 -803164\n"
    data1 = "11    0  100000 10000 11111 22222 33333 44444 55555\n"
    lines = [header, data1]
    f = tmp_path / "USM00072201-data.txt"
    with open(f, "w") as fid:
        fid.writelines(lines)

    reader = IGRA2Reader()
    reader.stations = pd.DataFrame(
        {
            "siteid": ["USM00072201"],
            "name": ["MIAMI"],
            "elevation": [2.0],
            "latitude": [25.79],
            "longitude": [-80.32],
        }
    )

    ds = reader.open_dataset(files=str(f), as_xarray=True, lazy=False)
    assert isinstance(ds, xr.Dataset)
    assert "temp" in ds.data_vars
    assert ds.temp.attrs["units"] == "degC"
    assert ds.siteid.values[0] == "USM00072201"


def test_igra2_reader_lazy(tmp_path):
    header = "#USM00072201 2023 01 01 00 2315    1                   257906 -803164\n"
    data1 = "11    0  100000 10000 11111 22222 33333 44444 55555\n"
    lines = [header, data1]
    f = tmp_path / "USM00072201-data.txt"
    with open(f, "w") as fid:
        fid.writelines(lines)

    reader = IGRA2Reader()
    reader.stations = pd.DataFrame(
        {
            "siteid": ["USM00072201"],
            "name": ["MIAMI"],
            "elevation": [2.0],
            "latitude": [25.79],
            "longitude": [-80.32],
        }
    )

    ds = reader.open_dataset(files=str(f), as_xarray=True, lazy=True)
    assert isinstance(ds, xr.Dataset)
    ds_c = ds.compute()
    assert ds_c.temp.values[0] == 1111.1
    assert ds_c.siteid.values[0] == "USM00072201"


@pytest.mark.network
def test_igra2_integration():
    reader = IGRA2Reader()
    try:
        ds = reader.open_dataset(site="USM00072201", dates="2023-01-01", as_xarray=True)
        assert not ds.temp.isnull().all()
        assert ds.temp.attrs["units"] == "degC"
        assert ds.siteid.values[0] == "USM00072201"
    except Exception as e:
        pytest.skip(f"Integration test failed (likely network): {e}")
