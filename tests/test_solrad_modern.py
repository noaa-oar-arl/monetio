import pandas as pd
import xarray as xr

from monetio.readers.solrad import SOLRADReader, read_solrad


def test_read_solrad_logic(tmp_path):
    # Mock SOLRAD file content
    header = "Albuquerque_NM\n35.05 -106.62 1617 7\n"
    # WIDTHS = [4, 3, 2, 2, 2, 2, 6, 6] + 5 * [7, 1] + 4 * [9]
    # Each followed by 1 space in get_colspecs
    widths = [4, 3, 2, 2, 2, 2, 6, 6, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 9, 9, 9, 9]
    values = [
        2024,
        1,
        1,
        1,
        0,
        0,
        0.0,
        150.0,
        10.0,
        0,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0.0,
        0.0,
        0.0,
    ]
    data_line = "".join([f"{str(v):>{w}} " for v, w in zip(values, widths)]) + "\n"

    fn = tmp_path / "abq24001.dat"
    fn.write_text(header + data_line)

    df = read_solrad(str(fn))
    assert len(df) == 1
    assert df.siteid.iloc[0] == "Albuquerque_NM"
    assert df.latitude.iloc[0] == 35.05
    assert df.time.iloc[0] == pd.Timestamp("2024-01-01 00:00:00")
    assert float(df.ghi.iloc[0]) == 10.0


def test_solrad_eager_lazy_consistency(tmp_path):
    header = "Albuquerque_NM\n35.05 -106.62 1617 7\n"
    widths = [4, 3, 2, 2, 2, 2, 6, 6, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 9, 9, 9, 9]
    values = [
        2024,
        1,
        1,
        1,
        0,
        0,
        0.0,
        150.0,
        20.0,
        0,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0,
        0.0,
        0.0,
        0.0,
        0.0,
    ]
    data_line = "".join([f"{str(v):>{w}} " for v, w in zip(values, widths)]) + "\n"

    fn = tmp_path / "abq24001.dat"
    fn.write_text(header + data_line)

    reader = SOLRADReader()
    # 1. Eager
    ds_eager = reader.open_dataset(files=str(fn), as_xarray=True, lazy=False)
    # 2. Lazy
    ds_lazy = reader.open_dataset(files=str(fn), as_xarray=True, lazy=True)

    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    assert float(ds_eager.ghi.values[0, 0]) == 20.0
    assert "Read SOLRAD dataset" in ds_eager.attrs["history"]
