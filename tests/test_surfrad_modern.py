import pandas as pd
import xarray as xr

from monetio.readers.surfrad import SURFRADReader, read_surfrad


def test_read_surfrad_logic(tmp_path):
    # Mock SURFRAD file content
    # Line 1: Station Name
    # Line 2: Lat Lon Elev ...
    # Line 3+: Data
    header = "Bondville\n40.05192 -88.37309 213 1\n"
    # year jday month day hour minute dt zen dw_solar dw_solar_flag ... (48 cols total)
    data = "2024 1 1 1 0 0 0.0 162.0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0\n"
    fn = tmp_path / "bon24001.dat"
    fn.write_text(header + data)

    df = read_surfrad(str(fn))
    assert len(df) == 1
    assert df.siteid.iloc[0] == "Bondville"
    assert df.latitude.iloc[0] == 40.05192
    assert df.time.iloc[0] == pd.Timestamp("2024-01-01 00:00:00")


def test_surfrad_eager_lazy_consistency(tmp_path):
    header = "Bondville\n40.05192 -88.37309 213 1\n"
    data = "2024 1 1 1 0 0 0.0 162.0 10.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0\n"
    fn = tmp_path / "bon24001.dat"
    fn.write_text(header + data)

    reader = SURFRADReader()
    # 1. Eager
    ds_eager = reader.open_dataset(files=str(fn), as_xarray=True, lazy=False)
    # 2. Lazy
    ds_lazy = reader.open_dataset(files=str(fn), as_xarray=True, lazy=True)

    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    assert ds_eager.ghi.values[0, 0] == 10.0
    assert "Read SURFRAD dataset" in ds_eager.attrs["history"]
