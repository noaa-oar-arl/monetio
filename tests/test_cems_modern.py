import zipfile

import pandas as pd
import xarray as xr

from monetio.readers.cems import CEMSReader, read_cems


def test_read_cems_logic(tmp_path):
    # Create a mock CSV content
    csv_content = (
        "Facility Name,ORISPL Code,Fac ID,SO2 Lbs,NOx Lbs,CO2 Short Tons,Date,Hour,Latitude,Longitude,State Name\n"
        "Test Plant,123,456,10.5,5.2,100.0,2023-01-01,0,39.0,-77.0,Maryland\n"
    )
    fn = tmp_path / "2023md01.csv"
    fn.write_text(csv_content)

    df = read_cems(str(fn))
    assert len(df) == 1
    assert df.facility_name.iloc[0] == "Test Plant"
    assert df.so2_lbs.iloc[0] == 10.5
    assert df.time.iloc[0] == pd.Timestamp("2023-01-01 00:00:00")
    assert df.siteid.iloc[0] == "123"


def test_cems_eager_lazy_consistency(tmp_path):
    csv_content = (
        "Facility Name,ORISPL Code,Fac ID,SO2 Lbs,NOx Lbs,CO2 Short Tons,Date,Hour,Latitude,Longitude,State Name\n"
        "Test Plant,123,456,10.5,5.2,100.0,2023-01-01,0,39.0,-77.0,Maryland\n"
    )
    fn = tmp_path / "2023md01.zip"
    with zipfile.ZipFile(fn, "w") as z:
        z.writestr("2023md01.csv", csv_content)

    reader = CEMSReader()
    # 1. Eager
    ds_eager = reader.open_dataset(files=str(fn), as_xarray=True, lazy=False)
    # 2. Lazy
    ds_lazy = reader.open_dataset(files=str(fn), as_xarray=True, lazy=True)

    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    assert ds_eager.so2_lbs.attrs["units"] == "lbs"
    assert "Read CEMS data" in ds_eager.attrs["history"]
