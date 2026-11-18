import zipfile

import pandas as pd
import pytest
import xarray as xr

from monetio.readers.cems import CEMSReader, read_cems
from monetio.readers.errors import OptionUnsupportedError


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


@pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
@pytest.mark.parametrize("use_dask", [False, True])
def test_cems_rejects_array_chunks_before_file_access(tmp_path, as_xarray, use_dask):
    with pytest.raises(OptionUnsupportedError, match="cems.*chunks"):
        CEMSReader().open_dataset(
            files=str(tmp_path / "missing-cems.zip"),
            as_xarray=as_xarray,
            use_dask=use_dask,
            chunks={"time": 1},
        )


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


def test_cems_lazy_table_schema_probe_is_bounded(tmp_path, monkeypatch):
    import dask.dataframe as dd

    import monetio.readers.cems as cems_module

    csv_content = (
        "Facility Name,ORISPL Code,Fac ID,SO2 Lbs,NOx Lbs,CO2 Short Tons,Date,Hour,Latitude,Longitude,State Name\n"
        "Test Plant,123,456,10.5,5.2,100.0,2023-01-01,0,39.0,-77.0,Maryland\n"
        "Test Plant,123,456,11.5,6.2,101.0,2023-01-01,1,39.0,-77.0,Maryland\n"
    )
    path = tmp_path / "2023md01.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("2023md01.csv", csv_content)

    original = cems_module.read_cems
    calls = []

    def counted(source, **kwargs):
        calls.append(kwargs.get("nrows"))
        return original(source, **kwargs)

    monkeypatch.setattr(cems_module, "read_cems", counted)
    reader = CEMSReader()
    eager = reader.open_dataset(files=str(path), as_xarray=False)
    calls.clear()
    lazy = reader.open_dataset(files=str(path), as_xarray=False, use_dask=True)

    assert isinstance(lazy, dd.DataFrame)
    assert calls == [5]
    pd.testing.assert_frame_equal(lazy.compute(), eager, check_dtype=False)
    assert calls == [5, None]


@pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
def test_cems_month_state_selection_limits_input_files(tmp_path, monkeypatch, as_xarray):
    from monetio.readers import cems as cems_module

    content = (
        "Facility Name,ORISPL Code,Fac ID,SO2 Lbs,NOx Lbs,CO2 Short Tons,Date,Hour,"
        "Latitude,Longitude,State Name\n"
        "Test Plant,123,456,10.5,5.2,100.0,2023-01-01,0,39.0,-77.0,Maryland\n"
    )
    selected = tmp_path / "2023md01.zip"
    with zipfile.ZipFile(selected, "w") as archive:
        archive.writestr("2023md01.csv", content)
    calls = []

    def build_url(date, state):
        calls.append((pd.Timestamp(date), state))
        return str(selected)

    monkeypatch.setattr(cems_module, "build_url", build_url)
    result = CEMSReader().open_dataset(
        dates=["2023-01-01", "2023-01-31"],
        states="md",
        as_xarray=as_xarray,
        use_dask=True,
    )
    assert calls == [(pd.Timestamp("2023-01-01"), "md")]
    if as_xarray:
        assert result.so2_lbs.chunks is not None
        assert result.sizes["time"] == 1
        result.close()
    else:
        import dask.dataframe as dd

        assert isinstance(result, dd.DataFrame)
        assert len(result.compute()) == 1
