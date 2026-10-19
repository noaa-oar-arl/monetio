import pandas as pd
import pytest
import xarray as xr

from monetio.readers.gml_ozonesonde import GMLOzonesondeReader, read_100m


def test_gml_ozonesonde_eager_lazy(tmp_path):
    # Create a dummy .l100 file
    # Note: read_100m expects 2 blocks if header is simple, or 5 blocks.
    # We use 2 blocks here.
    dummy_content = """Station: Boulder, CO
Launch Date: 2023-12-27
Launch Time: 17:00:00
Latitude: 40.0
Longitude: -105.0
Station Height: 1743 meters
Flight Number: BU1043

Level   Press    Alt   Pottp   Temp   FtempV   Hum  Ozone  Ozone   Ozone  Ptemp  O3 # DN O3 Res  O3 Uncert
 Num     hPa      km     K      C       C       %    mPa    ppmv   atmcm    C   10^11/cc   DU          %
    0   892.2   0.100   301.1   18.3    19.1    105   1.07  0.012  0.0009   32.3    2.649    259   5.0
    1   800.0   1.000   310.0   15.0    16.0    90    1.50  0.020  0.0015   30.0    3.000    250   4.0
"""
    f = tmp_path / "test.l100"
    f.write_text(dummy_content)

    reader = GMLOzonesondeReader()

    # Eager Mode
    ds_eager = reader.open_dataset(files=str(f), as_xarray=True, lazy=False, expand2d=False)
    assert isinstance(ds_eager, xr.Dataset)
    assert "o3" in ds_eager.data_vars
    assert ds_eager.sizes["time"] == 2
    assert ds_eager.o3.attrs["units"] == "ppmv"
    assert pd.Timestamp(ds_eager.time.values[0]) == pd.Timestamp("2023-12-27 17:00:00")

    # Lazy Mode
    ds_lazy = reader.open_dataset(files=str(f), as_xarray=True, lazy=True, expand2d=False)
    assert isinstance(ds_lazy, xr.Dataset)
    # Check if it's actually lazy (dask-backed)
    assert hasattr(ds_lazy.o3.data, "dask")

    # Assert identical values (excluding history which has timestamps)
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )

    # The native table output must also honor the explicit lazy alias.
    df_eager = reader.open_dataset(files=str(f), as_xarray=False, lazy=False)
    df_lazy = reader.open_dataset(files=str(f), as_xarray=False, lazy=True)
    assert isinstance(df_eager, pd.DataFrame)
    assert hasattr(df_lazy, "compute")
    pd.testing.assert_frame_equal(df_eager, df_lazy.compute(), check_dtype=False)

    # Check siteid normalization
    assert ds_eager.siteid.values[0] == "Boulder, Colorado"

    # Check attrs (for existing tests compatibility)
    df = read_100m(str(f))
    assert df.attrs["ds_attrs"]["Station"] == "Boulder, CO"
    assert df.attrs["var_attrs"]["o3"]["units"] == "ppmv"


def test_gml_ozonesonde_no_uncert(tmp_path):
    # Create a dummy .l100 file without uncertainty
    dummy_content = """Station: Boulder, CO
Launch Date: 2023-12-27
Launch Time: 17:00:00
Latitude: 40.0
Longitude: -105.0
Station Height: 1743 meters
Flight Number: BU1043

Level   Press    Alt   Pottp   Temp   FtempV   Hum  Ozone  Ozone   Ozone  Ptemp  O3 # DN O3 Res
 Num     hPa      km     K      C       C       %    mPa    ppmv   atmcm    C   10^11/cc   DU
    0   892.2   0.100   301.1   18.3    19.1    105   1.07  0.012  0.0009   32.3    2.649    259
"""
    f = tmp_path / "test_no_uncert.l100"
    f.write_text(dummy_content)

    reader = GMLOzonesondeReader()

    # Eager Mode
    ds_eager = reader.open_dataset(files=str(f), as_xarray=True, lazy=False, expand2d=False)
    assert "o3" in ds_eager.data_vars
    assert "o3_uncert" not in ds_eager.data_vars

    # Lazy Mode
    ds_lazy = reader.open_dataset(files=str(f), as_xarray=True, lazy=True, expand2d=False)
    assert "o3_uncert" not in ds_lazy.data_vars

    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )


@pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
def test_gml_date_location_selection_precedes_file_parsing(tmp_path, monkeypatch, as_xarray):
    from monetio.readers import gml_ozonesonde

    valid = tmp_path / "boulder_2023122717.l100"
    valid.write_text(
        """Station: Boulder, CO
Launch Date: 2023-12-27
Launch Time: 17:00:00
Latitude: 40.0
Longitude: -105.0
Station Height: 1743 meters
Flight Number: BU1043

Level   Press    Alt   Pottp   Temp   FtempV   Hum  Ozone  Ozone   Ozone  Ptemp  O3 # DN O3 Res  O3 Uncert
 Num     hPa      km     K      C       C       %    mPa    ppmv   atmcm    C   10^11/cc   DU          %
    0   892.2   0.100   301.1   18.3    19.1    105   1.07  0.012  0.0009   32.3    2.649    259   5.0
"""
    )
    unrelated = tmp_path / "unselected-invalid.l100"
    unrelated.write_text("invalid profile payload\n")
    listings = pd.DataFrame(
        {
            "location": ["Boulder, Colorado", "Boulder, Colorado"],
            "time": pd.to_datetime(["2023-12-27 17:00", "2022-01-01 00:00"]),
            "fn": [valid.name, unrelated.name],
            "url": [str(valid), str(unrelated)],
        }
    )
    monkeypatch.setattr(gml_ozonesonde, "discover_files", lambda location=None: listings)
    result = GMLOzonesondeReader().open_dataset(
        dates=["2023-12-27 00:00", "2023-12-27 23:59"],
        location="Boulder, Colorado",
        as_xarray=as_xarray,
        lazy=True,
    )
    if as_xarray:
        assert result.o3.chunks is not None
        assert result.sizes["time"] == 1
        result.close()
    else:
        import dask.dataframe as dd

        assert isinstance(result, dd.DataFrame)
        assert len(result.compute()) == 1


if __name__ == "__main__":
    pytest.main([__file__])
