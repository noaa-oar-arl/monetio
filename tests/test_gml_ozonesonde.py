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


if __name__ == "__main__":
    pytest.main([__file__])
