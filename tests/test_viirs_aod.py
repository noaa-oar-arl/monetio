import pytest

from monetio.sat.nesdis_viirs_aod_aws_gridded import open_dataset


@pytest.mark.parametrize("sat", ["SNPP", "NOAA20"])
@pytest.mark.parametrize("res", [0.05, 0.1, 0.25])
def test_open_dataset(sat, res):
    date = "2020-01-01"  # a date when we have both
    s_res = f"{res:.3f}"

    ds = open_dataset(date, sat, s_res)
    assert set(ds.dims) == {"time", "lat", "lon"}
    assert ds.sizes["time"] == 1
    assert ds.sizes["lat"] == int(180 / res)
    assert ds.sizes["lon"] == int(360 / res)
    assert ds.attrs["satellite_name"] == "NPP" if sat == "NPP" else "NOAA 20"
    assert ds.attrs["spatial_resolution"].strip().startswith(str(res))
