import sys

import pytest

from monetio.sat.nesdis_viirs_aod_aws_gridded import open_dataset

if sys.version_info < (3, 7):
    pytest.skip("s3fs requires Python 3.7+", allow_module_level=True)


@pytest.mark.parametrize("sat", ["SNPP", "NOAA20"])
@pytest.mark.parametrize("res", [0.05, 0.1, 0.25])
def test_open_dataset(sat, res):
    date = "2020-01-01"  # a date when we have both SNPP and NOAA-20 data available
    s_res = f"{res:.3f}"

    ds = open_dataset(date, sat, s_res)
    assert set(ds.dims) == {"time", "lat", "lon"}
    assert ds.sizes["time"] == 1
    assert ds.sizes["lat"] == int(180 / res)
    assert ds.sizes["lon"] == int(360 / res)
    assert ds.attrs["satellite_name"] == ("NPP" if sat == "SNPP" else "NOAA 20")
    assert ds.attrs["spatial_resolution"].strip().startswith(str(res))


def test_open_dataset_bad_input():
    with pytest.raises(ValueError, match="Invalid input"):
        open_dataset("2020-01-01", satellite="GOES-16")

    with pytest.raises(ValueError, match="Invalid input"):
        open_dataset("2020-01-01", data_resolution=100)

    with pytest.raises(ValueError, match="Invalid input"):
        open_dataset("2020-01-01", averaging_time="asdf")


def test_open_dataset_no_data():
    with (
        pytest.raises(ValueError, match="Invalid date"),
        # pytest.warns(UserWarning, match="not available"),
    ):
        open_dataset("1900-01-01")
