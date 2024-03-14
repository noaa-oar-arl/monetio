from monetio.sat.nesdis_viirs_aod_aws_gridded import open_dataset


def test_open_dataset():
    date = "2020-01-01"  # a date when we have both
    ds = open_dataset(date, "SNPP")
    assert tuple(ds.dims) == ("time", "lat", "lon")
    assert ds.sizes["time"] == 1
    assert ds.attrs["satellite_name"] == "NPP"
    assert ds.attrs["spatial_resolution"].strip().startswith("0.1")
