from monetio import geoms


def test_open():
    ds = geoms.open_dataset("./data/tolnet-hdf4-test-data.hdf")
    assert "O3.MIXING.RATIO.VOLUME_DERIVED" in ds.variables
