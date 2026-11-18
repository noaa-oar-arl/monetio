import numpy as np
import xarray as xr


def test_generated_multifile_fixture(multifile):
    assert len(multifile) == 60
    with xr.open_dataset(multifile[0]) as first, xr.open_dataset(multifile[-1]) as last:
        assert first.sizes == {"time": 1, "y": 8, "x": 6}
        assert first.O3.attrs["units"] == "ppbV"
        assert (last.time.values[0] - first.time.values[0]) / np.timedelta64(1, "h") == 59
