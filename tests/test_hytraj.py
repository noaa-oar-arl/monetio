import pandas as pd
import pytest
import xarray as xr

from monetio.readers.hytraj import HYTRAJReader, read_hytraj_file


@pytest.fixture
def mock_tdump(tmp_path):
    """Create a mock HYSPLIT tdump file."""
    fn = tmp_path / "tdump.txt"
    content = [
        "   1   1",  # n_met
        " METDATA 200101 00",
        "   1",  # n_start
        " 20 01 01 00 30.0 -100.0 1000.0",
        "   1 PRESSURE",  # n_vars, vars
        "   1   1 20 01 01 00 00  0.0  0.0 30.00 -100.00 1000.0 1013.2",  # traj_num, grid, y,m,d,h,m, fhr, age, lat, lon, alt, vars
        "   1   1 20 01 01 01 00  1.0  1.0 31.00 -101.00 1100.0 1010.0",
    ]
    with open(fn, "w") as f:
        f.write("\n".join(content) + "\n")
    return str(fn)


def test_read_hytraj_file(mock_tdump):
    df = read_hytraj_file(mock_tdump)
    assert len(df) == 2
    assert "time" in df.columns
    assert df.time.iloc[0] == pd.Timestamp("2020-01-01 00:00:00")
    assert df.time.iloc[1] == pd.Timestamp("2020-01-01 01:00:00")
    assert df.latitude.iloc[0] == 30.0
    assert df.pressure.iloc[0] == 1013.2


def test_hytraj_eager_lazy_consistency(mock_tdump):
    reader = HYTRAJReader()

    # Eager
    ds_eager = reader.open_dataset(files=mock_tdump, as_xarray=True, lazy=False)

    # Lazy
    ds_lazy = reader.open_dataset(files=mock_tdump, as_xarray=True, lazy=True)

    # Verify laziness
    assert ds_lazy.pressure.chunks is not None

    # Verify consistency
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())
    assert "Read HYTRAJ data" in ds_eager.attrs["history"]
