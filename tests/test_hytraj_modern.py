import pandas as pd
import pytest
import xarray as xr

from monetio.readers.hytraj import HYTRAJReader


def create_fake_hytraj(filename, n_traj=1):
    """Creates a minimal valid HYSPLIT trajectory file."""
    content = [
        "1",  # n_met
        "METDATA 0 0 0 0 0",
        str(n_traj),  # n_start
    ]
    for i in range(n_traj):
        content.append("24 1 1 0 0 40.0 -70.0 10.0")  # year month day hour minute lat lon alt

    content.append("1 PRESSURE")  # n_vars, varname

    for i in range(n_traj):
        # traj_num, met_grid, year, month, day, hour, minute, fhr, age, lat, lon, alt, pressure
        content.append(f"{i + 1} 1 24 1 1 0 0 0.0 0.0 40.0 -70.0 10.0 1013.25")

    with open(filename, "w") as f:
        f.write("\n".join(content))


@pytest.fixture
def fake_hytraj_files(tmp_path):
    f1 = tmp_path / "tdump_1.txt"
    f2 = tmp_path / "tdump_2.txt"
    create_fake_hytraj(f1, n_traj=1)
    create_fake_hytraj(f2, n_traj=1)
    return [str(f1), str(f2)]


def test_hytraj_eager_lazy_consistency(fake_hytraj_files):
    reader = HYTRAJReader()
    tags = ["run1", "run2"]

    # Eager
    df_eager = reader.open_dataset(
        fake_hytraj_files, taglist=tags, renumber=True, lazy=False, as_xarray=False
    )

    # Lazy
    df_lazy = reader.open_dataset(
        fake_hytraj_files, taglist=tags, renumber=True, lazy=True, as_xarray=False
    )

    # Basic Checks
    assert "pid" in df_eager.columns
    assert list(df_eager["pid"]) == ["run1", "run2"]

    # Renumbering check
    assert list(df_eager["traj_num"]) == ["0_1", "1_1"]

    # History check
    assert "Added tags from taglist" in df_eager.attrs["history"]
    assert "Renumbered trajectories" in df_eager.attrs["history"]

    # Consistency
    pd.testing.assert_frame_equal(
        df_eager.reset_index(drop=True),
        df_lazy.compute().reset_index(drop=True),
        check_dtype=False,
    )


def test_hytraj_xarray_consistency(fake_hytraj_files):
    reader = HYTRAJReader()
    tags = ["run1", "run2"]

    # Eager
    ds_eager = reader.open_dataset(
        fake_hytraj_files, taglist=tags, renumber=True, lazy=False, as_xarray=True
    )

    # Lazy
    ds_lazy = reader.open_dataset(
        fake_hytraj_files, taglist=tags, renumber=True, lazy=True, as_xarray=True
    )

    # Renumbering check in coords/vars
    assert [str(x) for x in ds_eager["traj_num"].to_numpy().ravel()] == ["0_1", "1_1"]
    assert [str(x) for x in ds_eager["pid"].to_numpy().ravel()] == ["run1", "run2"]

    # Replaced time coordinates check
    for c in ["year", "month", "day", "hour", "minute"]:
        assert c in ds_eager.coords

    # History check
    assert "Read HYTRAJ data" in ds_eager.attrs["history"]

    xr.testing.assert_allclose(ds_eager, ds_lazy)
