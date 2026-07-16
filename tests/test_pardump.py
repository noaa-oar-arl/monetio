import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.pardump import PardumpReader


def create_mock_pardump(fname):
    """Create a mock HYSPLIT PARDUMP binary file."""
    tp1 = ">f4"
    tp2 = ">i4"
    tp3 = ">i8"

    # Header
    hdr_dt = np.dtype(
        [
            ("padding", tp2),
            ("parnum", tp2),
            ("pollnum", tp2),
            ("year", tp2),
            ("month", tp2),
            ("day", tp2),
            ("hour", tp2),
            ("minute", tp2),
        ]
    )

    # Particle Data
    pardt = np.dtype(
        [
            ("p1", tp2),
            ("p2", tp2),
            ("pmass", tp1),
            ("p3", tp3),
            ("lat", tp1),
            ("lon", tp1),
            ("ht", tp1),
            ("su", tp1),
            ("sv", tp1),
            ("sx", tp1),
            ("p4", tp3),
            ("age", tp2),
            ("dist", tp2),
            ("poll", tp2),
            ("mgrid", tp2),
            ("sorti", tp2),
        ]
    )

    # Record 1
    parnum1 = 5
    hdr1 = np.array([(4 * 7, parnum1, 1, 2023, 1, 1, 12, 0)], dtype=hdr_dt)

    particles1 = np.zeros(parnum1, dtype=pardt)
    particles1["lat"] = np.linspace(30, 40, parnum1)
    particles1["lon"] = np.linspace(-100, -90, parnum1)
    particles1["sorti"] = np.arange(parnum1)
    particles1["pmass"] = 1.0

    # Record 2
    parnum2 = 3
    hdr2 = np.array([(4 * 7, parnum2, 1, 2023, 1, 1, 13, 0)], dtype=hdr_dt)

    particles2 = np.zeros(parnum2, dtype=pardt)
    particles2["lat"] = np.linspace(31, 41, parnum2)
    particles2["lon"] = np.linspace(-101, -91, parnum2)
    particles2["sorti"] = np.arange(parnum2)
    particles2["pmass"] = 2.0

    with open(fname, "wb") as f:
        # Record 1
        f.write(hdr1.tobytes())
        f.write(particles1.tobytes())
        f.write(np.array([4 * 7], dtype=tp2).tobytes())
        # Record 2
        f.write(hdr2.tobytes())
        f.write(particles2.tobytes())
        f.write(np.array([4 * 7], dtype=tp2).tobytes())


def test_pardump_reader(tmp_path):
    fname = tmp_path / "PARDUMP.bin"
    create_mock_pardump(fname)

    reader = PardumpReader()

    # 1. Eager Path (Pandas)
    df_eager = reader.open_dataset(str(fname), as_xarray=False, lazy=False)
    assert isinstance(df_eager, pd.DataFrame)
    assert len(df_eager) == 8
    assert "time" in df_eager.columns
    assert "latitude" in df_eager.columns
    assert "siteid" in df_eager.columns
    assert df_eager.siteid.nunique() == 5  # sorti 0-4

    # 2. Lazy Path (Dask)
    df_lazy = reader.open_dataset(str(fname), as_xarray=False, lazy=True)
    import dask.dataframe as dd

    assert isinstance(df_lazy, dd.DataFrame)

    # Check consistency
    pd.testing.assert_frame_equal(df_eager, df_lazy.compute(), check_dtype=False)

    # 3. Xarray Path (Eager)
    ds_eager = reader.open_dataset(str(fname), as_xarray=True, lazy=False)
    assert isinstance(ds_eager, xr.Dataset)
    assert "time" in ds_eager.coords
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords
    assert "time" in ds_eager.dims

    # 4. Xarray Path (Lazy)
    ds_lazy = reader.open_dataset(str(fname), as_xarray=True, lazy=True)
    assert isinstance(ds_lazy, xr.Dataset)
    # Check that data is dask-backed
    assert hasattr(ds_lazy.pmass.data, "dask")

    # Verify consistency between Eager and Lazy Xarray
    # Drop history as it might differ slightly in timestamps if recorded
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )


def test_pardump_drange(tmp_path):
    fname = tmp_path / "PARDUMP.bin"
    create_mock_pardump(fname)

    reader = PardumpReader()
    drange = (pd.Timestamp("2023-01-01 12:30:00"), pd.Timestamp("2023-01-01 13:30:00"))

    df = reader.open_dataset(str(fname), drange=drange, as_xarray=False, lazy=False)
    assert len(df) == 3  # Only Record 2
    assert (df.time == pd.Timestamp("2023-01-01 13:00:00")).all()


if __name__ == "__main__":
    pytest.main([__file__])
