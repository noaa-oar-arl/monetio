import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.solrad import SOLRADReader


def test_solrad_reader_basic(tmp_path):
    # Create a mock SOLRAD file (non-Madison)
    # WIDTHS = [4, 3, 2, 2, 2, 2, 6, 6] + 5 * [7, 1] + 4 * [9]
    def fmt_solrad(
        y,
        jd,
        m,
        d,
        h,
        mi,
        dt,
        zen,
        ghi,
        ghf,
        dni,
        dnf,
        dhi,
        dhf,
        uvb,
        uvf,
        uvbt,
        uvbtf,
        sghi,
        sdni,
        sdhi,
        suvb,
    ):
        return (
            f"{y:4d} {jd:3d} {m:2d} {d:2d} {h:2d} {mi:2d} {dt:6.3f} {zen:6.2f} "
            f"{ghi:7.1f} {ghf:1d} {dni:7.1f} {dnf:1d} {dhi:7.1f} {dhf:1d} {uvb:7.1f} {uvf:1d} {uvbt:7.1f} {uvbtf:1d} "
            f"{sghi:9.1f} {sdni:9.1f} {sdhi:9.1f} {suvb:9.1f}"
        )

    l1 = fmt_solrad(
        2024,
        1,
        1,
        1,
        0,
        0,
        0.0,
        0.0,
        -9999.9,
        0,
        -9999.9,
        0,
        -9999.9,
        0,
        -9999.9,
        0,
        -9999.9,
        0,
        -9999.9,
        -9999.9,
        -9999.9,
        -9999.9,
    )
    l2 = fmt_solrad(
        2024,
        1,
        1,
        1,
        0,
        1,
        0.017,
        0.0,
        100.0,
        0,
        200.0,
        0,
        300.0,
        0,
        400.0,
        0,
        500.0,
        0,
        10.0,
        20.0,
        30.0,
        40.0,
    )

    mock_content = f"Albuquerque, NM\n35.03796 -106.62211 1617 7\n{l1}\n{l2}\n"

    f = tmp_path / "abq24001.dat"
    f.write_text(mock_content)

    reader = SOLRADReader()

    # Test Eager
    df = reader.open_dataset(files=str(f), as_xarray=False)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df["siteid"].iloc[0] == "Albuquerque, NM"
    assert df["latitude"].iloc[0] == 35.03796
    assert df["time"].iloc[0] == pd.Timestamp("2024-01-01 00:00:00")

    # Check NaN
    assert np.isnan(df["ghi"].iloc[0])
    assert df["ghi"].iloc[1] == 100.0

    # Test Lazy
    ds = reader.open_dataset(files=str(f), as_xarray=True, lazy=True)
    assert isinstance(ds, xr.Dataset)
    assert "ghi" in ds.data_vars
    assert ds.sizes["time"] == 2


def test_solrad_build_urls():
    reader = SOLRADReader()
    dates = pd.to_datetime(["2024-01-01"])
    sites = ["abq"]
    urls = reader.build_urls(dates, sites)

    assert len(urls) == 1
    assert "https://gml.noaa.gov/aftp/data/radiation/solrad/abq/2024/abq24001.dat" in urls
