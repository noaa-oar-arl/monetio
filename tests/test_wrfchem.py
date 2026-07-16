import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.wrfchem import WRFChemReader


def create_synthetic_wrfchem_ds(include_alt=True, include_p_pb_t=False):
    """Create a synthetic WRF-Chem dataset."""
    times = pd.date_range("2023-01-01", periods=3, freq="h")
    # Times in WRF format: YYYY-MM-DD_HH:MM:SS
    times_strings = [t.strftime("%Y-%m-%d_%H:%M:%S") for t in times]
    times_bytes = np.array([list(s) for s in times_strings], dtype="|S1")

    data_vars = {
        "Times": (("time", "DateStrLen"), times_bytes),
        "XLAT": (("time", "south_north", "west_east"), np.zeros((3, 5, 5))),
        "XLONG": (("time", "south_north", "west_east"), np.zeros((3, 5, 5))),
        "O3": (
            ("time", "bottom_top", "south_north", "west_east"),
            np.ones((3, 2, 5, 5)),
            {"units": "ppmv"},
        ),
        "NO": (
            ("time", "bottom_top", "south_north", "west_east"),
            np.ones((3, 2, 5, 5)),
            {"units": "ppmv"},
        ),
        "NO2": (
            ("time", "bottom_top", "south_north", "west_east"),
            np.ones((3, 2, 5, 5)),
            {"units": "ppmv"},
        ),
        "P25": (
            ("time", "bottom_top", "south_north", "west_east"),
            np.ones((3, 2, 5, 5)),
            {"units": "ug/kg"},
        ),
        "BC1": (
            ("time", "bottom_top", "south_north", "west_east"),
            np.ones((3, 2, 5, 5)),
            {"units": "ug/kg"},
        ),
    }

    if include_alt:
        data_vars["ALT"] = (
            ("time", "bottom_top", "south_north", "west_east"),
            np.ones((3, 2, 5, 5)),
            {"units": "m3/kg"},
        )

    if include_p_pb_t:
        # Standard values that will result in a known density
        # P_tot = 101325 Pa, T_actual = 288.15 K -> rho approx 1.225 kg/m3
        # In WRF: P is perturbation, PB is base state. PB=100000, P=1325.
        # T is perturbation from 300K. T = 288.15 - 300 = -11.85.
        data_vars["P"] = (
            ("time", "bottom_top", "south_north", "west_east"),
            np.full((3, 2, 5, 5), 1325.0),
            {"units": "Pa"},
        )
        data_vars["PB"] = (
            ("time", "bottom_top", "south_north", "west_east"),
            np.full((3, 2, 5, 5), 100000.0),
            {"units": "Pa"},
        )
        data_vars["T"] = (
            ("time", "bottom_top", "south_north", "west_east"),
            np.full((3, 2, 5, 5), -11.85),
            {"units": "K"},
        )

    ds = xr.Dataset(data_vars)
    # Set dimensions
    ds = ds.set_coords(["XLAT", "XLONG"])
    return ds


@pytest.mark.parametrize("lazy", [False, True])
def test_wrfchem_reader_logic(lazy, tmp_path):
    """Test WRF-Chem reader with Eager (NumPy) and Lazy (Dask) backends."""
    ds_orig = create_synthetic_wrfchem_ds()
    # Avoid colons in filenames for Windows compatibility
    file_path = tmp_path / "wrfout_d01_2023-01-01_00-00-00.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()
    chunks = {"time": 1} if lazy else None

    # Open with reader
    ds = reader.open_dataset(str(file_path), chunks=chunks)

    # 1. Verify backend
    if lazy:
        assert ds.O3.chunks is not None
    else:
        assert ds.O3.chunks is None

    # 2. Verify Time parsing
    assert "time" in ds.coords
    assert ds.time.dtype == "datetime64[ns]"
    assert pd.Timestamp(ds.time.values[0]) == pd.Timestamp("2023-01-01 00:00:00")

    # 3. Verify Unit Conversion (ppmv to ppbV)
    assert ds.O3.attrs["units"] == "ppbV"
    assert (ds.O3 == 1000.0).all().compute()

    # 4. Verify Diagnostic Sum (NOx = NO + NO2)
    assert "NOx" in ds.data_vars
    assert ds.NOx.attrs["units"] == "ppbV"
    # NO=1000, NO2=1000 -> NOx=2000
    assert (ds.NOx == 2000.0).all().compute()

    # 5. Verify Unit Conversion (ug/kg to ug/m3 using ALT)
    # P25=1, ALT=1 -> PM2.5 = 1/1 = 1
    assert ds.P25.attrs["units"] == r"$\mu g m^{-3}$"
    assert (ds.P25 == 1.0).all().compute()

    # 6. Verify Diagnostic Sum (PM25)
    assert "PM25" in ds.data_vars
    # P25=1, BC1=1, others=0 -> PM25=2
    assert (ds.PM25 == 2.0).all().compute()

    # 7. Verify History
    assert "history" in ds.attrs
    assert "Preprocessed WRF-Chem data" in ds.attrs["history"]
    assert "Added lazy diagnostic: NOx" in ds.attrs["history"]
    assert "Added lazy diagnostic: PM25" in ds.attrs["history"]


def test_wrfchem_eager_lazy_consistency(tmp_path):
    """Explicitly verify that Eager and Lazy results are identical."""
    ds_orig = create_synthetic_wrfchem_ds()
    # Avoid colons in filenames for Windows compatibility
    file_path = tmp_path / "wrfout_d01_2023-01-01_00-00-00.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()

    ds_eager = reader.open_dataset(str(file_path), chunks=None).compute()
    ds_lazy = reader.open_dataset(str(file_path), chunks={"time": 1}).compute()

    # Compare values
    xr.testing.assert_allclose(ds_eager, ds_lazy)

    # Compare history (timestamps might differ, so check keys/events)
    assert len(ds_eager.attrs["history"].split("\n")) == len(ds_lazy.attrs["history"].split("\n"))


def test_wrfchem_no_ppb_conversion(tmp_path):
    """Verify that if convert_to_ppb=False, units remain ppmv and diagnostics reflect that."""
    ds_orig = create_synthetic_wrfchem_ds()
    file_path = tmp_path / "wrfout_d01_2023-01-01_00-00-00_no_ppb.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()
    ds = reader.open_dataset(str(file_path), convert_to_ppb=False)

    # 1. Verify units remain ppmv
    assert ds.O3.attrs["units"] == "ppmv"
    assert (ds.O3 == 1.0).all().compute()

    # 2. Verify NOx is in ppmv (inherited from NO/NO2)
    assert "NOx" in ds.data_vars
    assert ds.NOx.attrs["units"] == "ppmv"
    assert (ds.NOx == 2.0).all().compute()


@pytest.mark.parametrize("lazy", [False, True])
def test_wrfchem_density_conversion(lazy, tmp_path):
    """Verify unit conversion using density path (P, PB, T)."""
    ds_orig = create_synthetic_wrfchem_ds(include_alt=False, include_p_pb_t=True)
    file_path = tmp_path / "wrfout_d01_2023-01-01_00-00-00_density.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()
    chunks = {"time": 1} if lazy else None
    ds = reader.open_dataset(str(file_path), chunks=chunks)

    # P_tot = 101325, T_actual = (T + 300) * (P_tot / 100000)**(R/Cp)
    # R = 287.05, Cp = 1004.5
    # T = -11.85 -> T + 300 = 288.15
    # T_actual = 288.15 * (101325 / 100000)**(287.05 / 1004.5)
    p_tot = 101325.0
    r = 287.05
    cp = 1004.5
    t_actual = 288.15 * (p_tot / 100000.0) ** (r / cp)
    expected_rho = p_tot / (r * t_actual)

    # P25 was 1.0 ug/kg. After conversion it should be 1.0 * rho ug/m3.
    assert ds.P25.attrs["units"] == r"$\mu g m^{-3}$"
    np.testing.assert_allclose(ds.P25.values, expected_rho, rtol=1e-5)

    assert "using air density calculated from P, PB, T" in ds.attrs["history"]


@pytest.mark.parametrize("lazy", [False, True])
def test_wrfchem_diagnostic_alias(lazy, tmp_path):
    """Verify that pre-calculated aliases are handled correctly."""
    ds_orig = create_synthetic_wrfchem_ds()
    # Add an alias for PM2.5
    ds_orig["PM2_5_DRY"] = ds_orig["P25"].copy()
    ds_orig["PM2_5_DRY"].attrs["units"] = "ug m-3"

    file_path = tmp_path / "wrfout_d01_2023-01-01_00-00-00_alias.nc"
    ds_orig.to_netcdf(file_path)

    reader = WRFChemReader()
    chunks = {"time": 1} if lazy else None
    ds = reader.open_dataset(str(file_path), chunks=chunks)

    assert "PM25" in ds.data_vars
    # It should have used PM2_5_DRY instead of calculating it
    assert "using alias PM2_5_DRY" in ds.attrs["history"]
    # PM2_5_DRY was 1.0, so PM25 should be 1.0 (not 2.0 from P25+BC1)
    assert (ds.PM25 == 1.0).all()
