from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr

from monetio.readers.aeronet import AERONETReader, add_angstrom_exponent, add_aod_at_wavelength


def create_mock_aeronet_ds(lazy=False, seed=42):
    """Create a mock AERONET-like dataset."""
    rng = np.random.default_rng(seed)
    times = pd.date_range("2021-01-01", periods=5, freq="h")
    n_nodes = 3

    data = {
        "aod_440nm": (("time", "node"), rng.random((5, n_nodes)) + 0.1),
        "aod_500nm": (("time", "node"), rng.random((5, n_nodes)) + 0.1),
        "aod_870nm": (("time", "node"), rng.random((5, n_nodes)) + 0.1),
        "siteid": (("node",), [f"Site{i}" for i in range(n_nodes)]),
        "latitude": (("node",), [10.0, 20.0, 30.0]),
        "longitude": (("node",), [-10.0, -20.0, -30.0]),
    }

    ds = xr.Dataset(data, coords={"time": times, "node": np.arange(n_nodes)})

    if lazy:
        ds = ds.chunk({"time": 2, "node": 2})

    return ds


def test_add_angstrom_exponent():
    """Test AE calculation for both eager and lazy backends."""
    ds_eager = create_mock_aeronet_ds(lazy=False, seed=42)
    ds_lazy = create_mock_aeronet_ds(lazy=True, seed=42)

    res_eager = add_angstrom_exponent(ds_eager, wv1=440.0, wv2=870.0)
    res_lazy = add_angstrom_exponent(ds_lazy, wv1=440.0, wv2=870.0)

    ae_name = "440-870_angstrom_exponent"
    assert ae_name in res_eager.variables
    assert ae_name in res_lazy.variables
    assert hasattr(res_lazy[ae_name].data, "dask")

    xr.testing.assert_allclose(res_eager[ae_name], res_lazy[ae_name].compute())
    assert "Calculated Angstrom Exponent" in res_eager.attrs["history"]


def test_add_aod_at_wavelength():
    """Test AOD interpolation for both eager and lazy backends."""
    ds_eager = create_mock_aeronet_ds(lazy=False, seed=123)
    # Add AE first
    ds_eager = add_angstrom_exponent(ds_eager)

    ds_lazy = ds_eager.chunk({"time": 2})

    res_eager = add_aod_at_wavelength(ds_eager, target_wv=550.0, base_wv=500.0)
    res_lazy = add_aod_at_wavelength(ds_lazy, target_wv=550.0, base_wv=500.0)

    aod_name = "aod_550nm"
    assert aod_name in res_eager.variables
    assert aod_name in res_lazy.variables
    assert hasattr(res_lazy[aod_name].data, "dask")

    xr.testing.assert_allclose(res_eager[aod_name], res_lazy[aod_name].compute())
    assert "Estimated AOD at 550.0nm" in res_eager.attrs["history"]


@patch("monetio.readers.aeronet.AERONETReader.to_xarray")
@patch("monetio.readers.aeronet.PointReader.open_dataset")
def test_aeronet_reader_diagnostics(mock_open, mock_to_xarray):
    """Test that open_dataset correctly calls diagnostics."""
    # Setup mock returns
    mock_df = pd.DataFrame({"time": [pd.Timestamp("2021-01-01")], "siteid": ["Site1"]})
    mock_open.return_value = mock_df

    ds_mock = create_mock_aeronet_ds()
    mock_to_xarray.return_value = ds_mock

    reader = AERONETReader()

    # Call with add_diagnostics=True
    # Need to pass as_xarray=True which is default but good to be explicit
    res = reader.open_dataset(files="mock.txt", add_diagnostics=True, as_xarray=True)

    assert "aod_550nm" in res.variables
    assert "440-870_angstrom_exponent" in res.variables
    assert "Estimated AOD at 550.0nm" in res.attrs["history"]
