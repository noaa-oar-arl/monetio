import numpy as np
import pytest
import xarray as xr

from monetio.readers.tempo import TEMPOReader


def test_tempo_eager_lazy_consistency():
    """
    Verify TEMPO reader produces identical results for Eager (NumPy) and Lazy (Dask).
    """
    # 1. Create a mock TEMPO-like dataset
    nx, ny, nz = 10, 20, 5

    # Coordinates
    lon = np.linspace(-120, -70, nx)
    lat = np.linspace(25, 50, ny)
    lon2d, lat2d = np.meshgrid(lon, lat, indexing="ij")

    # Data variables
    no2 = np.random.rand(nx, ny).astype("f4")
    qf = np.zeros((nx, ny), dtype="i4")
    qf[0, 0] = 10  # This should be masked if threshold < 10

    # Surface pressure with hybrid coefficients in attributes
    sp = np.full((nx, ny), 1013.25, dtype="f4")  # hPa
    eta_a = np.linspace(0, 10000, nz).astype("f4")
    eta_b = np.linspace(1, 0, nz).astype("f4")

    ds_base = xr.Dataset(
        data_vars={
            "vertical_column_troposphere": (("x", "y"), no2),
            "main_data_quality_flag": (("x", "y"), qf),
            "surface_pressure": (("x", "y"), sp, {"units": "hPa", "Eta_A": eta_a, "Eta_B": eta_b}),
        },
        coords={
            "latitude": (("x", "y"), lat2d),
            "longitude": (("x", "y"), lon2d),
        },
    )

    variable_dict = {
        "vertical_column_troposphere": {"scale": 2.0, "quality_flag_max": 5},
        "pressure": {},
    }

    # 2. Test Eager (NumPy)
    # We mock the internal open_dataset of GriddedReader's driver to return our ds_base
    # But since TEMPOReader.open_dataset calls super().open_dataset(files, **kwargs),
    # and XarrayDriver.open returns xr.open_dataset(filename, ...),
    # we can just pass the dataset directly to tempo_preprocess for testing logic.
    from monetio.readers.tempo import tempo_preprocess

    ds_eager = tempo_preprocess(ds_base.copy(deep=True), variable_dict=variable_dict)

    # 3. Test Lazy (Dask)
    ds_lazy_input = ds_base.copy(deep=True).chunk({"x": 5, "y": 5})
    ds_lazy = tempo_preprocess(ds_lazy_input, variable_dict=variable_dict)

    # 4. Assertions
    # Check that lazy result is still dask-backed
    assert ds_lazy["vertical_column_troposphere"].chunks is not None
    assert ds_lazy["pres_pa_mid"].chunks is not None

    # Compute lazy result
    ds_lazy_computed = ds_lazy.compute()

    # Verify values are identical
    xr.testing.assert_allclose(ds_eager, ds_lazy_computed)

    # Specific logic checks
    # Scaling
    assert ds_eager["vertical_column_troposphere"].values[1, 1] == pytest.approx(no2[1, 1] * 2.0)

    # Quality masking (qf[0,0]=10, threshold=5)
    assert np.isnan(ds_eager["vertical_column_troposphere"].values[0, 0])

    # Unit conversion (hPa to Pa)
    assert ds_eager["surface_pressure"].values[0, 0] == pytest.approx(101325.0)
    assert ds_eager["surface_pressure"].attrs["units"] == "Pa"
    assert ds_eager["surface_pressure"].attrs["Eta_A"][1] == pytest.approx(eta_a[1] * 100.0)

    # Pressure calculation
    # p = Eta_A + Eta_B * surface_pressure
    # For k=0: eta_a[0]=0, eta_b[0]=1, sp=101325 => p = 101325
    expected_p0 = eta_a[0] * 100.0 + eta_b[0] * 101325.0
    assert ds_eager["pres_pa_mid"].values[0, 0, 0] == pytest.approx(expected_p0)

    # Check dimensions
    # Current standardize_satellite_coords behavior seems to keep x, y order in this mock
    assert "z" in ds_eager["pres_pa_mid"].dims
    assert ds_eager.sizes["z"] == nz


def test_tempo_reader_integration_mock(monkeypatch):
    """
    Test TEMPOReader.open_dataset with mocked XarrayDriver to verify multi-group merging.
    """

    def mock_open(self, files, **kwargs):
        group = kwargs.get("group")
        if group == "product":
            return xr.Dataset({"vertical_column_troposphere": (("x", "y"), np.ones((5, 5)))})
        if group == "geolocation":
            return xr.Dataset(
                {
                    "latitude": (("x", "y"), np.ones((5, 5))),
                    "longitude": (("x", "y"), np.ones((5, 5))),
                }
            )
        if group == "support_data":
            return xr.Dataset({"surface_pressure": (("x", "y"), np.ones((5, 5)))})
        return xr.Dataset()

    from monetio.readers.drivers import XarrayDriver

    monkeypatch.setattr(XarrayDriver, "open", mock_open)

    reader = TEMPOReader()
    ds = reader.open_dataset("mock_file.nc")

    assert "vertical_column_troposphere" in ds.data_vars
    assert "latitude" in ds.coords
    assert "longitude" in ds.coords
    assert "surface_pressure" in ds.data_vars
    assert "history" in ds.attrs
