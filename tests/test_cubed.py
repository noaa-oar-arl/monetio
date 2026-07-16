import numpy as np
import pytest
import xarray as xr

from monetio.readers.drivers import XarrayDriver


def test_cubed_backend(tmp_path):
    # 1. Create a mock NetCDF file
    fn = tmp_path / "test_cubed.nc"
    data = np.random.rand(4, 4)
    ds_orig = xr.Dataset({"foo": (("x", "y"), data)})
    ds_orig.to_netcdf(fn)

    # 2. Open using XarrayDriver with use_cubed=True
    driver = XarrayDriver()

    # We need to make sure cubed and cubed-xarray are installed for this test
    pytest.importorskip("cubed")
    pytest.importorskip("cubed_xarray")

    # Open with cubed
    ds = driver.open(str(fn), use_cubed=True, chunks={"x": 2, "y": 2})

    # 3. Verify it is a cubed-backed dataset
    assert "foo" in ds.data_vars

    # Check if the data is a cubed array.
    # Cubed-xarray wraps the cubed array in an xarray-compatible object.
    # We check the underlying data type.
    assert hasattr(ds.foo.data, "__array_namespace__")

    import cubed.array_api.array_object

    assert isinstance(ds.foo.data, cubed.array_api.array_object.Array)

    # 4. Verify values
    np.testing.assert_allclose(ds.foo.values, data)


def test_cubed_error_if_not_installed(monkeypatch):
    # Mock ImportError for cubed
    import builtins

    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name in ["cubed", "cubed_xarray"]:
            raise ImportError(f"No module named '{name}'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    driver = XarrayDriver()
    with pytest.raises(
        ImportError, match="The 'cubed' backend requires 'cubed' and 'cubed-xarray'"
    ):
        driver.open("dummy.nc", use_cubed=True)
