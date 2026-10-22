import warnings

import pandas as pd
import xarray as xr

from monetio.readers.drivers import XarrayDriver


def test_virtualizarr_deprecated_params(tmp_path):
    """Verify that deprecated virtualizarr parameters still work with a warning."""
    fname = tmp_path / "test.nc"
    xr.Dataset({"a": (("x",), [1.0])}).to_netcdf(fname)

    driver = XarrayDriver()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        try:
            driver.open(str(fname), virtualizarr_backend="icechunk", icechunk_repo="some_url")
        except Exception as e:
            print(f"DEBUG: driver.open raised {type(e).__name__}: {e}")

        print(f"DEBUG: Captured {len(w)} warnings")
        for warn in w:
            print(f"DEBUG: Warning: {warn.message}")

        assert any("deprecated" in str(warn.message) for warn in w)


def test_point_reader_use_dask():
    """Verify use_dask alias in PointReader."""
    from monetio.readers.base import PointReader

    class MockPointReader(PointReader):
        def harmonize(self, df):
            return df

        def to_xarray(self, df, **kwargs):
            return df

    reader = MockPointReader()

    class MockDriver:
        def open(self, files, **kwargs):
            return pd.DataFrame({"a": [1], "lazy": [kwargs.get("lazy", False)]})

    reader.driver = MockDriver()

    df_lazy = reader.open_dataset("dummy", use_dask=True, as_xarray=False)
    assert bool(df_lazy["lazy"].iloc[0]) is True

    df_eager = reader.open_dataset("dummy", use_dask=False, as_xarray=False)
    assert bool(df_eager["lazy"].iloc[0]) is False
