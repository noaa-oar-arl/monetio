import numpy as np
import xarray as xr

from monetio.readers.amdar import AMDARReader


def test_amdar_modern_consistency(tmp_path):
    # 1. Create a dummy AMDAR-like NetCDF file
    fn = tmp_path / "test_amdar.nc"

    # AMDAR structure: 1D dimension 'recNum'
    recNum = 5
    ds = xr.Dataset(
        {
            "observationTime": (("recNum",), [1672531200 + i * 3600 for i in range(recNum)]),
            "latitude": (("recNum",), np.linspace(30, 40, recNum)),
            "longitude": (("recNum",), np.linspace(-100, -90, recNum)),
            "tailNumber": (("recNum",), [f"AC{i}" for i in range(recNum)]),
            "temperature": (("recNum",), np.random.rand(recNum) + 290),
            "altitude": (("recNum",), np.linspace(0, 10000, recNum)),
        }
    )
    ds.observationTime.attrs["units"] = "seconds since 1970-01-01 00:00:00.0 +0000"
    ds.to_netcdf(fn)

    reader = AMDARReader()

    # 2. Eager Path (NumPy)
    ds_eager = reader.open_dataset(str(fn), use_dask=False, as_xarray=True)
    assert isinstance(ds_eager.temperature.data, np.ndarray)

    # 3. Lazy Path (Dask)
    ds_lazy = reader.open_dataset(str(fn), use_dask=True, as_xarray=True)
    assert hasattr(ds_lazy.temperature.data, "dask")

    # 4. Assert Identity
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())

    # Check that time was converted correctly
    assert ds_eager.time.dtype == "datetime64[ns]"
