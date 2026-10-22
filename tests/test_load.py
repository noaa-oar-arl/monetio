import numpy as np
import xarray as xr

import monetio
from monetio.readers.wrfchem import wrfchem_preprocess


def test_load_wrfchem():
    nx, ny, nz, nt = 4, 5, 3, 2
    ds = xr.Dataset(
        {
            "OZONE": (("time", "z", "y", "x"), np.random.rand(nt, nz, ny, nx).astype(np.float32)),
            "Times": (
                ("time", "DateStrLen"),
                np.array([list("2023-01-01_00:00:00"), list("2023-01-01_01:00:00")], dtype="|S1"),
            ),
        },
        coords={"time": np.arange(nt), "y": np.arange(ny), "x": np.arange(nx), "z": np.arange(nz)},
    )

    import unittest.mock as mock

    # Open dataset calls driver.open. Driver.open calls preprocess.
    # So we should mock driver.open to return the preprocessed ds if we want to check results.
    ds_pre = wrfchem_preprocess(ds)
    with mock.patch("monetio.readers.drivers.XarrayDriver.open", return_value=ds_pre):
        res = monetio.load("wrfchem", files="dummy.nc")

    assert res.time.dtype == "datetime64[ns]"


def test_load_grib2():
    ds = xr.Dataset(
        {"tmp": (("lat_0", "lon_0"), np.random.rand(10, 20))},
        coords={"lat_0": np.arange(10), "lon_0": np.arange(20)},
    )

    import unittest.mock as mock

    # Grib2Reader.open_dataset calls self.driver.open then harmonize.
    # driver.open returns ds. harmonize returns harmonized ds.
    with mock.patch("monetio.readers.drivers.XarrayDriver.open", return_value=ds):
        res = monetio.load("grib2", files="dummy.grib2")

    assert "latitude" in res.coords
