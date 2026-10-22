"""Local reader path checks for gridded NetCDF sources."""

import numpy as np
import xarray as xr

from monetio.readers.gpm_imerg import GPMIMERGReader
from monetio.readers.mrms import MRMSReader
from monetio.readers.smap import SMAPReader


def _dataset(variable, y_name, x_name):
    values = np.arange(12, dtype="f4").reshape(3, 4) / 10
    return xr.Dataset(
        {
            variable: ((y_name, x_name), values),
            "unrelated": ((y_name, x_name), np.full((3, 4), -1, dtype="f4")),
        },
        coords={y_name: np.arange(3, dtype="f4"), x_name: np.arange(4, dtype="f4")},
    )


def _assert_reader_dask_parity(reader, dataset, path, variable):
    dataset.to_netcdf(path, engine="h5netcdf")
    eager = reader.open_dataset(str(path), engine="h5netcdf", drop_variables=["unrelated"])
    input_dims = next(iter(dataset.data_vars.values())).dims
    chunks = {input_dims[0]: 1, input_dims[1]: 2}
    lazy = reader.open_dataset(
        str(path),
        engine="h5netcdf",
        use_dask=True,
        chunks=chunks,
        drop_variables=["unrelated"],
    )
    assert "unrelated" not in eager
    assert "unrelated" not in lazy
    assert hasattr(lazy[variable].data, "dask")
    assert lazy[variable].chunks == ((1, 1, 1), (2, 2))
    xr.testing.assert_allclose(eager, lazy.compute())


def test_mrms_local_reader_use_dask_parity(tmp_path):
    _assert_reader_dask_parity(
        MRMSReader(),
        _dataset("precip_rate", "lat", "lon"),
        tmp_path / "mrms.nc",
        "precipitation_rate",
    )


def test_smap_local_reader_use_dask_parity(tmp_path):
    _assert_reader_dask_parity(
        SMAPReader(), _dataset("soil_moisture", "row", "col"), tmp_path / "smap.nc", "soil_moisture"
    )


def test_gpm_imerg_local_reader_use_dask_parity(tmp_path):
    _assert_reader_dask_parity(
        GPMIMERGReader(),
        _dataset("precipitationCal", "lat", "lon"),
        tmp_path / "gpm-imerg.nc",
        "precipitation",
    )
