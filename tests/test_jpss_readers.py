import numpy as np
import pytest
import xarray as xr

from monetio.readers.jpss_atms import JPSSATMSReader
from monetio.readers.jpss_cris import JPSSCrISReader


@pytest.mark.parametrize("reader_cls", [JPSSATMSReader, JPSSCrISReader])
def test_jpss_reader_eager_use_dask_parity(tmp_path, reader_cls):
    path = tmp_path / f"{reader_cls.__name__}.nc"
    source = xr.Dataset(
        {
            "temperature": (("nscan", "nlon"), np.arange(12, dtype="f4").reshape(3, 4)),
            "unrelated": (("nscan", "nlon"), np.full((3, 4), -1, dtype="f4")),
        },
        coords={
            "latitude": (("nscan", "nlon"), np.full((3, 4), 35.0, dtype="f4")),
            "longitude": (("nscan", "nlon"), np.tile(np.arange(4, dtype="f4"), (3, 1))),
        },
    )
    source.to_netcdf(path, engine="h5netcdf")

    reader = reader_cls()
    eager = reader.open_dataset(str(path), engine="h5netcdf", drop_variables=["unrelated"])
    lazy = reader.open_dataset(
        str(path),
        engine="h5netcdf",
        use_dask=True,
        chunks={"nscan": 1, "nlon": 2},
        drop_variables=["unrelated"],
    )

    assert "unrelated" not in eager
    assert "unrelated" not in lazy
    assert eager.temperature.chunks is None
    assert lazy.temperature.chunks is not None
    assert lazy.temperature.chunks == ((1, 1, 1), (2, 2))
    eager.attrs.pop("history", None)
    lazy.attrs.pop("history", None)
    xr.testing.assert_identical(eager, lazy.compute())
    eager.close()
    lazy.close()
