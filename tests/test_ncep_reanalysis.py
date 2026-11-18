import numpy as np
import pytest
import xarray as xr

from monetio.readers.ncep_reanalysis import NCEPReanalysisReader


def _ncep_worker_pid_block(block):
    import os

    return np.full(block.shape, os.getpid(), dtype="int64")


def test_ncep_reanalysis_reader_eager_use_dask_parity(tmp_path):
    path = tmp_path / "air.nc"
    source = xr.Dataset(
        {
            "air": (("time", "level", "lat", "lon"), np.arange(24, dtype="f4").reshape(2, 2, 2, 3)),
            "uwnd": (("time", "level", "lat", "lon"), np.full((2, 2, 2, 3), 99, dtype="f4")),
        },
        coords={
            "time": np.array(["2023-01-01", "2023-01-02"], dtype="datetime64[ns]"),
            "level": [1000, 850],
            "lat": [-10.0, 10.0],
            "lon": [0.0, 180.0, 270.0],
        },
    )
    source.to_netcdf(path, engine="h5netcdf")

    reader = NCEPReanalysisReader()
    eager = reader.open_dataset(files=str(path), engine="h5netcdf", drop_variables=["uwnd"])
    lazy = reader.open_dataset(
        files=str(path),
        engine="h5netcdf",
        use_dask=True,
        chunks={"time": 1, "lat": 1},
        drop_variables=["uwnd"],
    )

    assert "uwnd" not in eager
    assert "uwnd" not in lazy
    assert eager.temperature.chunks is None
    assert lazy.temperature.chunks is not None
    assert lazy.temperature.chunks[0] == (1, 1)
    assert lazy.temperature.chunks[-2] == (1, 1)
    eager.attrs.pop("history", None)
    lazy.attrs.pop("history", None)
    xr.testing.assert_identical(eager, lazy.compute())
    eager.close()
    lazy.close()


@pytest.mark.parametrize("use_dask", [True], ids=["use-dask"])
def test_ncep_reanalysis_runs_on_process_workers(tmp_path, process_distributed_cluster, use_dask):
    import os

    path = tmp_path / "air-worker.nc"
    source = xr.Dataset(
        {"air": (("time", "level", "lat", "lon"), np.arange(24, dtype="f4").reshape(2, 2, 2, 3))},
        coords={
            "time": np.array(["2023-01-01", "2023-01-02"], dtype="datetime64[ns]"),
            "level": [1000, 850],
            "lat": [-10.0, 10.0],
            "lon": [0.0, 180.0, 270.0],
        },
    )
    source.to_netcdf(path, engine="h5netcdf")
    client = process_distributed_cluster
    worker_pids = set(client.run(os.getpid).values())
    reader = NCEPReanalysisReader()
    lazy = reader.open_dataset(str(path), engine="h5netcdf", use_dask=use_dask, chunks={"time": 1})
    eager = reader.open_dataset(str(path), engine="h5netcdf", use_dask=False)
    marked = lazy.temperature.copy(
        data=lazy.temperature.data.map_blocks(_ncep_worker_pid_block, dtype="int64")
    )
    computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
    actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
    assert actual_pids and actual_pids <= worker_pids
    xr.testing.assert_allclose(
        computed.drop_vars("_worker_pid").drop_vars("history", errors="ignore"),
        eager.drop_vars("history", errors="ignore"),
    )
    lazy.close()
    eager.close()
