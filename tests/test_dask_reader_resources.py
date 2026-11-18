"""Separate process and retained HDF5 resource behavior."""

from __future__ import annotations

import os
from pathlib import Path

import h5py
import numpy as np
import pytest

from monetio.readers.geoms import open_dataset_geoms


def _read_file(path: str) -> tuple[int, str]:
    return os.getpid(), Path(path).read_text()


def _read_worker_table(path: str):
    import pandas as pd

    if Path(path).name == "broken.csv":
        raise ValueError("worker fixture exploded")
    return pd.DataFrame({"value": [1.0, 2.0]})


def _read_retry_once(path: str) -> str:
    marker = Path(path).with_suffix(".attempted")
    try:
        marker.touch(exist_ok=False)
    except FileExistsError:
        return Path(path).read_text()
    raise ValueError("retry this task once")


def test_workers_execute_in_separate_processes(process_distributed_cluster, tmp_path):
    client = process_distributed_cluster
    path = tmp_path / "payload.txt"
    path.write_text("monetio")
    client.wait_for_workers(2)
    worker_pids = set(client.run(os.getpid).values())
    assert len(worker_pids) == 2
    assert os.getpid() not in worker_pids
    result = client.submit(_read_file, str(path), pure=False).result()
    assert result[0] in worker_pids
    assert result[1] == "monetio"


def test_process_worker_error_keeps_input_identity_and_success_is_reusable(
    process_distributed_cluster, tmp_path
):
    import pandas as pd

    from monetio.readers.drivers import PandasDriver

    client = process_distributed_cluster
    valid = tmp_path / "valid.csv"
    broken = tmp_path / "broken.csv"
    valid.write_text("value\n1\n")
    broken.write_text("value\n2\n")
    meta = pd.DataFrame({"value": pd.Series(dtype="float64")})
    frame = PandasDriver(source="airnow").open(
        [str(valid)], read_method=_read_worker_table, lazy=True, meta=meta
    )

    first = client.compute(frame).result()
    second = client.compute(frame).result()
    pd.testing.assert_frame_equal(first, second)
    assert first.value.tolist() == [1.0, 2.0]

    retry_path = tmp_path / "retry.txt"
    retry_path.write_text("retried")
    assert (
        client.submit(_read_retry_once, str(retry_path), retries=1, pure=False).result()
        == "retried"
    )

    broken_frame = PandasDriver(source="airnow").open(
        [str(broken)], read_method=_read_worker_table, lazy=True, meta=meta
    )
    with pytest.raises(ValueError, match=r"worker fixture exploded.*broken\.csv"):
        client.compute(broken_frame).result()


def test_geoms_dataset_close_releases_hdf5_handle(tmp_path, track_h5py_opens):
    path = tmp_path / "small.h5"
    with h5py.File(path, "w") as f:
        f.create_dataset("value", data=np.arange(12).reshape(3, 4))
    opened = track_h5py_opens()
    ds = open_dataset_geoms(str(path), rename_all=False, squeeze=False)
    assert opened and not opened[0].id.valid
    ds.close()
    assert not opened[0].id.valid


def test_tolnet_closes_metadata_group_handle_after_copying_attrs(tmp_path, track_h5py_opens):
    from monetio.readers.tolnet import read_tolnet

    path = tmp_path / "tolnet-groups.h5"
    with h5py.File(path, "w") as handle:
        attrs = handle.create_group("INSTRUMENT_ATTRIBUTES")
        attrs.attrs["site"] = "test"
        data = handle.create_group("DATA")
        data.create_dataset("ALT", data=np.array([0.0, 1.0]))
        data.create_dataset("TIME_MID_UT_UNIX", data=np.array([1_600_000_000_000]))
        data.create_dataset("O3MR", data=np.array([[0.1], [0.2]]))

    opened = track_h5py_opens()
    ds = read_tolnet(str(path), chunks={})
    try:
        assert len(opened) >= 2
        assert opened[0].id.valid  # DATA remains available to lazy array tasks.
        assert not opened[1].id.valid  # metadata-only group is already released.
        assert ds.attrs["site"] == "test"
    finally:
        ds.close()
    assert not opened[0].id.valid


@pytest.mark.parametrize("reader_name", ["amdar", "madis"])
def test_direct_point_netcdf_readers_close_all_source_handles(
    tmp_path, track_xr_closes, reader_name
):
    import xarray as xr

    from monetio.readers.amdar import AMDARReader
    from monetio.readers.madis import MADISReader

    reader_cls = {"amdar": AMDARReader, "madis": MADISReader}[reader_name]
    paths = []
    for i in range(2):
        path = tmp_path / f"{reader_name}-{i}.nc"
        xr.Dataset(
            {"temperature": (("recNum",), np.array([270.0 + i, 271.0 + i], dtype="f4"))},
            coords={"recNum": [0, 1]},
        ).to_netcdf(path, engine="h5netcdf")
        paths.append(str(path))

    handles = track_xr_closes("open_dataset")
    opened = handles.opened
    closed = handles.closed
    ds = reader_cls().open_dataset(paths, use_dask=True, expand2d=False)
    assert len(opened) == len(paths)
    ds.close()
    assert len(closed) == len(opened)

    frame = reader_cls().open_dataset(paths, use_dask=True, as_xarray=False)
    assert len(opened) == 2 * len(paths)
    assert len(closed) == len(opened)
    assert frame.compute().temperature.tolist() == [270.0, 271.0, 271.0, 272.0]


@pytest.mark.parametrize("reader_name", ["amdar", "madis"])
def test_direct_point_netcdf_readers_close_partial_opens_on_failure(
    tmp_path, track_xr_closes, reader_name
):
    import xarray as xr

    from monetio.readers.amdar import AMDARReader
    from monetio.readers.madis import MADISReader

    reader_cls = {"amdar": AMDARReader, "madis": MADISReader}[reader_name]
    path = tmp_path / f"{reader_name}.nc"
    xr.Dataset(
        {"temperature": (("recNum",), np.array([270.0, 271.0], dtype="f4"))},
        coords={"recNum": [0, 1]},
    ).to_netcdf(path, engine="h5netcdf")

    closed = track_xr_closes("open_dataset").closed
    missing = tmp_path / "missing.nc"
    with pytest.raises(FileNotFoundError):
        reader_cls().open_dataset([str(path), str(missing)], use_dask=True, expand2d=False)
    assert len(closed) == 1


@pytest.mark.parametrize("reader_name", ["iagos", "tccon"])
def test_direct_mfdataset_table_readers_close_source_after_graph_build(
    tmp_path, track_xr_closes, reader_name
):
    import dask.array as da
    import xarray as xr

    from monetio.readers.iagos import IAGOSReader
    from monetio.readers.tccon import TCCONReader

    reader_cls = {"iagos": IAGOSReader, "tccon": TCCONReader}[reader_name]
    path = tmp_path / f"{reader_name}.nc"
    xr.Dataset(
        {"v": (("time",), np.arange(4, dtype="f4"))}, coords={"time": np.arange(4)}
    ).to_netcdf(path, engine="h5netcdf")

    handles = track_xr_closes("open_mfdataset")
    opened = handles.opened
    closed = handles.closed
    frame = reader_cls().open_dataset(str(path), as_xarray=False, use_dask=True)
    assert len(opened) == 1
    assert closed == opened
    assert frame.compute().v.tolist() == [0.0, 1.0, 2.0, 3.0]

    ds = reader_cls().open_dataset(str(path), expand2d=False, use_dask=True)
    assert len(opened) == 2
    assert len(closed) == 1
    assert isinstance(ds["v"].data, da.Array)
    ds.close()
    assert len(closed) == 2


def test_actris_direct_netcdf_paths_close_or_transfer_source(tmp_path, track_xr_closes):
    import dask.array as da
    import xarray as xr

    from monetio.readers.actris import ACTRISReader

    path = tmp_path / "actris-netcdf.nc"
    xr.Dataset(
        {"v": (("time",), np.arange(4, dtype="f4"))}, coords={"time": np.arange(4)}
    ).to_netcdf(path, engine="h5netcdf")
    handles = track_xr_closes("open_mfdataset")
    opened = handles.opened
    closed = handles.closed
    reader = ACTRISReader()
    frame = reader.open_dataset(str(path), as_xarray=False, lazy=True)
    assert len(opened) == 1
    assert closed == opened
    assert frame.compute().v.tolist() == [0.0, 1.0, 2.0, 3.0]

    ds = reader.open_dataset(str(path), lazy=True)
    assert len(opened) == 2
    assert len(closed) == 1
    assert isinstance(ds["v"].data, da.Array)
    ds.close()
    assert len(closed) == 2

    eager = reader.open_dataset(str(path), lazy=False)
    assert len(opened) == len(closed) == 3
    assert eager["v"].values.tolist() == [0.0, 1.0, 2.0, 3.0]


@pytest.mark.parametrize("reader_name", ["pandora", "ndacc"])
def test_geoms_dask_dataframe_closes_source_and_task_handles(track_h5py_opens, reader_name):
    from monetio.readers.ndacc import NDACCReader
    from monetio.readers.pandora import PandoraReader

    path = Path(__file__).parent / "data" / "pandora-uvvis-no2-boulder-20231206.h5"
    opened = track_h5py_opens()
    reader = {"pandora": PandoraReader, "ndacc": NDACCReader}[reader_name]()
    frame = reader.open_dataset(str(path), as_xarray=False, use_dask=True)
    assert opened and not opened[0].id.valid
    assert not frame.head(1).empty
    assert all(not handle.id.valid for handle in opened)
