"""Slow RSS checks for subset access to inputs larger than the memory budget."""

from __future__ import annotations

import pandas as pd
import pytest
from dask_audit_support import (
    make_large_compressed_table,
    make_sparse_gridded_fixture,
    monitor_process_tree,
)

from monetio.readers.drivers import PandasDriver, XarrayDriver


@pytest.mark.slow
def test_gridded_selected_chunk_stays_below_total_rss_budget(tmp_path, process_distributed_cluster):
    import os

    path = make_sparse_gridded_fixture(tmp_path / "large.nc")
    logical_bytes = 1024 * 1024 * 512 * 4
    budget = 512 * 1024 * 1024
    assert logical_bytes >= 2 * budget
    client = process_distributed_cluster
    client.wait_for_workers(2)
    worker_pids = tuple(client.run(os.getpid).values())
    assert len(set(worker_pids)) == 2 and os.getpid() not in worker_pids
    with monitor_process_tree(worker_pids=worker_pids) as memory:
        with XarrayDriver(source="cmaq").open(
            path,
            defer=True,
            chunks={"time": 1, "y": 128, "x": 128},
        ) as ds:
            selected = ds["value"].isel(time=0, y=slice(0, 128), x=slice(0, 128)).compute()
            selected_warm = ds["value"].isel(time=0, y=slice(0, 128), x=slice(0, 128)).compute()
    import xarray as xr

    xr.testing.assert_equal(selected, selected_warm)
    print(
        f"gridded RSS: {memory}; decoded_bytes={logical_bytes}; budget_bytes={budget}; "
        "evaluations=first then repeated (no Dask persistence; OS cache not controlled)"
    )
    assert selected.shape == (128, 128)
    assert memory["aggregate_growth_peak_bytes"] < budget


@pytest.mark.slow
def test_tabular_partition_subset_stays_below_total_rss_budget(
    tmp_path, process_distributed_cluster
):
    paths, logical_bytes = make_large_compressed_table(
        tmp_path / "tables", files=193, bytes_per_file=8 * 1024 * 1024
    )
    # The two worker processes plus the client have a higher idle RSS floor
    # than the single-process baseline, so scale the budget with the input.
    budget = 768 * 1024 * 1024
    assert logical_bytes >= 2 * budget
    meta = pd.DataFrame(
        {
            "time": pd.Series(dtype="str"),
            "siteid": pd.Series(dtype="str"),
            "obs": pd.Series(dtype="float64"),
        }
    )
    client = process_distributed_cluster
    client.wait_for_workers(2)
    worker_pids = tuple(client.run(__import__("os").getpid).values())
    assert len(set(worker_pids)) == 2
    with monitor_process_tree(worker_pids=worker_pids) as memory:
        frame = PandasDriver(source="airnow").open(
            [str(p) for p in paths],
            lazy=True,
            defer=True,
            meta=meta,
            compression="gzip",
        )
        selected = frame.get_partition(0).compute()
        selected_warm = frame.get_partition(0).compute()
    pd.testing.assert_frame_equal(selected, selected_warm)
    print(
        f"tabular RSS: {memory}; decoded_bytes={logical_bytes}; budget_bytes={budget}; "
        "workers=2; spill=disabled; evaluations=first then repeated "
        "(no Dask persistence; OS cache not controlled)"
    )
    assert len(selected) > 0
    assert memory["aggregate_growth_peak_bytes"] < budget
