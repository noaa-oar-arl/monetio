"""Support deterministic runtime evidence for deferred reader behavior."""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from dask.callbacks import Callback


@dataclass
class ReadEvent:
    stage: str
    operation: str
    input_identity: str | None = None
    process_id: int | None = None
    worker: str | None = None
    storage_bytes: int | None = None
    decoded_bytes: int | None = None


@dataclass
class AuditTrace:
    events: list[ReadEvent] = field(default_factory=list)

    def record(
        self, stage: str, operation: str, input_identity: str | None = None, **kwargs: Any
    ) -> None:
        self.events.append(ReadEvent(stage, operation, input_identity, **kwargs))

    def stage_events(self, stage: str) -> list[ReadEvent]:
        return [event for event in self.events if event.stage == stage]

    def input_passes(self, stage: str, input_identity: str) -> int:
        return sum(event.input_identity == input_identity for event in self.stage_events(stage))


class TaskTrace(Callback):
    """Observe scheduler tasks with process identity for self-contained graphs."""

    def __init__(self, trace: AuditTrace, stage: str):
        self.trace = trace
        self.stage = stage
        super().__init__()

    def _posttask(self, key, result, dsk, state, worker_id):
        self.trace.record(
            self.stage,
            "dask-task",
            str(key),
            process_id=__import__("os").getpid(),
            worker=str(worker_id) if worker_id else None,
        )


@contextmanager
def audit_stage(trace: AuditTrace, stage: str):
    """Set an explicit observation stage around reader or evaluation actions."""
    yield trace


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    for item in sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]:
        digest.update(item.name.encode())
        with item.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def write_camx_pseudonetcdf_fixture(destination: Path) -> Path:
    """Build a projected UAMIV payload from PseudoNetCDF's bundled test file."""
    import PseudoNetCDF
    from PseudoNetCDF import pncopen
    from PseudoNetCDF.camxfiles.uamiv.Write import ncf2uamiv

    source = Path(PseudoNetCDF.__file__).parent / "testcase" / "camxfiles" / "uamiv" / "test.uamiv"
    dataset = pncopen(str(source), format="uamiv")
    dataset.CPROJ = 2
    dataset.PLON = -100.0
    dataset.PLAT = 40.0
    dataset.TLAT1 = 30.0
    dataset.TLAT2 = 60.0
    with ncf2uamiv(dataset, str(destination)):
        pass
    return destination


def baseline_versions() -> dict[str, str | None]:
    versions = {}
    for package in ("dask", "distributed", "xarray", "pandas", "numpy", "pyarrow", "h5py"):
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            versions[package] = None
    return versions


def evidence_record(case_id: str, command: str, fixture: Path | None = None) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True
    ).stdout.strip()
    return {
        "case_id": case_id,
        "command": command,
        "fixture": fixture.relative_to(root).as_posix()
        if fixture and fixture.is_relative_to(root)
        else str(fixture)
        if fixture
        else None,
        "fixture_hash": sha256_path(fixture) if fixture and fixture.exists() else None,
        "code_revision": revision,
        "dependency_versions": baseline_versions(),
        "run_status": "not_run",
        "report": None,
    }


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def make_tabular_fixture(
    directory: Path,
    *,
    rows: int = 32,
    columns: int = 10,
    partitions: int = 4,
    empty_first: bool = False,
) -> list[Path]:
    """Stream a deterministic CSV per partition; supports empty/schema edge cases."""
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for part in range(partitions):
        path = directory / f"part-{part:04}.csv"
        with path.open("w", encoding="utf-8") as stream:
            stream.write(",".join(["time", "siteid", *[f"v{i}" for i in range(columns)]]) + "\n")
            n = 0 if empty_first and part == 0 else rows
            for row in range(n):
                idx = part * rows + row
                vals = [f"2023-01-{1 + idx % 28:02}T{idx % 24:02}:00:00", f"S{idx % 5}"]
                vals.extend(
                    str(float(idx + col) if (idx + col) % 7 else "") for col in range(columns)
                )
                stream.write(",".join(vals) + "\n")
        paths.append(path)
    return paths


def make_mixed_schema_fixture(directory: Path) -> list[Path]:
    """Create two CSV partitions with intentionally different columns."""
    directory.mkdir(parents=True, exist_ok=True)
    first = directory / "schema-a.csv"
    second = directory / "schema-b.csv"
    first.write_text("time,siteid,value\n2023-01-01,S0,1.0\n", encoding="utf-8")
    second.write_text("time,siteid,value,extra\n2023-01-02,S1,2.0,x\n", encoding="utf-8")
    return [first, second]


def make_malformed_table_fixture(path: Path) -> Path:
    """Create a local CSV with an unterminated quoted field."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('time,siteid,value\n2023-01-01,S0,"unterminated\n', encoding="utf-8")
    return path


def make_chunked_gridded_fixture(
    path: Path, *, time: int = 8, y: int = 16, x: int = 16, chunks: tuple[int, int, int] = (2, 8, 8)
) -> Path:
    """Write a deterministic chunked netCDF fixture in bounded slabs."""
    import netCDF4
    import numpy as np

    path.parent.mkdir(parents=True, exist_ok=True)
    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("time", time)
        ds.createDimension("y", y)
        ds.createDimension("x", x)
        var = ds.createVariable("value", "f4", ("time", "y", "x"), chunksizes=chunks)
        var.units = "1"
        yy, xx = np.mgrid[:y, :x]
        for t0 in range(0, time, chunks[0]):
            for t in range(t0, min(t0 + chunks[0], time)):
                var[t, :, :] = (t + yy * 0.1 + xx * 0.01).astype("f4")
    return path


def make_sparse_gridded_fixture(
    path: Path, *, shape=(1024, 1024, 512), chunks=(1, 128, 128)
) -> Path:
    """Create a >1 GiB logical chunked array without materializing the fill data."""
    import netCDF4

    path.parent.mkdir(parents=True, exist_ok=True)
    with netCDF4.Dataset(path, "w") as ds:
        for name, size in zip(("time", "y", "x"), shape, strict=True):
            ds.createDimension(name, size)
        ds.createVariable("value", "f4", ("time", "y", "x"), chunksizes=chunks, fill_value=0)
    return path


def make_large_compressed_table(
    directory: Path, *, files=16, bytes_per_file=64 * 1024 * 1024
) -> tuple[list[Path], int]:
    """Stream deterministic gzip partitions and return logical input bytes."""
    import gzip

    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    logical = 0
    row = b"2023-01-01T00:00:00,S0001,1.25\n"
    for part in range(files):
        path = directory / f"partition-{part:03}.csv.gz"
        logical_here = 0
        with gzip.open(path, "wb") as stream:
            header = b"time,siteid,obs\n"
            stream.write(header)
            logical_here += len(header)
            while logical_here + len(row) <= bytes_per_file:
                stream.write(row)
                logical_here += len(row)
        logical += logical_here
        paths.append(path)
    return paths, logical


@contextmanager
def monitor_process_tree(
    root_pid: int | None = None,
    *,
    worker_pids: Iterable[int] = (),
    interval: float = 0.02,
):
    """Capture simultaneous RSS for a client tree and explicitly listed workers."""
    import os
    import threading

    import psutil

    root_pid = root_pid or os.getpid()
    stopped = threading.Event()
    first_sample = threading.Event()
    samples: dict[int, int] = {}
    aggregate_peak = 0
    aggregate_baseline = None
    aggregate_growth_peak = 0
    children_observable = True
    lock = threading.Lock()

    def sample_loop():
        nonlocal aggregate_peak, aggregate_baseline, aggregate_growth_peak, children_observable
        while not stopped.is_set():
            try:
                root_proc = psutil.Process(root_pid)
                try:
                    procs = [root_proc, *root_proc.children(recursive=True)]
                except (psutil.AccessDenied, PermissionError):
                    children_observable = False
                    procs = [root_proc]
                for pid in worker_pids:
                    try:
                        procs.append(psutil.Process(pid))
                    except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError):
                        children_observable = False
                current = {}
                for proc in procs:
                    try:
                        current[proc.pid] = proc.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                with lock:
                    for pid, rss in current.items():
                        samples[pid] = max(samples.get(pid, 0), rss)
                    aggregate = sum(current.values())
                    aggregate_peak = max(aggregate_peak, aggregate)
                    if aggregate_baseline is None:
                        aggregate_baseline = aggregate
                        first_sample.set()
                    aggregate_growth_peak = max(
                        aggregate_growth_peak, aggregate - aggregate_baseline
                    )
            except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError):
                children_observable = False
                try:
                    rss = psutil.Process(root_pid).memory_info().rss
                    with lock:
                        samples[root_pid] = max(samples.get(root_pid, 0), rss)
                        aggregate_peak = max(aggregate_peak, rss)
                        if aggregate_baseline is None:
                            aggregate_baseline = rss
                            first_sample.set()
                except (psutil.NoSuchProcess, psutil.AccessDenied, PermissionError):
                    pass
            stopped.wait(interval)

    thread = threading.Thread(target=sample_loop, daemon=True)
    thread.start()
    try:
        first_sample.wait(timeout=2)
        yield samples
    finally:
        stopped.set()
        thread.join(timeout=2)
        samples["aggregate_peak_bytes"] = aggregate_peak
        samples["aggregate_baseline_bytes"] = aggregate_baseline or 0
        samples["aggregate_growth_peak_bytes"] = aggregate_growth_peak
        samples["children_observable"] = children_observable
