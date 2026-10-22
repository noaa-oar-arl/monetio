"""Deferred-loading scale tests (tasks.md T022; US2, SC-011/SC-014, FR-025/FR-035).

Proves the ``defer=True`` contract end-to-end on fixture-backed sources:

* the result is an *unevaluated* object (dask-backed Dataset / dask DataFrame);
* the data-model §6 crux invariant holds —
  ``evaluate(load(s, defer=True, **A)) == load(s, **A)``;
* **SC-011**: ``defer=True`` triggers **zero full-dataset computes** (the
  compute-boundary instrumentation shows only the bounded metadata computes
  permitted by FR-025, never a whole-column materialization);
* **SC-014**: a slice evaluation touches only the files that slice needs
  (counted at the backend read boundary);
* a source without the capability raises, naming the source and reason
  (FR-035) — it never silently materializes.

The instrumentation patches the real compute boundary (``dask.base.compute``
and ``dask.compute``) and classifies each compute by how many *data elements*
it materializes: a deferred load may compute a handful of scalar/coordinate
values (bounded, FR-025) but must never compute a full data column/array.
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path

import dask
import dask.array as da
import dask.base
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio._manifest import MANIFEST_BY_NAME, ScaleCapabilities
from monetio.readers.airnow import AirNowReader
from monetio.readers.cmaq import CMAQReader

_ROOT = Path(__file__).parent
_MULTIFILE = sorted(str(p) for p in (_ROOT / "data" / "multifile").glob("*.nc"))


# ---------------------------------------------------------------------------
# Compute-boundary instrumentation
# ---------------------------------------------------------------------------
class ComputeProbe:
    """Record every ``dask.compute`` call and the data it materializes.

    A *full* compute is one that materializes more than ``threshold`` array
    elements (a whole column / grid). Bounded metadata computes (``.size``,
    ``len`` of a partition, coordinate scalars) stay far below the threshold.
    """

    def __init__(self, threshold: int = 64):
        self.threshold = threshold
        self.calls = 0
        self.full = 0
        self.max_elements = 0
        self._orig_base = None
        self._orig_mod = None

    def __enter__(self):
        probe = self

        def counting(*args, **kwargs):
            out = probe._orig_base(*args, **kwargs)
            outs = out if isinstance(out, tuple) else (out,)
            for o in outs:
                probe.calls += 1
                n = _element_count(o)
                probe.max_elements = max(probe.max_elements, n)
                if n > probe.threshold:
                    probe.full += 1
            return out

        self._orig_base = dask.base.compute
        self._orig_mod = dask.compute
        dask.base.compute = counting
        dask.compute = counting
        return self

    def __exit__(self, *exc):
        dask.base.compute = self._orig_base
        dask.compute = self._orig_mod
        return False


def _element_count(obj) -> int:
    if isinstance(obj, pd.DataFrame):
        return int(obj.size)
    if isinstance(obj, pd.Series):
        return int(obj.size)
    if isinstance(obj, np.ndarray):
        return int(obj.size)
    return 0


_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?::\d{2})?")


def _dask_data_vars(ds: xr.Dataset):
    """The data-bearing variables (drop 0-d CF markers like ``mesh``).

    A deferred load must make every *dimensioned* data variable a dask array;
    scalar CF bookkeeping (the UGRID ``mesh`` topology marker is a 0-d int)
    is legitimately eager.
    """
    return {k: v for k, v in ds.data_vars.items() if v.ndim > 0}


def _all_dask(ds: xr.Dataset) -> bool:
    return all(isinstance(v.data, da.Array) for v in _dask_data_vars(ds).values())


def _normalize_history(ds: xr.Dataset) -> xr.Dataset:
    """Blank out wall-clock timestamps in ``history`` attrs (global + per-var).

    The CMAQ reader stamps a generation time into ``history`` on every call, so
    two loads a second apart differ only there; the crux invariant is about
    data + structure, not the recording second.
    """
    out = ds.copy()
    for k, v in list(out.attrs.items()):
        if isinstance(v, str):
            out.attrs[k] = _TS_RE.sub("<TS>", v)
    for var in out.variables:
        for k, v in list(out[var].attrs.items()):
            if isinstance(v, str):
                out[var].attrs[k] = _TS_RE.sub("<TS>", v)
    return out


def _drop_history(ds: xr.Dataset) -> xr.Dataset:
    """Remove the ``history`` provenance log entirely (global + per-var).

    ``history`` is the one attribute that legitimately records *which code path*
    produced the dataset: the eager (pandas) path keeps per-stage provenance
    lines that the dask path has always dropped (dask DataFrames carry no
    ``.attrs``). That divergence predates this feature — ``defer`` matches the
    existing ``lazy`` path exactly — and fixing it would change the default
    (``defer=False``) output, violating the additive-only guarantee (FR-034).
    The crux invariant is therefore asserted on **data + all other attrs**,
    with ``history`` compared separately (defer == lazy, byte-for-byte).
    """
    out = ds.copy()
    out.attrs.pop("history", None)
    for var in out.variables:
        out[var].attrs.pop("history", None)
    return out


# ---------------------------------------------------------------------------
# Synthetic airnow fixture helper (mirrors tests/test_aero_airnow.py)
# ---------------------------------------------------------------------------
def _synthetic_airnow(tmp_path, monkeypatch, n_rows: int, n_sites: int = 1):
    import monetio.readers.epa_utils as epa_utils

    def fake_read_monitor_file(*a, **k):
        return pd.DataFrame(
            {
                "siteid": [f"0{i:08d}" for i in range(n_sites)],
                "latitude": [40.0 + i for i in range(n_sites)],
                "longitude": [-80.0 - i for i in range(n_sites)],
                "site_name": [f"SITE {i}" for i in range(n_sites)],
            }
        )

    monkeypatch.setattr(epa_utils, "read_monitor_file", fake_read_monitor_file)
    # Real AirNow rows are unique on (time, siteid, variable): the wide pivot
    # is well-defined and both the eager ``pivot_table`` and the deferred
    # ``ds_to_2d`` unstack agree. A fixture that repeats one key would make
    # eager (mean-aggregate) and deferred (drop-duplicate) legitimately differ.
    lines = []
    for i in range(n_rows):
        site = f"0{i % n_sites:08d}"
        hour = (i // n_sites) % 24
        day = 1 + (i // n_sites) // 24
        lines.append(f"01/{day:02d}/23|{hour:02d}:00|{site}|T|-5|OZONE|PPB|{50.0 + i}|X")
    fn = tmp_path / "HourlyData_2023010100.dat"
    fn.write_text("\n".join(lines), encoding="ISO-8859-1")
    return str(fn)


# ---------------------------------------------------------------------------
# Gridded deferred loading
# ---------------------------------------------------------------------------
class TestGriddedDeferred:
    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    def test_defer_returns_unevaluated_dataset(self):
        reader = CMAQReader()
        ds = reader.open_dataset(_MULTIFILE, defer=True)
        assert isinstance(ds, xr.Dataset)
        assert all(isinstance(v.data, da.Array) for v in ds.data_vars.values()), (
            "defer=True must return a dask-backed (unevaluated) Dataset"
        )

    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    def test_crux_invariant(self):
        """evaluate(load(defer=True)) == load() — the §6 contract."""
        reader = CMAQReader()
        eager = reader.open_dataset(_MULTIFILE)
        lazy = reader.open_dataset(_MULTIFILE, defer=True)
        xr.testing.assert_identical(_normalize_history(lazy.compute()), _normalize_history(eager))

    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    def test_sc011_zero_full_computes(self):
        """SC-011: a deferred gridded load triggers zero full-dataset computes."""
        reader = CMAQReader()
        with ComputeProbe() as probe:
            reader.open_dataset(_MULTIFILE, defer=True)
        assert probe.full == 0, (
            f"deferred gridded load materialized {probe.full} full computes "
            f"(max {probe.max_elements} elements); SC-011 requires zero"
        )

    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    def test_defer_overrides_use_dask_false(self):
        """defer=True forces the dask path even when use_dask=False is implied."""
        reader = CMAQReader()
        # The default (no defer) with use_dask not set still returns a Dataset;
        # defer=True must yield a dask-backed one regardless.
        lazy = reader.open_dataset(_MULTIFILE, defer=True, use_dask=False)
        assert _all_dask(lazy)


# ---------------------------------------------------------------------------
# Point deferred loading
# ---------------------------------------------------------------------------
class TestPointDeferred:
    def test_defer_dataframe_returns_unevaluated(self, tmp_path, monkeypatch):
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=300, n_sites=5)
        reader = AirNowReader()
        df = reader.open_dataset(
            files=fn, defer=True, wide_fmt=False, as_xarray=False, bad_utcoffset="leave"
        )
        assert isinstance(df, dask.dataframe.DataFrame), (
            "defer=True + as_xarray=False must return an unevaluated dask DataFrame"
        )

    def test_defer_xarray_returns_unevaluated(self, tmp_path, monkeypatch):
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=300, n_sites=5)
        reader = AirNowReader()
        ds = reader.open_dataset(
            files=fn, defer=True, wide_fmt=False, as_xarray=True, bad_utcoffset="leave"
        )
        assert isinstance(ds, xr.Dataset)
        assert _all_dask(ds)

    def test_crux_invariant_dataframe(self, tmp_path, monkeypatch):
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=250, n_sites=4)
        reader = AirNowReader()
        eager = reader.open_dataset(
            files=fn, wide_fmt=False, as_xarray=False, bad_utcoffset="leave"
        )
        lazy = reader.open_dataset(
            files=fn, defer=True, wide_fmt=False, as_xarray=False, bad_utcoffset="leave"
        )
        pd.testing.assert_frame_equal(lazy.compute(), eager)

    def test_crux_invariant_long_xarray(self, tmp_path, monkeypatch):
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=250, n_sites=4)
        reader = AirNowReader()
        eager = reader.open_dataset(
            files=fn, defer=False, wide_fmt=False, as_xarray=True, bad_utcoffset="leave"
        )
        lazy = reader.open_dataset(
            files=fn, defer=True, wide_fmt=False, as_xarray=True, bad_utcoffset="leave"
        )
        # Data + all attrs except the path-dependent provenance ``history``.
        xr.testing.assert_identical(_drop_history(lazy.compute()), _drop_history(eager))
        # ``history``: defer is the existing lazy path, modulo the wall-clock
        # second each load happened to run in.
        lazyref = reader.open_dataset(
            files=fn, lazy=True, wide_fmt=False, as_xarray=True, bad_utcoffset="leave"
        )
        assert (
            _normalize_history(lazy.compute()).attrs["history"]
            == _normalize_history(lazyref.compute()).attrs["history"]
        )

    def test_crux_invariant_wide_xarray(self, tmp_path, monkeypatch):
        """The wide (reshape) case: strategy-2 defers the pivot to ``ds_to_2d``."""
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=200, n_sites=4)
        reader = AirNowReader()
        eager = reader.open_dataset(
            files=fn, defer=False, wide_fmt=True, as_xarray=True, bad_utcoffset="leave"
        )
        lazy = reader.open_dataset(
            files=fn, defer=True, wide_fmt=True, as_xarray=True, bad_utcoffset="leave"
        )
        # The deferred wide result stays unevaluated...
        assert _all_dask(lazy)
        # ...and evaluates to the eager wide result (modulo provenance history).
        xr.testing.assert_identical(_drop_history(lazy.compute()), _drop_history(eager))
        lazyref = reader.open_dataset(
            files=fn, lazy=True, wide_fmt=True, as_xarray=True, bad_utcoffset="leave"
        )
        assert (
            _normalize_history(lazy.compute()).attrs["history"]
            == _normalize_history(lazyref.compute()).attrs["history"]
        )

    def test_table_and_xarray_conversion_share_preparation_without_persist(self, monkeypatch):
        """Width growth must not add parser passes or persist the complete table."""
        import dask.dataframe as dd

        from monetio.readers.airnow import AirNowReader

        meta = pd.DataFrame(
            {
                "time": pd.Series(dtype="datetime64[ns]"),
                "siteid": pd.Series(dtype=object),
                "variable": pd.Series(dtype="str"),
                "obs": pd.Series(dtype="float64"),
                "units": pd.Series(dtype="str"),
                "latitude": pd.Series(dtype="float64"),
                "longitude": pd.Series(dtype="float64"),
            }
        )
        widths = (10, 100)
        preparation_passes = []
        times = [pd.Timestamp("2023-01-01") + pd.to_timedelta(i, unit="h") for i in range(4)]
        original_persist = dd.DataFrame.persist

        def fail_if_persist(self, *args, **kwargs):
            raise AssertionError("reader conversion must not persist the full input")

        monkeypatch.setattr(dd.DataFrame, "persist", fail_if_persist)
        try:
            for width in widths:
                parser_calls = []

                def parse_partition(partition):
                    parser_calls.append(partition)
                    rows = []
                    for i in range(width):
                        rows.append(
                            {
                                "time": times[partition],
                                "siteid": f"S{partition}",
                                "variable": f"V{i}",
                                "obs": float(partition + i),
                                "units": "ppb",
                                "latitude": 40.0 + partition,
                                "longitude": -80.0 - partition,
                            }
                        )
                    return pd.DataFrame(rows)

                # Native table construction stays deferred and needs no schema or
                # row-length scan because the source supplies explicit metadata.
                table = dd.from_map(parse_partition, range(4), meta=meta)
                assert parser_calls == []
                assert len(table.compute()) == 4 * width
                native_table_passes = len(parser_calls)
                assert native_table_passes == 4

                # A fresh graph lets Xarray's required node layout preparation be
                # counted separately from later value evaluation.
                parser_calls.clear()
                frame = dd.from_map(parse_partition, range(4), meta=meta)
                result = AirNowReader().to_xarray(frame, defer=True)
                prep_passes = len(parser_calls)
                assert prep_passes > 0
                assert prep_passes == 4
                assert all(
                    hasattr(var.data, "dask") for var in result.data_vars.values() if var.ndim
                )
                preparation_passes.append(prep_passes)
                computed = result.compute()
                assert computed.sizes["time"] == 4
                assert len(parser_calls) > prep_passes
                result.close()
        finally:
            monkeypatch.setattr(dd.DataFrame, "persist", original_persist)

        assert preparation_passes[0] == preparation_passes[1], (
            "Xarray preparation must perform the same number of partition passes "
            "for 10 and 100 output variables"
        )

    def test_long_xarray_preparation_batches_coordinate_discovery(self):
        """The 1D node layout prepares required coordinate keys in one pass."""
        import dask.dataframe as dd

        meta = pd.DataFrame(
            {
                "time": pd.Series(dtype="datetime64[ns]"),
                "siteid": pd.Series(dtype=object),
                "obs": pd.Series(dtype="float64"),
            }
        )
        parser_calls = []

        def parse_partition(partition):
            parser_calls.append(partition)
            return pd.DataFrame(
                {
                    "time": pd.date_range("2023-01-01", periods=2) + pd.Timedelta(days=partition),
                    "siteid": [f"S{partition}"] * 2,
                    "obs": [float(partition), float(partition + 1)],
                }
            )

        frame = dd.from_map(parse_partition, range(4), meta=meta)
        result = AirNowReader().to_xarray(frame, expand2d=False, defer=True)

        assert sorted(parser_calls) == [0, 1, 2, 3]
        prep_passes = len(parser_calls)
        assert prep_passes == 4
        assert result.sizes["time"] == 8
        assert all(hasattr(var.data, "dask") for var in result.data_vars.values() if var.ndim)
        computed = result.compute()
        assert computed["obs"].values.tolist() == [0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0]
        assert len(parser_calls) == prep_passes * 2

    def test_sc011_no_observation_pivot_compute(self, tmp_path, monkeypatch):
        """SC-011: the deferred wide route performs **zero** ``long_to_wide``
        full-data computes — the exact claim in contracts/scale-options.md
        ("today ``util.long_to_wide`` performs exactly one ``.compute()`` with a
        ``UserWarning``; the deferred path performs none").

        The observation payload stays a lazy dask array end-to-end; the only
        computes during a deferred wide load are (a) the bounded per-partition
        lengths (FR-025) and (b) the ``ds_to_2d`` join-key ``set_index``, which
        materializes the *index columns* (time/siteid/variable), never the
        ``obs`` data. Neither is the ``long_to_wide`` pivot compute.
        """
        import dask.dataframe as dd

        from monetio.util import long_to_wide

        reader = AirNowReader()
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=500, n_sites=8)
        # Build the dask long frame the pivot would receive.
        df = reader.open_dataset(
            files=fn, defer=True, wide_fmt=True, as_xarray=False, bad_utcoffset="leave"
        )
        assert isinstance(df, dd.DataFrame)
        # Baseline: the non-deferred pivot computes the whole frame + warns.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with ComputeProbe(threshold=64) as eager_probe:
                long_to_wide(df, defer=False)
        assert eager_probe.full >= 1, (
            "baseline long_to_wide(defer=False) should perform a full compute"
        )
        assert any("long_to_wide" in str(w.message) for w in caught), (
            "baseline should emit the documented UserWarning"
        )
        # Deferred: long_to_wide(df, defer=True) returns the long frame, no compute.
        with ComputeProbe(threshold=64) as defer_probe:
            out = long_to_wide(df, defer=True)
        assert defer_probe.full == 0, (
            f"SC-011: long_to_wide(defer=True) performed {defer_probe.full} "
            f"full computes; must be zero"
        )
        assert isinstance(out, dd.DataFrame)
        assert "variable" in out.columns, "deferred frame must stay long"

    def test_bounded_compute_allowance_is_metadata_only(self, tmp_path, monkeypatch):
        """FR-025: any compute during a deferred load stays bounded (sizes only)."""
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=400, n_sites=4)
        reader = AirNowReader()
        with ComputeProbe(threshold=64) as probe:
            reader.open_dataset(
                files=fn, defer=True, wide_fmt=False, as_xarray=True, bad_utcoffset="leave"
            )
        # The deferred path may compute a small bounded amount (the shared
        # per-partition lengths), but never a full data column.
        assert probe.max_elements <= 64, (
            f"deferred load computed {probe.max_elements} elements in one call; "
            "FR-025 bounds this to metadata/dimension sizes"
        )


# ---------------------------------------------------------------------------
# FR-035: unsupported defer raises, naming source + reason
# ---------------------------------------------------------------------------
class TestDeferUnsupportedRaises:
    def test_point_source_without_capability_raises(self, tmp_path, monkeypatch):
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=10, n_sites=1)
        reader = AirNowReader()
        real = MANIFEST_BY_NAME["airnow"]
        object.__setattr__(real, "capabilities", ScaleCapabilities(deferred=False))
        try:
            with pytest.raises(NotImplementedError) as exc:
                reader.open_dataset(
                    files=fn, defer=True, wide_fmt=False, as_xarray=False, bad_utcoffset="leave"
                )
            msg = str(exc.value)
            assert "airnow" in msg.lower()
            assert "defer" in msg.lower()
        finally:
            object.__setattr__(
                real, "capabilities", ScaleCapabilities(deferred=True, deferred_wide=True)
            )

    def test_gridded_source_without_capability_raises(self):
        if len(_MULTIFILE) < 2:
            pytest.skip("multifile fixture missing")
        reader = CMAQReader()
        real = MANIFEST_BY_NAME["cmaq"]
        object.__setattr__(real, "capabilities", ScaleCapabilities(deferred=False))
        try:
            with pytest.raises(NotImplementedError) as exc:
                reader.open_dataset(_MULTIFILE, defer=True)
            assert "cmaq" in str(exc.value).lower()
        finally:
            object.__setattr__(
                real,
                "capabilities",
                ScaleCapabilities(
                    deferred=True, parallel_metadata=True, chunk_controls=("chunks",)
                ),
            )

    def test_never_silently_materializes(self, tmp_path, monkeypatch):
        """A raising source must not return an evaluated object as a fallback."""
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=10, n_sites=1)
        reader = AirNowReader()
        real = MANIFEST_BY_NAME["airnow"]
        object.__setattr__(real, "capabilities", ScaleCapabilities(deferred=False))
        try:
            with pytest.raises(NotImplementedError):
                reader.open_dataset(
                    files=fn, defer=True, wide_fmt=False, as_xarray=False, bad_utcoffset="leave"
                )
        finally:
            object.__setattr__(
                real, "capabilities", ScaleCapabilities(deferred=True, deferred_wide=True)
            )


# ---------------------------------------------------------------------------
# SC-014: slice evaluation touches only the required files
# ---------------------------------------------------------------------------
class TestSliceFileIsolation:
    @pytest.mark.skipif(len(_MULTIFILE) < 4, reason="multifile fixture missing")
    def test_slice_touches_only_required_files(self):
        """A single-timestep slice reads exactly the one backing file."""
        import h5py

        files = _MULTIFILE
        accessed: list[str] = []
        orig = h5py.Dataset.__getitem__

        def traced(self, key):
            accessed.append(Path(self.file.filename).name)
            return orig(self, key)

        h5py.Dataset.__getitem__ = traced
        try:
            ds = xr.open_mfdataset(
                files, combine="nested", concat_dim="time", chunks={}, engine="h5netcdf"
            )
            accessed.clear()  # keep only the evaluation-phase reads
            ds.isel(time=0).load()
        finally:
            h5py.Dataset.__getitem__ = orig

        distinct = set(accessed)
        assert len(distinct) == 1, (
            f"SC-014: a 1-step slice read {len(distinct)} distinct files "
            f"({sorted(distinct)}); it must touch only the 1 required file"
        )
        assert next(iter(distinct)) == Path(files[0]).name
