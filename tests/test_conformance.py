"""Shared reader conformance suite.

One parametrized suite that runs the same contract against every registered
source, driven by ``monetio.available_sources()`` so it cannot drift from the
registry. Rules:

- C-01 kind-appropriate return type
- C-02 non-empty ``history`` provenance
- C-03 ``latitude``/``longitude`` naming for location-bearing sources
- C-04 structural rules (one registered class per module except
  manifest-declared multi-class, base class, ``open_dataset`` + ``harmonize``,
  docstring)
- C-05 dual-backend equivalence for sources declaring deferred support
- C-06 no hidden computes on a deferred load
- C-07 no forced chunking on the default (eager) path
- C-08 capability honesty — every ``True`` in ``ScaleCapabilities`` is exercised
- C-09 a small slice of a deferred result does not evaluate the whole
- C-10 unsupported options reported (virtualization options rejected by name)
- C-11 read failures name the file and preserve the exception type
- C-12 registry consistency
- C-13 ``__all__`` / star-import resolvability

Every source is exercised: load-dependent rules run against bundled fixtures,
and sources with no local fixture appear in the checked-in exclusion list with
a reason (they are *reported*, not silently skipped). All tests are
network-free.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import json
import re
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from conftest import SampleRegistry

import monetio
from monetio._manifest import SOURCE_MANIFEST
from monetio.readers.base import READER_REGISTRY, GriddedReader

_ROOT = Path(__file__).parent.parent
_DATA = _ROOT / "tests" / "data"

# Family-recorded exclusion reasons live with the capture specs so a source can
# be excluded by the family that owns it without editing this shared file.
sys.path.insert(0, str(_ROOT / "scripts"))
from sample_specs import EXCLUSIONS as _FAMILY_EXCLUSIONS  # noqa: E402

# ---------------------------------------------------------------------------
# Fixture resolver: source -> local sample load (network-free)
# ---------------------------------------------------------------------------

# The offline sample set is the single source of truth for what can be exercised
# by a load-dependent rule: the Phase 1 fixtures plus every recorded payload
# under ``tests/data/samples/`` and every synthetic file under
# ``tests/data/synthetic/``. Resolution goes through the shared ``SampleRegistry``
# (conftest, T007) so a sample added by ``record_samples.py`` or
# ``make_synthetic_sample.py`` is picked up here automatically — the coverage map
# cannot drift from the samples on disk. Sources with no sample appear in
# ``EXCLUSIONS`` below with a reason (SC-007).
_REGISTRY = SampleRegistry()


@pytest.fixture(autouse=True)
def _offline_station_metadata(monkeypatch):
    """Keep the shared sampled-reader conformance checks independent of DNS."""
    import monetio.readers.epa_utils as epa_utils
    import monetio.readers.ish as ish_module
    import monetio.readers.ndbc as ndbc_module

    airnow_file = _DATA / "samples" / "airnow" / "HourlyData_2023010100.dat"
    airnow_records = pd.read_csv(airnow_file, sep="|", header=None)
    airnow_sites = (
        airnow_records[[2, 3]].drop_duplicates().rename(columns={2: "siteid", 3: "site_name"})
    )
    airnow_sites["siteid"] = airnow_sites["siteid"].astype(str).str.zfill(9)
    airnow_sites["latitude"] = 40.0
    airnow_sites["longitude"] = -80.0
    empty_sites = pd.DataFrame(columns=["siteid", "site_name", "latitude", "longitude"])
    monkeypatch.setattr(
        epa_utils,
        "read_monitor_file",
        lambda network=None, airnow=False: airnow_sites if airnow else empty_sites,
    )

    def local_ish_history(reader, dates=None):
        reader.history = pd.DataFrame(
            {
                "usaf": ["040520"],
                "wban": ["99999"],
                "lat": [40.0],
                "lon": [-80.0],
                "begin": [pd.Timestamp("1900-01-01")],
                "end": [pd.Timestamp("2100-01-01")],
                "ctry": ["US"],
                "state": ["XX"],
                "station name": ["Fixture station"],
                "station_id": ["04052099999"],
            }
        )

    monkeypatch.setattr(ish_module.ISH, "read_ish_history", local_ish_history)
    monkeypatch.setattr(
        ndbc_module,
        "get_station_table",
        lambda: pd.DataFrame(
            {
                "siteid": ["41002"],
                "name": ["Fixture buoy"],
                "latitude": [40.0],
                "longitude": [-80.0],
            }
        ),
    )


# ``files`` is already anchored to the repo root by the registry; ``kwargs`` are
# the sample's recorded ``load_kwargs`` (a nested ``group``, ``add_metadata``, ...).
_FIXTURES: dict[str, tuple] = {
    source: (
        _REGISTRY.resolve(source)["files"],
        _REGISTRY.resolve(source)["kwargs"],
    )
    for source in sorted(_REGISTRY.sampled_sources())
    if _REGISTRY.resolve(source) is not None
}

# A true alias resolves to the same reader class as its canonical, so exercising
# the canonical exercises the alias (C-SAMP-1): give the alias name the same
# fixture. A declared alias whose reader is a *distinct* class (the gfs family's
# gdas/gefs, each with its own bucket and URL builder) is not collapsed — it
# needs its own sample, so it stays uncovered until one lands.
from monetio._manifest import MANIFEST_BY_ALIAS  # noqa: E402


def _registered_class(name):
    """The reader class ``name`` resolves to, importing its module if needed.

    Duplicated here (rather than reusing ``_reader_class``) because the alias
    map is built before that helper is defined in module order.
    """
    if name not in READER_REGISTRY:
        importlib.import_module(monetio._manifest.module_for(name), package="monetio")
    return READER_REGISTRY.get(name)


for _alias, _spec in MANIFEST_BY_ALIAS.items():
    if _alias in _FIXTURES:
        continue
    _canonical = _spec.name
    if _canonical in _FIXTURES and _registered_class(_alias) is _registered_class(_canonical):
        _FIXTURES[_alias] = _FIXTURES[_canonical]

# Multi-file fixture for the gridded slice/concurrency rules.
_MULTIFILE = sorted(str(p) for p in (_DATA / "multifile").glob("*.nc"))

# Checked-in exclusion list for load-dependent rules (SC-007). Every
# sample-backed source is covered; everything else is excluded with an
# explicit reason and reported by ``test_every_source_covered_or_excluded``,
# so no source can disappear from coverage. ``_fixture_for`` skips with the
# recorded reason rather than silently passing.
#
# A source's reason comes from the family that owns it (``sample_specs``), which
# records *why* it cannot be sampled offline — not redistributable and not
# reproducible. The generic placeholder below only appears while a source is
# still awaiting capture; a final reasoned exclusion replaces it.
_NO_FIXTURE_REASON = (
    "no offline sample (capture via record_samples.py or synthesize via make_synthetic_sample.py)"
)
EXCLUSIONS: dict[str, str] = {
    name: _FAMILY_EXCLUSIONS.get(name, _NO_FIXTURE_REASON)
    for name in monetio.available_sources()
    if name not in _FIXTURES
}


def _fixture_for(source: str):
    """Return concrete (files, kwargs) for a sample-backed source, else skip.

    Paths come back already anchored to the repo root from the sample registry;
    a glob (a legacy fixture pattern) is expanded here.
    """
    if source not in _FIXTURES:
        pytest.skip(f"{source}: {_NO_FIXTURE_REASON} (recorded conformance exclusion)")
    module = _REGISTRY.requires(source)
    if module:
        import importlib.util

        if importlib.util.find_spec(module) is None:
            pytest.skip(f"{source}: sample requires {module!r} (not installed)")
    files, kwargs = _FIXTURES[source]
    if isinstance(files, str) and "*" in files:
        matches = sorted(str(p) for p in _ROOT.glob(files))
        if not matches:
            pytest.skip(f"fixture glob matched nothing: {files}")
        files = matches
    # A recorded payload can be absent from the checkout: the large HDF-EOS5/
    # HDF4 samples (mopitt, omps) are git-ignored so they exist only where they
    # were captured, never on a fresh CI runner. Skip with the reason rather
    # than failing on a file the environment never had — matching
    # ``SampleRegistry.require``, which guards the same case for the other
    # load-dependent suites.
    if isinstance(files, str) and not Path(files).exists():
        pytest.skip(f"{source}: sample payload missing: {files}")
    if isinstance(files, list) and files and not all(Path(f).exists() for f in files):
        missing = [f for f in files if not Path(f).exists()]
        pytest.skip(f"{source}: sample payload missing: {missing}")
    return files, dict(kwargs)


def _reader_class(source: str):
    if source not in READER_REGISTRY:
        importlib.import_module(monetio.source_info(source).module, package="monetio")
    return READER_REGISTRY[source]


def _kind(source: str) -> str:
    return monetio.source_info(source).kind


# ---------------------------------------------------------------------------
# Coverage: every source is either fixture-backed or explicitly excluded
# ---------------------------------------------------------------------------


def test_every_source_covered_or_excluded():
    """SC-007: registry coverage is 100%; exclusions carry a reason."""
    sources = set(monetio.available_sources())
    covered = sources - set(EXCLUSIONS)
    assert covered == set(_FIXTURES), "coverage map and fixture map disagree"
    for name, reason in EXCLUSIONS.items():
        assert reason, f"exclusion for {name!r} has no reason"


# ---------------------------------------------------------------------------
# C-01 / C-02 / C-03 — load-dependent, parametrized over fixture-backed sources
# ---------------------------------------------------------------------------

_FIXTURED = sorted(_FIXTURES)


@pytest.mark.parametrize("source", _FIXTURED)
def test_c01_kind_appropriate_return_type(source):
    """C-01: load returns the type appropriate to the source's kind."""
    files, kwargs = _fixture_for(source)
    result = monetio.load(source, files=files, **kwargs)
    kind = _kind(source)
    if kind == "gridded":
        assert isinstance(result, xr.Dataset), (
            f"{source}: kind=gridded but load returned {type(result).__name__}"
        )
    else:
        assert isinstance(result, pd.DataFrame | xr.Dataset), (
            f"{source}: kind=point but load returned {type(result).__name__}, "
            "expected DataFrame or Dataset"
        )


@pytest.mark.parametrize("source", _FIXTURED)
def test_c02_history_provenance(source):
    """C-02: the result carries a non-empty ``history`` recording the read."""
    files, kwargs = _fixture_for(source)
    result = monetio.load(source, files=files, **kwargs)
    _assert_history(source, result)


def _assert_history(source: str, result) -> None:
    """The C-02 assertion, shared by the sweep and the teeth test."""
    history = str(result.attrs.get("history", "")).strip()
    assert history, f"{source}: load produced an empty or missing 'history' attribute"


@pytest.mark.parametrize("source", _FIXTURED)
def test_c03_coordinate_naming(source):
    """C-03: location-bearing sources name their location coordinate canonically.

    The location a source carries is represented as a coordinate (a Dataset
    ``latitude``/``longitude``) or, for a tabular result, a column. Those must
    use the canonical names, not the ``lat``/``lon``/``grid_lat`` abbreviations.
    Only the coordinate/column set is scanned, not every data variable: a
    product's own metadata variables routinely embed the letters ``lat``/``lon``
    (GOES ships ``lat_band_aod550_*`` quality fields and a
    ``geospatial_lat_lon_extent`` scalar) without being the location, so
    renaming them would corrupt the product rather than fix naming. Matching on
    word boundaries keeps unrelated coordinate names that merely contain the
    letter run (``relative_humidity`` is a data variable; ``elevation`` a
    coordinate) out of the offender list while still catching bare
    ``lat``/``lon`` and the ``grid_lat``/``nav_lon``-style prefixed variants.
    """
    files, kwargs = _fixture_for(source)
    result = monetio.load(source, files=files, **kwargs)
    names = set(result.coords) if isinstance(result, xr.Dataset) else set(result.columns)
    offenders = sorted(n for n in names if re.search(r"(?:^|_)(?:lat|lon)(?:_|$)", str(n).lower()))
    assert not offenders, (
        f"{source}: location-bearing result uses non-canonical names: {offenders} "
        "(expected latitude/longitude)"
    )


# ---------------------------------------------------------------------------
# C-05 / C-06 / C-08 / C-10 — deferred-capable sources
# ---------------------------------------------------------------------------

_DEFERRED_SOURCES = sorted(s.name for s in SOURCE_MANIFEST if s.capabilities.deferred)


def _drop_history(obj):
    """Copy with the ``history`` provenance log removed (global + per-var).

    The eager (pandas) path keeps per-stage provenance lines that the dask
    path has always dropped (dask DataFrames carry no ``.attrs``); that
    divergence predates deferred loading. Equivalence is asserted on data and
    structure, with provenance strings excluded.
    """
    out = obj.copy()
    out.attrs.pop("history", None)
    if isinstance(out, xr.Dataset):
        for var in out.variables:
            out[var].attrs.pop("history", None)
    return out


def _is_dask(obj) -> bool:
    import dask.dataframe as dd

    if isinstance(obj, dd.DataFrame):
        return True
    if isinstance(obj, xr.Dataset):
        return any(hasattr(v.data, "dask") for v in obj.data_vars.values() if v.ndim > 0)
    return False


def _airnow_synthetic(tmp_path, monkeypatch, n_rows=48, n_sites=4):
    """Write a small network-free AirNow-format hourly file.

    The AirNow reader joins site metadata from a remote file; patch that join
    out so the fixture load stays offline. Rows are unique on
    (time, siteid, variable) so the eager pivot and the deferred unstack
    agree by construction.
    """
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
    lines = []
    for i in range(n_rows):
        site = f"0{i % n_sites:08d}"
        hour = (i // n_sites) % 24
        day = 1 + (i // n_sites) // 24
        lines.append(f"01/{day:02d}/23|{hour:02d}:00|{site}|T|-5|OZONE|PPB|{50.0 + i}|X")
    fn = tmp_path / "HourlyData_2023010100.dat"
    fn.write_text("\n".join(lines), encoding="ISO-8859-1")
    return str(fn)


def _tolnet_synthetic(tmp_path, n_files=2):
    """Write small TOLNet-layout HDF5 files (the bundled fixture is single-
    granule HDF4 that the custom reader parses to an empty result here).

    A fresh subdirectory per call: a deferred tolnet result is dask-backed and
    leaves its files open until computed, so a later call must not reuse
    (i.e. truncate) the same paths.
    """
    h5py = pytest.importorskip("h5py")
    target = tmp_path / f"tolnet_{uuid.uuid4().hex[:8]}"
    target.mkdir(parents=True)
    paths = []
    for i in range(n_files):
        tstart = 1600000000000 + i * 7200000
        p = target / f"TOLNet_{i}.hdf5"
        with h5py.File(p, "w") as f:
            ga = f.create_group("INSTRUMENT_ATTRIBUTES")
            ga.attrs["Location_Latitude"] = b"39.0 N"
            ga.attrs["Location_Longitude"] = b"76.5 W"
            gd = f.create_group("DATA")
            gd.create_dataset("ALT", data=np.linspace(0, 10000, 10))
            gd.create_dataset("TIME_MID_UT_UNIX", data=np.array([tstart, tstart + 3600000]))
            gd.create_dataset("O3MR", data=np.full((10, 2), 0.5 + i))
            gd.create_dataset("O3ND", data=np.full((10, 2), 1e9 * (0.5 + i)))
        paths.append(str(p))
    return paths


def _aeronet_small(tmp_path, n_rows=10):
    """A short slice of the bundled AERONET fixture (header + first rows).

    The deferred wide path is quadratic in the number of wavelength columns,
    not rows — a small row count keeps the eager/deferred comparison quick
    without weakening the check.
    """
    lines = (_DATA / "aeronet-AOD15-example.txt").read_text().splitlines()
    hdr = next(i for i, ln in enumerate(lines) if ln.startswith("AERONET_Site,"))
    out = tmp_path / "aeronet_small.txt"
    out.write_text("\n".join(lines[: hdr + 1] + lines[hdr + 1 : hdr + 1 + n_rows]))
    return str(out)


def _deferred_load(source, tmp_path, monkeypatch, defer: bool, output_form=None):
    """Load a deferred-capable source/output form either eager or deferred."""
    point_options = {}
    if output_form == "table":
        point_options = {"as_xarray": False, "wide_fmt": False}
    elif output_form == "wide_dataset":
        point_options = {"as_xarray": True, "wide_fmt": True}
    if source == "airnow":
        files = _airnow_synthetic(tmp_path, monkeypatch)
        return monetio.load(source, files=files, defer=defer, **point_options)
    if source == "tolnet":
        files = _tolnet_synthetic(tmp_path)
        return monetio.load(source, files=files, defer=defer)
    if source == "aeronet":
        files = [_aeronet_small(tmp_path)]
        return monetio.load(source, files=files, defer=defer, **point_options)
    files, kwargs = _fixture_for(source)
    return monetio.load(source, files=files, defer=defer, **kwargs)


_DEFERRED_OUTPUT_MODES = [
    ("airnow", "table"),
    ("airnow", "wide_dataset"),
    ("aeronet", "table"),
    ("aeronet", "wide_dataset"),
    ("cmaq", "gridded_dataset"),
    ("tolnet", "gridded_dataset"),
]


@pytest.mark.parametrize(("source", "output_form"), _DEFERRED_OUTPUT_MODES)
def test_c05_dual_backend_equivalence(source, output_form, tmp_path, monkeypatch):
    """C-05: each advertised deferred output matches its eager result exactly."""
    eager = _deferred_load(source, tmp_path, monkeypatch, defer=False, output_form=output_form)
    deferred = _deferred_load(source, tmp_path, monkeypatch, defer=True, output_form=output_form)
    assert _is_dask(deferred), f"{source}/{output_form}: defer=True returned an evaluated object"
    if isinstance(deferred, xr.Dataset):
        computed = deferred.compute()
        xr.testing.assert_identical(_drop_history(eager), _drop_history(computed))
        assert set(eager.data_vars) == set(computed.data_vars), (
            f"{source}: deferred load changed the variable set"
        )
        assert dict(eager.sizes) == dict(computed.sizes), f"{source}: deferred load changed dims"
    else:
        import dask.dataframe as dd

        computed = deferred.compute() if isinstance(deferred, dd.DataFrame) else deferred
        pd.testing.assert_frame_equal(
            _drop_history(eager).reset_index(drop=True), computed.reset_index(drop=True)
        )
        assert _drop_history(eager).attrs == computed.attrs, (
            f"{source}/{output_form}: deferred table changed meaningful provenance attrs"
        )


def test_manifest_deferred_claims_have_mode_evidence():
    """Manifest Dask flags must resolve to passing mode-level ledger evidence."""
    ledger = json.loads((_DATA / "dask_reader_audit.json").read_text())
    modes_by_source = {}
    for mode in ledger["modes"]:
        modes_by_source.setdefault(mode["source"], []).append(mode)
    evidence = {case["case_id"]: case for case in ledger["evidence"]}

    for spec in SOURCE_MANIFEST:
        if not spec.capabilities.deferred:
            continue
        source_modes = modes_by_source.get(spec.name, [])
        deferred_modes = [mode for mode in source_modes if mode["options"].get("defer") is True]
        assert deferred_modes, f"{spec.name}: deferred=True has no inventoried defer=True mode"
        supported = [
            mode
            for mode in deferred_modes
            if mode["capabilities"]["deferred"]["status"] == "supported"
        ]
        assert len(supported) == len(deferred_modes), (
            f"{spec.name}: manifest deferred=True is not supported for every output/backend mode"
        )
        for mode in deferred_modes:
            refs = mode["capabilities"]["deferred"]["evidence_ids"]
            assert refs, f"{mode['mode_id']}: deferred support has no evidence IDs"
            assert any(
                evidence[ref]["run_status"] == "passed"
                and mode["mode_id"] in evidence[ref]["affected_modes"]
                for ref in refs
            ), f"{mode['mode_id']}: deferred support lacks matching passing evidence"

        def _has_passing_evidence(mode, capability):
            return any(
                ref in evidence
                and evidence[ref]["run_status"] == "passed"
                and mode["mode_id"] in evidence[ref]["affected_modes"]
                for ref in mode["capabilities"][capability]["evidence_ids"]
            )

        if spec.capabilities.distributed:
            for mode in supported:
                assert mode["capabilities"]["distributed"]["status"] == "supported", (
                    f"{mode['mode_id']}: manifest distributed=True lacks a supported assessment"
                )
                assert _has_passing_evidence(mode, "distributed"), (
                    f"{mode['mode_id']}: distributed support lacks passing mode evidence"
                )

        if spec.capabilities.chunk_controls:
            chunk_modes = [
                mode
                for mode in source_modes
                if mode["options"].get("defer") is False
                or mode["capabilities"]["deferred"]["status"] == "supported"
            ]
            for mode in chunk_modes:
                assert mode["capabilities"]["chunk_partition_controls"]["status"] == "supported", (
                    f"{mode['mode_id']}: manifest chunk control lacks a supported assessment"
                )
                assert _has_passing_evidence(mode, "chunk_partition_controls"), (
                    f"{mode['mode_id']}: chunk control lacks passing mode evidence"
                )

        if spec.capabilities.deferred_wide:
            wide_modes = [mode for mode in deferred_modes if mode["output_form"] == "wide_dataset"]
            assert wide_modes, f"{spec.name}: deferred_wide has no wide Xarray mode"
            for mode in wide_modes:
                assert (
                    mode["capabilities"]["deferred_output_conversion"]["status"] == "supported"
                ), f"{mode['mode_id']}: deferred_wide claim lacks a supported mode assessment"
                assert _has_passing_evidence(mode, "deferred_output_conversion"), (
                    f"{mode['mode_id']}: deferred_wide claim lacks passing wide-conversion evidence"
                )


class _ComputeProbe:
    """Record computations triggered during a block and flag payload computes.

    A *payload* compute is one that materializes the observation data: a
    DataFrame with more than ``threshold`` elements, or a numeric
    (float/int) array/Series above the threshold. The documented bounded
    allowance — a deferred wide point load materializing its join-key index
    columns (time/siteid/variable, which are datetime64/object) and per-
    partition lengths — is *not* a payload compute, because it never touches
    the numeric observation values. This is the exact invariant the scale
    suite pins down; here it is asserted per deferred-capable source.
    """

    def __init__(self, threshold: int = 64):
        self.threshold = threshold
        self.payload = 0
        self.records = []
        self._orig_base = None
        self._orig_mod = None

    @staticmethod
    def _is_numeric(obj) -> bool:
        dtype = getattr(obj, "dtype", None)
        if dtype is None:
            return False
        return np.issubdtype(np.dtype(dtype), np.number)

    def __enter__(self):
        import dask

        probe = self

        def counting(*args, **kwargs):
            out = probe._orig_base(*args, **kwargs)
            for o in out if isinstance(out, tuple) else (out,):
                if isinstance(o, pd.DataFrame):
                    size = int(o.size)
                    numeric = size > 0
                    self.records.append(("df", size))
                elif isinstance(o, pd.Series):
                    size = int(o.size)
                    numeric = self._is_numeric(o)
                    self.records.append(("series", size))
                elif isinstance(o, np.ndarray):
                    size = int(o.size)
                    numeric = self._is_numeric(o)
                    self.records.append(("ndarray", size))
                else:
                    size = 0
                    numeric = False
                # A payload compute: a materialized frame, or a large numeric
                # array. Small/bounded and index-column (non-numeric) computes
                # are the allowed metadata materialization.
                if size > probe.threshold and numeric:
                    probe.payload += 1
            return out

        self._orig_base = dask.base.compute
        self._orig_mod = dask.compute
        dask.base.compute = counting
        dask.compute = counting
        return self

    def __exit__(self, *exc):
        import dask

        dask.base.compute = self._orig_base
        dask.compute = self._orig_mod
        return False


@pytest.mark.parametrize("source", _DEFERRED_SOURCES)
def test_c06_no_hidden_computes(source, tmp_path, monkeypatch):
    """C-06: a defer=True load never materializes the observation payload.

    The result must come back unevaluated (dask-backed), and no compute during
    the load may materialize numeric observation data. Bounded metadata and
    the join-key index columns a wide reshape structurally needs are allowed
    (they are not the payload).
    """
    with _ComputeProbe() as probe:
        result = _deferred_load(source, tmp_path, monkeypatch, defer=True)
    assert _is_dask(result), f"{source}: defer=True returned an evaluated object"
    assert probe.payload == 0, (
        f"{source}: defer=True load materialized the observation payload "
        f"{probe.payload} time(s); computes seen: {probe.records}"
    )


@pytest.mark.parametrize("source", _DEFERRED_SOURCES)
def test_c08_deferred_flag_honored(source, tmp_path, monkeypatch):
    """C-08 (deferred): the declared deferred support is observable."""
    result = _deferred_load(source, tmp_path, monkeypatch, defer=True)
    assert _is_dask(result), f"{source}: claims deferred=True but result is materialized"


def test_c08_deferred_wide_flag_honored(tmp_path, monkeypatch):
    """C-08 (deferred_wide): the reshape really is deferred.

    Eager wide output carries a column per pollutant; a deferred wide load
    must still be LONG (the variable/obs columns) — proving the long->wide
    reshape was skipped in the DataFrame stage, not merely computed lazily.
    """
    import dask.dataframe as dd

    files = _airnow_synthetic(tmp_path, monkeypatch)
    eager = monetio.load("airnow", files=files, as_xarray=False, wide_fmt=True)
    assert "OZONE" in eager.columns, "eager wide path should carry pollutant columns"
    deferred = monetio.load("airnow", files=files, as_xarray=False, defer=True)
    assert isinstance(deferred, dd.DataFrame) and not isinstance(deferred, pd.DataFrame)
    assert "variable" in deferred.columns and "obs" in deferred.columns, (
        "airnow claims deferred_wide=True but the deferred frame is not LONG "
        f"(columns: {list(deferred.columns)})"
    )
    assert "OZONE" not in deferred.columns


def test_c08_parallel_metadata_flag_honored():
    """C-08 (parallel_metadata): parallel=True yields the same result as serial.

    The flag's observable contract is that the source accepts it and the
    metadata parsing is result-identical (the speed upside is measured in the
    scale-concurrency tests, not here).
    """
    files = _MULTIFILE if len(_MULTIFILE) >= 2 else _FIXTURES["cmaq"][0]
    serial = monetio.load("cmaq", files=files)
    parallel = monetio.load("cmaq", files=files, parallel=True)
    xr.testing.assert_allclose(_drop_history(serial), _drop_history(parallel))


def test_c08_concurrent_files_flag_honored(tmp_path):
    """C-08 (concurrent_files): the delayed fan-out path yields the serial result.

    TOLNet merges per-file groups through a custom ``read_method``, so the
    concurrent fan-out applies there. Two synthetic files are generated in
    TOLNet's HDF5 layout (the bundled fixture is single-granule).
    """
    h5py = pytest.importorskip("h5py")
    paths = []
    for i, tstart in enumerate([1600000000000, 1600007200000]):
        p = tmp_path / f"TOLNet_{i}.hdf5"
        with h5py.File(p, "w") as f:
            ga = f.create_group("INSTRUMENT_ATTRIBUTES")
            ga.attrs["Location_Latitude"] = b"39.0 N"
            ga.attrs["Location_Longitude"] = b"76.5 W"
            gd = f.create_group("DATA")
            gd.create_dataset("ALT", data=np.linspace(0, 10000, 10))
            gd.create_dataset("TIME_MID_UT_UNIX", data=np.array([tstart, tstart + 3600000]))
            gd.create_dataset("O3MR", data=np.full((10, 2), 0.5 + i))
        paths.append(str(p))
    serial = monetio.load("tolnet", files=paths, concurrent_files=False)
    concurrent = monetio.load("tolnet", files=paths, concurrent_files=True)
    xr.testing.assert_allclose(_drop_history(serial), _drop_history(concurrent))
    assert serial.sizes == concurrent.sizes


def test_c08_chunk_controls_flag_honored():
    """C-08 (chunk_controls): the declared ``chunks`` control reaches the result."""
    ds = monetio.load(
        "cmaq", files=_MULTIFILE if _MULTIFILE else "benchmark_cmaq.nc", chunks={"x": 2}
    )
    var = next(v for v in ds.data_vars.values() if v.ndim > 0)
    assert var.chunks is not None, "cmaq declares chunks control but result is not chunked"
    nx = ds.sizes["x"]
    assert var.chunks[var.get_axis_num("x")] == (2,) * (nx // 2) + ((nx % 2,) if nx % 2 else ())


@pytest.mark.parametrize("source", ["cmaq", "airnow"])
def test_c08_distributed_flag_honored(source, tmp_path, monkeypatch, local_distributed_cluster):
    """C-08 (distributed): the deferred graph evaluates on a distributed scheduler.

    The in-process cluster is the same graph the threads scheduler runs; the
    observable contract is that evaluation succeeds there and matches the
    synchronous result.
    """
    import dask

    if source == "airnow":
        # The deferred point path shuffles (drop_duplicates); dask's default
        # p2p shuffle needs real worker processes, so pin the scheduler-
        # agnostic ``tasks`` shuffle before building the graph. The contract
        # is that the *same graph* evaluates equivalently across schedulers.
        from monetio.readers.airnow import AirNowReader

        monkeypatch_ctx = dask.config.set({"dataframe.shuffle.method": "tasks"})
        monkeypatch_ctx.__enter__()
        try:
            files = _airnow_synthetic(tmp_path, monkeypatch)
            deferred = AirNowReader().open_dataset(
                files=files,
                defer=True,
                wide_fmt=False,
                as_xarray=False,
                bad_utcoffset="leave",
            )
            assert hasattr(deferred, "npartitions")
            on_distributed = deferred.compute()
            with dask.config.set(scheduler="threads"):
                on_threads = deferred.compute()
        finally:
            monkeypatch_ctx.__exit__(None, None, None)

        def _ordered(df):
            key = [c for c in ("time", "siteid", "variable") if c in df.columns]
            return df.sort_values(key).reset_index(drop=True) if key else df.reset_index(drop=True)

        pd.testing.assert_frame_equal(_ordered(on_distributed), _ordered(on_threads))
    else:
        ds = monetio.load("cmaq", files=_MULTIFILE, defer=True)
        dist = ds.compute()  # under the active distributed Client
        with dask.config.set(scheduler="sync"):
            sync = monetio.load("cmaq", files=_MULTIFILE, defer=True).compute()
        xr.testing.assert_allclose(_drop_history(dist), _drop_history(sync))


# ---------------------------------------------------------------------------
# C-07 — no forced chunking on the default path
# ---------------------------------------------------------------------------


# The default (eager, no scale options) path must not hand back dask-backed
# arrays: chunking is the caller's decision at the I/O stage.
@pytest.mark.parametrize("source", _FIXTURED)
def test_c07_no_forced_chunking(source):
    """C-07: the default load does not hardcode chunking.

    Two forms are checked: a static scan of the reader module for
    ``.chunk(`/`chunks=` literals outside the documented backend-agnostic
    coordinate helpers, and a functional check that an eager default load
    returns numpy-backed (not dask-backed) arrays.
    """
    spec = monetio.source_info(source)
    mod = importlib.import_module(spec.module, package="monetio")
    src = Path(inspect.getsourcefile(mod)).read_text(encoding="utf-8")
    tree = ast.parse(src)

    # Line ranges of ``if`` bodies whose test guards on existing chunks (e.g.
    # ``if hasattr(ds, "chunks") and ds.chunks:``). A ``.chunk()`` inside such
    # a block re-chunks already-lazy data and is the documented backend-
    # agnostic coordinate-helper pattern, not forced eager chunking.
    guarded = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and "chunks" in ast.unparse(node.test):
            for stmt in node.body:
                for sub in ast.walk(stmt):
                    if hasattr(sub, "lineno"):
                        guarded.add(sub.lineno)

    hardcoded = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Attribute) and fn.attr == "chunk":
                if node.args and isinstance(node.args[0], ast.Dict):
                    if node.lineno not in guarded:
                        hardcoded.append(node.lineno)
    assert not hardcoded, (
        f"{source}: reader hardcodes .chunk() at line(s) {hardcoded}; "
        "chunking must be the caller's decision"
    )

    files, kwargs = _fixture_for(source)
    result = monetio.load(source, files=files, **kwargs)
    if isinstance(result, xr.Dataset):
        dasky = [k for k, v in result.data_vars.items() if hasattr(v.data, "dask")]
        assert not dasky, (
            f"{source}: default (eager) load returned dask-backed variables {dasky[:5]} "
            "without being asked — forced chunking"
        )


# ---------------------------------------------------------------------------
# C-09 — deferred slice evaluates only what it needs
# ---------------------------------------------------------------------------


def test_c09_deferred_slice_touches_required_files():
    """C-09: reading one time step of a deferred multi-file load touches one file."""
    h5py = pytest.importorskip("h5py")
    if len(_MULTIFILE) < 3:
        pytest.skip("multifile fixture missing")
    touched = {}
    orig = h5py.Dataset.__getitem__

    def _read_count(shape, key):
        """Number of elements an h5py ``__getitem__`` key selects."""
        if key is Ellipsis or key == ():
            return int(np.prod(shape)) if shape else 1
        if not isinstance(key, tuple):
            key = (key,)
        # Expand a leading/trailing Ellipsis to full-dimension slices.
        if Ellipsis in key:
            i = key.index(Ellipsis)
            n_explicit = len([k for k in key if k is not Ellipsis])
            fill = (slice(None),) * (len(shape) - n_explicit)
            key = key[:i] + fill + key[i + 1 :]
        n = 1
        for axis, k in enumerate(key):
            dim = shape[axis]
            if isinstance(k, slice):
                n *= max(0, len(range(*k.indices(dim))))
            elif isinstance(k, int | np.integer):
                n *= 1
            else:  # array-like fancy index
                n *= int(np.size(k))
        return n

    def traced(self, key):
        # Record (file, variable) for every element read. Building a
        # multi-file dataset reads the coordinate variables (time/y/x) from
        # every file — that is metadata, not the observation payload. A lazy
        # data slice must pull the *gridded field* from only the file(s)
        # holding the selected step.
        n = _read_count(self.shape, key)
        if n:
            touched[(self.file.filename, self.name)] = n
        return orig(self, key)

    h5py.Dataset.__getitem__ = traced
    try:
        ds = monetio.load("cmaq", files=_MULTIFILE, defer=True)
        # The load reads only coordinate/metadata; the gridded fields are
        # untouched until a compute is requested. h5py names carry a leading
        # slash, so match on the data-variable names.
        field_names = {f"/{v}" for v in ds.data_vars}
        load_fields = {k for k in touched if k[1] in field_names}
        assert not load_fields, (
            f"cmaq defer=True load eagerly read field data "
            f"{sorted(load_fields)[:3]} before any compute was requested"
        )
        touched.clear()
        # Each fixture file carries one time step; selecting the first keeps a
        # length-1 time axis (expand-style) so the result is comparable.
        small = ds.isel(time=slice(0, 1)).load()
    finally:
        h5py.Dataset.__getitem__ = orig
    assert small.sizes["time"] == 1
    # The coordinates were already materialized at load time, so every read
    # during the slice is field data (the on-disk field name may differ in
    # case from the xarray variable name).
    field_files = {f for (f, name), n in touched.items() if n > 1}
    assert len(field_files) == 1, (
        f"cmaq deferred single-step slice pulled gridded data from "
        f"{len(field_files)} of {len(_MULTIFILE)} files; a lazy slice must "
        f"read the field from only the needed file"
    )


# ---------------------------------------------------------------------------
# C-10 — unsupported options reported
# ---------------------------------------------------------------------------

_VZ_KWARGS = {
    "use_virtualizarr": True,
    "virtualizarr_file": "refs.json",
    "virtualizarr_backend": "kerchunk",
}


@pytest.mark.parametrize("source", ["cmaq", "tempo"])
def test_c10_gridded_options_not_silently_dropped(source, tmp_path):
    """C-10 (gridded): virtualizarr options are honored or raised — never ignored.

    The gridded driver either consumes the option (writing a reference file
    or opening through the virtual backend) or fails loudly when the
    machinery is unavailable. A silent no-op — returning the normal eager
    result with no reference written and no error — is the violation.
    """
    files, kwargs = _fixture_for(source)
    ref = tmp_path / "refs.json"
    try:
        monetio.load(source, files=files, virtualizarr_file=str(ref), **_VZ_KWARGS, **kwargs)
    except Exception as exc:  # noqa: BLE001 - loud failure is the acceptable outcome
        _loud = ValueError | ImportError | RuntimeError | TypeError | NotImplementedError
        assert "virtualizarr" in str(exc).lower() or isinstance(exc, _loud), (
            f"{source}: virtualizarr option failed with an unexplained error: {exc!r}"
        )
    else:
        assert ref.exists(), (
            f"{source}: virtualizarr option silently discarded — no reference "
            f"file written at {ref} and nothing raised"
        )


@pytest.mark.parametrize("source", ["aeronet", "airnow"])
def test_c10_point_virtualizarr_rejected(source, tmp_path, monkeypatch):
    """C-10 (point): virtualizarr kwargs on a tabular source are rejected by name.

    A point source has no chunked grid to virtualize against, so accepting the
    option and doing nothing with it lets callers believe they configured
    something real. The option is now refused before any read runs, naming both
    the option and the source. (Pandora is excluded: it subclasses the gridded
    GEOMS reader, where the option is honored rather than rejected.)
    """
    from monetio.readers.errors import OptionUnsupportedError

    if source == "airnow":
        files = _airnow_synthetic(tmp_path, monkeypatch)
    else:
        files, _ = _fixture_for(source)
    with pytest.raises(OptionUnsupportedError) as excinfo:
        monetio.load(source, files=files, virtualizarr_file=str(tmp_path / "refs.json"))
    msg = str(excinfo.value)
    assert "virtualizarr_file" in msg, f"the rejected option is not named: {msg}"
    assert source in msg, f"the source is not named: {msg}"


# ---------------------------------------------------------------------------
# C-11 — error visibility
# ---------------------------------------------------------------------------


def test_c11_corrupt_file_names_the_file(tmp_path):
    """C-11: a read failure names the specific file and preserves its type."""
    bad = tmp_path / "corrupt_cmaq.nc"
    bad.write_bytes(b"definitely not netCDF")
    with pytest.raises(Exception) as excinfo:
        monetio.load("cmaq", files=str(bad))
    msg = str(excinfo.value)
    assert "corrupt_cmaq.nc" in msg, f"error does not name the file: {msg[:200]}"
    assert not isinstance(excinfo.value, OSError) or type(excinfo.value) is not OSError, (
        "the original exception type was flattened into a generic OSError"
    )


def test_c11_missing_file_names_the_file(tmp_path):
    """C-11: a missing file raises with the path in the message."""
    missing = tmp_path / "nope_missing_xyz.nc"
    with pytest.raises(FileNotFoundError) as excinfo:
        monetio.load("cmaq", files=str(missing))
    assert "nope_missing_xyz" in str(excinfo.value)


# ---------------------------------------------------------------------------
# C-12 / C-13 — package-level consistency
# ---------------------------------------------------------------------------


def test_c12_registry_consistency():
    """C-12: available_sources() equals READER_REGISTRY after a full sweep."""
    sources = monetio.available_sources()
    for s in sources:
        _reader_class(s)
    assert set(sources) == set(READER_REGISTRY), (
        f"only-in-enumeration={set(sources) - set(READER_REGISTRY)} "
        f"only-in-registry={set(READER_REGISTRY) - set(sources)}"
    )


def test_c13_all_star_import_resolves():
    """C-13: every name in ``monetio.__all__`` resolves; star-import works."""
    unresolvable = [n for n in monetio.__all__ if not hasattr(monetio, n)]
    assert not unresolvable, f"monetio.__all__ names unresolvable: {unresolvable}"
    ns: dict = {}
    exec("from monetio import *", ns)  # noqa: S102 - the contract under test
    for n in monetio.__all__:
        assert n in ns or n == "__version__", f"star-import dropped {n}"


# ---------------------------------------------------------------------------
# C-CONF-5 — the suite has teeth: a non-conforming reader is caught
# ---------------------------------------------------------------------------


def test_suite_detects_nonconforming_reader(monkeypatch):
    """A reader violating exactly one rule produces one attributed failure.

    A throwaway source is registered (test-local, never shipped) whose load
    omits ``history``. Running the C-02 check against it must fail naming both
    the source and the rule — proving the suite catches violations rather than
    merely passing.
    """
    from monetio._manifest import MANIFEST_BY_NAME, ScaleCapabilities, SourceSpec
    from monetio.readers.base import register_reader

    spec = SourceSpec(
        name="badconf",
        module="tests.test_conformance",
        kind="gridded",
        capabilities=ScaleCapabilities(reason="test-local"),
    )
    monkeypatch.setitem(MANIFEST_BY_NAME, "badconf", spec)

    class BadConfReader(GriddedReader):
        """Reader that forgets provenance — violates the history rule."""

        def open_dataset(self, files=None, **kwargs):
            return xr.Dataset({"a": (("x",), np.arange(3.0))})  # no history attr

        def harmonize(self, ds):
            return ds

    register_reader("badconf")(BadConfReader)
    try:
        result = monetio.load("badconf", files=[])
        # The suite's C-02 check must flag it, naming source and rule.
        with pytest.raises(AssertionError) as excinfo:
            _assert_history("badconf", result)
        msg = str(excinfo.value)
        assert "badconf" in msg and "history" in msg
    finally:
        READER_REGISTRY.pop("badconf", None)
