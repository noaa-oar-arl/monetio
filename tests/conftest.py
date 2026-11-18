import json
import os
import pickle
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_DATA = Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def is_ci():
    return os.environ.get("CI", "false").lower() in {"true", "yes", "1", ""}


@pytest.fixture
def data_dir() -> Path:
    return _DATA


@pytest.fixture
def printer():
    return print


# ---------------------------------------------------------------------------
# Baseline artifacts (recorded by scripts/record_import_baseline.py and
# scripts/record_bc_baseline.py BEFORE the refactor). See tasks.md T001/T002.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def baseline_import() -> dict:
    """Parsed ``tests/data/baseline_import.json`` (C-LAZY-7, SC-001/002/003)."""
    path = _DATA / "baseline_import.json"
    if not path.exists():
        pytest.skip(f"import baseline missing: run scripts/record_import_baseline.py ({path})")
    return json.loads(path.read_text())


@pytest.fixture(scope="session")
def baseline_dir() -> Path:
    """Directory holding the per-source BC golden references (SC-019)."""
    path = _DATA / "baseline"
    if not (path / "index.json").exists():
        pytest.skip(f"BC baseline missing: run scripts/record_bc_baseline.py ({path})")
    return path


@pytest.fixture(scope="session")
def bc_calls() -> dict:
    """The ``(source -> load kwargs)`` table used to build the goldens."""
    path = _DATA / "baseline" / "calls.json"
    if not path.exists():
        pytest.skip(f"BC calls missing: run scripts/record_bc_baseline.py ({path})")
    return json.loads(path.read_text())


@pytest.fixture(scope="session")
def load_baseline_factory(baseline_dir):
    """Return ``load(source)`` giving the timestamp-normalized golden object.

    Kept as a factory so individual tests can request just the sources they
    parametrize over. Used by tests/test_bc_baseline.py (SC-019).
    """

    def _load(source: str):
        path = baseline_dir / f"{source}.pkl"
        if not path.exists():
            pytest.skip(f"no golden reference for source {source!r}")
        return pickle.loads(path.read_bytes())

    return _load


# ---------------------------------------------------------------------------
# Fresh-interpreter subprocess helper (C-LAZY-7). In-process import timing is
# invalid: a second ``import monetio`` is a dict lookup.
# ---------------------------------------------------------------------------


@pytest.fixture
def run_in_fresh_python():
    """Run ``code`` in a fresh interpreter that has imported ``monetio``.

    The child prints ONE line of JSON (``print(json.dumps(...))``); this
    returns that object. ``sys.modules`` is exposed to the code as ``mods``
    and, when ``time_import=True``, the measured import wall-time as
    ``import_seconds``. Used by tests/test_lazy_import.py (SC-001/002/003).
    """

    def _run(code: str, *, time_import: bool = False, timeout: int = 120) -> dict:
        if time_import:
            prelude = (
                "import json, sys, time\n"
                "_t0 = time.perf_counter()\n"
                "import monetio\n"
                "import_seconds = time.perf_counter() - _t0\n"
            )
        else:
            prelude = "import json, sys\nimport monetio\nimport_seconds = None\n"
        child = prelude + "mods = list(sys.modules)\n" + code
        proc = subprocess.run(
            [sys.executable, "-W", "ignore", "-c", child],
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
            timeout=timeout,
        )
        if proc.returncode != 0:
            raise AssertionError(
                f"fresh-interpreter run failed ({proc.returncode}):\n{proc.stderr}"
            )
        return json.loads(proc.stdout.strip().splitlines()[-1])

    return _run


# ---------------------------------------------------------------------------
# In-process distributed cluster (C-SCALE-8, SC-015). Skips when `distributed`
# is not installed so the suite stays runnable without the optional extra.
# ---------------------------------------------------------------------------


@pytest.fixture
def local_distributed_cluster():
    distributed = pytest.importorskip("distributed")
    from distributed import LocalCluster

    with LocalCluster(
        n_workers=2,
        processes=False,  # in-process: CI-friendly, no spawn/pickle of user code
        dashboard_address=None,
        silence_logs=50,  # CRITICAL+ -> quiet
    ) as cluster:
        with distributed.Client(cluster) as client:
            yield client


# ---------------------------------------------------------------------------
# Offline sample resolution (Phase 2 Stage 1, T007).
#
# Contract `sample-coverage.md` C-SAMP-4 puts every sample at
#
#     tests/data/samples/<source>/manifest.json   + payload files
#     tests/data/synthetic/<source>/manifest.json + generated files
#
# The 12 sources sampled during Phase 1 predate that layout: their inputs are
# loose files under ``tests/data/`` (and several are gitignored binaries). They
# are mapped here rather than moved, so US1 failure-policy tests and the US2
# coverage gate resolve *every* source through one path. A source is
# "sampled" when either its new-layout directory exists or it appears in the
# legacy map.
# ---------------------------------------------------------------------------

_SAMPLES = _DATA / "samples"
_SYNTHETIC = _DATA / "synthetic"

# Legacy Phase 1 fixtures: source -> (files, load kwargs). ``files`` may be a
# glob, a single path, or a list; all are repo-root relative. Kept in sync with
# ``tests/test_conformance.py::_FIXTURES`` — the coverage test asserts the two
# agree so neither can silently drop a source.
_LEGACY_FIXTURES = {
    "cmaq": ("benchmark_cmaq.nc", {}),
    "chimere": ("tests/data/chimere_test.nc", {}),
    "hytraj": (["tests/data/tdump.1", "tests/data/tdump.2"], {}),
    "aeronet": (
        [
            "tests/data/aeronet-AOD15-example.txt",
            "tests/data/aeronet-inv-ALM15-SIZ-example.txt",
        ],
        {},
    ),
    "pandora": ("tests/data/pandora-uvvis-no2-boulder-20231206.h5", {}),
    # The Phase 1 HDF4 fixture could never exercise this reader: the TOLNet
    # reader opens the HDF5 ``DATA`` group, which an HDF4 file does not have,
    # so every load-dependent rule passed vacuously against an empty Dataset.
    # This small HDF5 file (DATA + INSTRUMENT_ATTRIBUTES groups) does.
    "tolnet": ("tests/data/tolnet-hdf5-sample.h5", {}),
    "tempo": ("tests/data/sample_TEMPO_NO2_L2_V03_20240826T204005Z_S012G01.nc", {}),
    "ufs": ("tests/data/ufs/aqm.t12z.dyn.f00*.nc", {}),
    "omps": ("tests/data/OMPS-TO3-L3-DAILY_v2.1_20190905.h5", {}),
    "omps_nadir": ("tests/data/OMPS-TO3-L3-DAILY_v2.1_20190905.h5", {}),
    # MOPITT's variables live in a nested HDF-EOS5 group; without it the file
    # opens as an empty Dataset and every load-dependent rule passes vacuously.
    "mopitt": (
        "tests/data/MOP03JM-201701-L3V95.9.3.he5",
        {"group": "HDFEOS/GRIDS/MOP03/Data Fields"},
    ),
}

# Manifest keys required by C-SAMP-4 / data-model §7.
_MANIFEST_KEYS = {"location", "captured", "content_hash", "producer", "reason"}


def _skip_if_unmet_requirement(source, found):
    """Skip a sample whose optional decode package is not importable.

    A manifest's ``requires`` field names a package (typically an xarray decode
    engine such as ``grib2io``) that is not a core dependency. When the package
    is absent, the load-dependent conformance rules for that source are skipped
    with a stated reason rather than failing on an uninstalled optional
    dependency, so the suite runs green in an environment that lacks the engine
    while still exercising every rule in one that has it.
    """
    module = found.get("requires")
    if not module:
        return
    import importlib.util

    if importlib.util.find_spec(module) is None:
        pytest.skip(f"{source}: sample requires {module!r} (not installed)")


def _expand_files(files):
    """Resolve repo-root-relative paths/globs to concrete absolute paths."""
    if isinstance(files, str) and "*" in files:
        return sorted(str(p) for p in _REPO_ROOT.glob(files))
    if isinstance(files, list):
        return [str(_REPO_ROOT / f) for f in files]
    return str(_REPO_ROOT / files)


def _read_sample_dir(directory):
    """Return a resolved sample dict for one C-SAMP-4 directory, or None."""
    manifest_path = directory / "manifest.json"
    if not manifest_path.exists():
        return None
    manifest = json.loads(manifest_path.read_text())
    # A GRIB2 decode through the grib2io engine drops a binary index cache next
    # to the payload (``<file>.grib2ioidx`` / ``<file>.<hash>.grib2ioidx``, and a
    # wgrib2 ``.idx`` if one is present). Those are derived caches, not part of
    # the recorded payload; counting them would turn a cache file into a second
    # input on the next load. The manifest is likewise metadata, never payload.
    payload = sorted(
        str(p)
        for p in directory.iterdir()
        if p.name != "manifest.json" and not p.name.endswith((".grib2ioidx", ".idx"))
    )
    if not payload:
        return None
    load_kwargs = manifest.get("load_kwargs", {})
    files = payload if len(payload) > 1 else payload[0]
    return {
        "kind": manifest.get("kind", "recorded"),
        "files": files,
        "kwargs": dict(load_kwargs),
        "manifest": manifest,
        "layout": manifest.get("layout"),
        "requires": manifest.get("requires"),
        "directory": str(directory),
    }


class SampleRegistry:
    """Resolution and coverage queries over the offline sample set.

    Used by ``tests/test_sample_coverage.py`` (the coverage gate) and
    ``tests/test_failure_policy.py`` (per-source corrupt/missing input tests).
    """

    def __init__(self):
        self._cache = {}

    def _resolve(self, source):
        if source in self._cache:
            return self._cache[source]
        found = None
        for base, kind in ((_SAMPLES, "samples"), (_SYNTHETIC, "synthetic")):
            candidate = _read_sample_dir(base / source)
            if candidate is not None:
                candidate["kind"] = candidate.get("kind") or kind
                found = candidate
                break
        if found is None and source in _LEGACY_FIXTURES:
            files, kwargs = _LEGACY_FIXTURES[source]
            found = {
                "kind": "legacy",
                "files": _expand_files(files),
                "kwargs": dict(kwargs),
                "manifest": None,
                "layout": None,
                "directory": None,
            }
        self._cache[source] = found
        return found

    def sampled_sources(self):
        """Every source with a sample in either layout or the legacy map."""
        names = set(_LEGACY_FIXTURES)
        for base in (_SAMPLES, _SYNTHETIC):
            if base.is_dir():
                names |= {p.name for p in base.iterdir() if (p / "manifest.json").exists()}
        return names

    def resolve(self, source):
        """Return the resolved sample dict, or None when the source is unsampled."""
        return self._resolve(source)

    def requires(self, source):
        """The optional package a source's sample needs to load (``None`` if none)."""
        found = self._resolve(source)
        return found.get("requires") if found else None

    def require(self, source):
        """Resolve or skip with the recorded-exclusion message."""
        found = self._resolve(source)
        if found is None:
            pytest.skip(f"{source}: no offline sample (recorded conformance exclusion)")
        _skip_if_unmet_requirement(source, found)
        if isinstance(found["files"], list) and not found["files"]:
            pytest.skip(f"{source}: sample glob matched nothing")
        if isinstance(found["files"], str) and not Path(found["files"]).exists():
            pytest.skip(f"{source}: sample payload missing: {found['files']}")
        return found

    def load(self, source, **overrides):
        """Load a source through its sample, honoring the sample's own kwargs."""
        import monetio

        found = self.require(source)
        kwargs = dict(found["kwargs"])
        kwargs.update(overrides)
        return monetio.load(source, files=found["files"], **kwargs)


@pytest.fixture(scope="session")
def sample_registry() -> SampleRegistry:
    return SampleRegistry()


@pytest.fixture
def sample(sample_registry):
    """``sample(source)`` -> resolved sample dict (skips when unsampled)."""
    return sample_registry.require


@pytest.fixture(scope="session")
def legacy_fixtures() -> dict:
    """The Phase 1 fixture map, exposed so the coverage test can assert agreement."""
    return dict(_LEGACY_FIXTURES)


@pytest.fixture(scope="session")
def manifest_keys() -> set:
    """Required manifest fields (C-SAMP-4)."""
    return set(_MANIFEST_KEYS)


@pytest.fixture(scope="session")
def multifile(tmp_path_factory):
    """Generate the gridded multi-file dataset once per test session."""
    import runpy

    build_file = runpy.run_path(str(_REPO_ROOT / "scripts" / "make_multifile_fixture.py"))[
        "build_file"
    ]
    outdir = tmp_path_factory.mktemp("multifile")
    return [str(build_file(i, "2023-06-01 00:00", outdir)) for i in range(60)]
