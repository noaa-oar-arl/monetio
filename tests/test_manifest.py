"""Manifest / single-source-of-truth invariants (tasks.md T009).

Covers data-model.md invariants I-1..I-6 and contracts/source-manifest.md
C-MAN-1..C-MAN-6. Satisfies FR-011..FR-015, SC-005, SC-006, SC-009.

The public-API tests (``available_sources`` / ``source_info`` /
``source_capabilities`` and the throwaway-source fixture) are expected to FAIL
until T012 exposes that surface on the package — this is the TDD red state.
The structural tests (uniqueness, resolvability, derived-mapping equality)
validate ``monetio._manifest`` directly and pass now.
"""

from __future__ import annotations

import importlib
import re
import subprocess
import sys

import numpy as np
import pytest
import xarray as xr

import monetio
from monetio import _manifest as manifest
from monetio.readers.base import READER_REGISTRY, GriddedReader, PointReader

# --- I-1 uniqueness --------------------------------------------------------


def test_canonical_names_unique():
    names = [s.name for s in manifest.SOURCE_MANIFEST]
    assert len(names) == len(set(names))


def test_aliases_unique_and_disjoint_from_canonical():
    aliases = [a for s in manifest.SOURCE_MANIFEST for a in s.aliases]
    names = {s.name for s in manifest.SOURCE_MANIFEST}
    assert len(aliases) == len(set(aliases)), "a name is claimed as an alias twice"
    assert not (set(aliases) & names), "an alias collides with a canonical name"


def test_alias_map_matches_specs():
    # every alias resolves to the spec that declares it
    for s in manifest.SOURCE_MANIFEST:
        for a in s.aliases:
            assert manifest.resolve(a) is s


def test_legacy_paths_are_importable():
    """I-2: every declared ``legacy_paths`` entry must import (C-MAN-2).

    A phantom path (e.g. a facade module that does not exist) would make the
    corresponding top-level attribute silently fall through to the reader,
    diverging from today's facade binding — so it is a manifest bug.
    """
    for s in manifest.SOURCE_MANIFEST:
        for path in s.legacy_paths:
            try:
                importlib.import_module(f"monetio.{path}")
            except ImportError as exc:  # pragma: no cover - the assertion is the point
                pytest.fail(
                    f"source {s.name!r} declares legacy path {path!r} which does not import: {exc}"
                )


# --- I-5 import-freeness (subprocess, honest isolation) -------------------


def test_manifest_import_pulls_no_readers_or_heavy_deps():
    """Importing the manifest alone must not import readers or heavy deps.

    Loads ``monetio/_manifest.py`` as a standalone module (bypassing the
    package ``__init__``) so this measures the manifest's own import graph,
    independent of the still-eager package root pre-T012.
    """
    code = (
        "import importlib.util, sys\n"
        "spec = importlib.util.spec_from_file_location('_manifest', 'monetio/_manifest.py')\n"
        "m = importlib.util.module_from_spec(spec); sys.modules['_manifest'] = m\n"
        "spec.loader.exec_module(m)\n"
        "bad = [x for x in sys.modules if x.startswith('monetio.readers')\n"
        "       or x.split('.')[0] in {'numpy','pandas','xarray','dask','fsspec'}]\n"
        "print(bad)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-W", "ignore", "-c", code],
        capture_output=True,
        text=True,
        cwd=monetio.__path__[0] + "/..",
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "[]", f"manifest pulled: {proc.stdout.strip()}"


# --- derived view equals today's hand-maintained index (C-MAN-4) ----------


def test_derived_reader_modules_equals_legacy_index():
    derived = manifest.build_reader_modules()
    legacy = dict(monetio._READER_MODULES)
    assert derived == legacy, "derived manifest view diverges from _READER_MODULES"


# --- I-2 resolvability + I-3 no drift + I-4 kind agreement ----------------


def test_full_import_sweep_populates_registry_identically():
    """After importing every declared module, registry == manifest names (I-3)."""
    for s in manifest.SOURCE_MANIFEST:
        importlib.import_module(s.module, package="monetio")
    swept = set(READER_REGISTRY)
    assert swept == set(manifest.all_source_names()), (
        f"drift: only-in-registry={swept - set(manifest.all_source_names())} "
        f"only-in-manifest={set(manifest.all_source_names()) - swept}"
    )


def test_kind_agrees_with_base_class():
    for s in manifest.SOURCE_MANIFEST:
        cls = READER_REGISTRY[s.name]
        expected = GriddedReader if s.kind == "gridded" else PointReader
        assert issubclass(cls, expected), f"{s.name}: kind={s.kind} but base != {expected.__name__}"


# --- C-MAN-6 error contracts (manifest-level, work now) -------------------


def test_resolve_unknown_raises_keyerror():
    with pytest.raises(KeyError):
        manifest.resolve("definitely-not-a-source")


def test_duplicate_registration_raises_naming_both(tmp_path, monkeypatch):
    """A second class claiming an existing name must raise (FR-015)."""
    from monetio.readers.base import register_reader

    existing = READER_REGISTRY["cmaq"]

    class Impostor(GriddedReader):  # same kind so it reaches the dup check
        def open_dataset(self, files=None, **kwargs):  # pragma: no cover
            raise NotImplementedError

    with pytest.raises(ValueError, match="Duplicate registration"):
        register_reader("cmaq")(Impostor)

    # registry unchanged after the rejected attempt
    assert READER_REGISTRY["cmaq"] is existing


def test_register_undeclared_name_raises():
    from monetio.readers.base import register_reader

    class Rogue(PointReader):
        def open_dataset(self, files=None, **kwargs):  # pragma: no cover
            raise NotImplementedError

    with pytest.raises(ValueError, match="not declared"):
        register_reader("rogue_source_not_in_manifest")(Rogue)


# --- C-MAN-1 public enumeration API (RED until T012) ----------------------


def test_available_sources_matches_registry():
    avail = set(monetio.available_sources())
    assert avail == set(manifest.all_source_names())


def test_source_info_imports_nothing(run_in_fresh_python):
    """source_info must answer from the manifest without importing the reader.

    Runs in a fresh interpreter (C-LAZY-7): in-process, an earlier test's
    ``import`` would already populate ``sys.modules`` and the check would be
    meaningless (and order-dependent).
    """
    result = run_in_fresh_python(
        "spec = monetio.source_info('cmaq')\n"
        "print(json.dumps({"
        "'name': spec.name, 'module': spec.module, "
        "'imported': 'monetio.readers.cmaq' in sys.modules}))"
    )
    assert result["name"] == "cmaq"
    assert result["module"] == ".readers.cmaq"
    assert result["imported"] is False


def test_source_info_rejects_unknown_with_list():
    with pytest.raises(ValueError, match="Available"):
        monetio.source_info("nope-not-real")


def test_load_rejects_unknown_with_same_error_as_source_info():
    # The package's view of "what can be loaded" and the error it reports must
    # come from the same place, so a bad name fails consistently whichever
    # entry point is used (ValueError naming the source + the available list),
    # never a bare KeyError leaking out of the manifest lookup.
    with pytest.raises(ValueError, match="Available") as load_exc:
        monetio.load("nope-not-real", files="x.nc")
    with pytest.raises(ValueError, match="Available") as info_exc:
        monetio.source_info("nope-not-real")
    assert str(load_exc.value) == str(info_exc.value)


def test_source_capabilities_shape():
    caps = monetio.source_capabilities("gfs")
    assert hasattr(caps, "deferred")
    assert hasattr(caps, "parallel_metadata")


def test_alias_source_info_and_capabilities():
    assert monetio.source_info("gefs").name == "gfs"
    assert monetio.source_info("gdas").name == "gfs"
    assert monetio.source_info("viirs_jrr").name == "nesdis_viirs_jrr"


# --- C-MAN-5 / SC-006: a source is addable without editing the root -------


def test_dummy_source_addable_via_manifest_only(monkeypatch):
    """Registering a throwaway source in the manifest makes it loadable and
    enumerable with NO edit to ``__init__.py`` or ``cli.py`` (SC-006)."""
    from monetio.readers.base import PointReader, register_reader

    # 1. the single declaration: inject one SourceSpec into the manifest FIRST,
    #    because register_reader validates against it (C-MAN-3 step 1).
    spec = manifest.SourceSpec(
        name="dummyxyz",
        module="tests.test_manifest",  # self-referential; already imported
        kind="point",
        capabilities=manifest.ScaleCapabilities(reason="dummy"),
    )
    monkeypatch.setitem(manifest.MANIFEST_BY_NAME, "dummyxyz", spec)

    # 2. a reader class that self-registers (as a real reader module would)
    class DummyReader(PointReader):
        def open_dataset(self, files=None, **kwargs):
            import pandas as pd

            return pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    register_reader("dummyxyz")(DummyReader)

    try:
        assert "dummyxyz" in monetio.available_sources()
        assert monetio.source_info("dummyxyz").name == "dummyxyz"
        df = monetio.load("dummyxyz")
        assert list(df.columns) == ["a", "b"]
    finally:
        READER_REGISTRY.pop("dummyxyz", None)


# --- alias equivalence (US3 scenario 3, FR-014) ---------------------------

_TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}: ", flags=re.MULTILINE)


def _normalize_history(ds):
    """Return a copy whose ``history`` drops the per-run timestamp prefix and
    the concrete reader-class name — the two fields that legitimately differ
    between runs and between sibling readers in one module. Everything else
    (data, coords, all other attrs) is left intact for an exact comparison."""
    ds = ds.copy()
    hist = ds.attrs.get("history", "")
    hist = _TS.sub("", hist)
    hist = re.sub(r"Read (GFS|GEFS|GDAS)Reader", "Read <NCEP>Reader", hist)
    ds.attrs["history"] = hist
    return ds


def test_alias_declarations_resolve_to_same_module():
    """Every alias names the same providing module as its canonical source,
    and ``source_info`` resolves both to the same declaration (FR-014)."""
    gfs = monetio.source_info("gfs")
    for alias in ("gefs", "gdas"):
        assert monetio.available_sources().count(alias) == 1
        assert monetio.source_info(alias).name == gfs.name
        assert monetio.source_info(alias).module == gfs.module

    jrr = monetio.source_info("nesdis_viirs_jrr")
    assert monetio.source_info("viirs_jrr").name == jrr.name
    assert monetio.source_info("viirs_jrr").module == jrr.module


def test_alias_shares_reader_class():
    """``viirs_jrr`` is a second registration on the *same* class as
    ``nesdis_viirs_jrr`` (the dual-decorator case): both names resolve to one
    identical class object."""
    from monetio import _lazy

    assert _lazy.get_reader_class("viirs_jrr") is _lazy.get_reader_class("nesdis_viirs_jrr")


def test_gfs_family_shares_module_distinct_classes():
    """The GFS family is the multi-class case: ``gfs``/``gefs``/``gdas`` all
    come from one module but each registers its own class, so load() routes
    each name to that name's reader (and they differ only in URL building, not
    in the data produced for a given file)."""
    from monetio import _lazy

    classes = {n: _lazy.get_reader_class(n) for n in ("gfs", "gefs", "gdas")}
    assert len(set(classes.values())) == 3, "expected three distinct reader classes"
    assert {c.__module__ for c in classes.values()} == {"monetio.readers.gfs"}


def test_alias_load_returns_identical_data_gfs(sample_registry):
    """Loading the same fixture through each GFS-family alias returns
    equivalent data (US3 scenario 3).

    Uses the recorded GFS GRIB2 sample: these readers are GRIB2-only and reject
    any other engine, so a netCDF stub cannot exercise them. Resolving through
    the registry skips the check when the optional ``grib2io`` engine is not
    installed, rather than failing on it.
    """
    sample = sample_registry.require("gfs")

    results = [
        monetio.load(name, files=sample["files"], **sample["kwargs"])
        for name in ("gfs", "gefs", "gdas")
    ]
    base = _normalize_history(results[0])
    for other in results[1:]:
        xr.testing.assert_identical(base, _normalize_history(other))


def test_alias_load_returns_identical_data_viirs_jrr(tmp_path):
    """``viirs_jrr`` and ``nesdis_viirs_jrr`` load the same fixture to identical
    datasets, including the identical reader class provenance in history."""
    ds = xr.Dataset(
        {
            "AOD550": (("y", "x"), np.linspace(0.1, 0.9, 6).reshape(2, 3)),
            "AOD_Quality_Flag": (("y", "x"), np.zeros((2, 3), dtype="int32")),
            "Latitude": (("y", "x"), np.full((2, 3), 40.0)),
            "Longitude": (("y", "x"), np.full((2, 3), -100.0)),
        },
        attrs={"time_coverage_start": "2024-01-01T00:00:00Z"},
    )
    path = str(tmp_path / "jrr_stub.nc")
    ds.to_netcdf(path)

    a = _normalize_history(monetio.load("viirs_jrr", files=path, product="AOD"))
    b = _normalize_history(monetio.load("nesdis_viirs_jrr", files=path, product="AOD"))
    xr.testing.assert_identical(a, b)
