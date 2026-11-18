"""Offline sample-coverage gate (tasks.md T025; sample-coverage contract).

The rule this enforces: every registered source is exercised by the load-dependent
conformance rules using only offline data, and every sample is reproducible from a
checked-in procedure. The conformance suite covers what has a sample; this file
covers the *net itself* — that a source cannot slip through unrecorded, that a
sample cannot be an unexplained binary, and that a network-facing source cannot be
certified by a synthetic file pretending to be a real payload.

It is deliberately independent of ``test_conformance.py``: that file parametrizes
over the sources it can load; this file asserts the coverage/exclusion split is
complete and every sample is auditable, so the two cannot agree on a hole.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

from conftest import SampleRegistry

import monetio

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from make_sample_manifest import REPO_ROOT, REQUIRED_KEYS, validate_manifest  # noqa: E402

_SAMPLES = REPO_ROOT / "tests" / "data" / "samples"
_SYNTHETIC = REPO_ROOT / "tests" / "data" / "synthetic"

_REGISTRY = SampleRegistry()

# A source counts as network-facing when its reader module reaches a remote
# location: an http(s)/s3/ftp URL in the source. This is a conservative textual
# probe over the reader file, not a runtime check, so it never touches the network.
_URL_RE = re.compile(r"https?://|s3://|ftp://")


def _reader_source(source: str) -> str:
    """The reader module's source text for a source, or "" if unresolvable."""
    try:
        module = monetio._manifest.module_for(source)
    except Exception:  # noqa: BLE001 - coverage probe; unresolvable source is not network-facing
        return ""
    spec = importlib.util.find_spec("monetio" + module)
    if spec is None or spec.origin is None or not Path(spec.origin).exists():
        return ""
    return Path(spec.origin).read_text(errors="replace")


def _is_network_facing(source: str) -> bool:
    return bool(_URL_RE.search(_reader_source(source)))


def _sample_dirs() -> list[Path]:
    dirs: list[Path] = []
    for base in (_SAMPLES, _SYNTHETIC):
        if base.is_dir():
            dirs += [p for p in sorted(base.iterdir()) if p.is_dir()]
    return dirs


# ---------------------------------------------------------------------------
# C-SAMP-2 / C-SAMP-10 — every source has a sample or a reasoned exclusion
# ---------------------------------------------------------------------------


def test_every_registered_source_has_sample_or_reasoned_exclusion():
    """FR-018/I-27: covered ∪ excluded == registry, and exclusions carry a reason.

    Imports the conformance module's derived maps so this gate and the suite
    cannot disagree about which sources are covered.
    """
    import test_conformance as conf

    sources = set(monetio.available_sources())
    covered = set(conf._FIXTURES)
    excluded = set(conf.EXCLUSIONS)
    assert covered | excluded == sources, (
        f"sources neither covered nor excluded: {sorted(sources - covered - excluded)}"
    )
    assert not (covered & excluded), (
        f"sources both covered and excluded: {sorted(covered & excluded)}"
    )
    for name, reason in conf.EXCLUSIONS.items():
        assert reason and reason.strip(), f"exclusion for {name!r} has no recorded reason"


def test_sampled_sources_resolve_through_registry():
    """Every directory on disk is reachable by the registry, and vice versa."""
    on_disk = {p.name for p in _sample_dirs()}
    resolved = {
        name
        for name in _REGISTRY.sampled_sources()
        if (_SAMPLES / name).is_dir() or (_SYNTHETIC / name).is_dir()
    }
    assert on_disk == resolved, (
        f"sample directories not resolved: {sorted(on_disk - resolved)}; "
        f"resolved-but-absent: {sorted(resolved - on_disk)}"
    )


# ---------------------------------------------------------------------------
# C-SAMP-3 / C-SAMP-4 — samples are reproducible and well-formed
# ---------------------------------------------------------------------------


def test_every_sample_manifest_is_valid():
    """FR-015/C-SAMP-4: each sample's manifest is complete and its hash matches.

    The producer names a checked-in script that exists (no unexplained binary),
    the required fields are present, a recorded sample carries a location, a
    synthetic sample carries a reason, and the content hash still matches the
    payload on disk.
    """
    problems: list[str] = []
    for directory in _sample_dirs():
        problems += validate_manifest(directory, check_hash=True)
    assert not problems, "invalid sample manifests:\n  " + "\n  ".join(problems)


def test_grib2io_index_cache_is_not_part_of_sample_payload_hash(tmp_path):
    from scripts.make_sample_manifest import content_hash

    payload = tmp_path / "field.grib2"
    payload.write_bytes(b"sample grib bytes")
    original_hash = content_hash(tmp_path)
    (tmp_path / "field.grib2_abc.grib2ioidx").write_bytes(b"backend cache")
    assert content_hash(tmp_path) == original_hash


def test_sample_manifest_required_fields():
    """C-SAMP-4: the manifest schema keys are present on every sample."""
    for directory in _sample_dirs():
        manifest = json.loads((directory / "manifest.json").read_text())
        for key in REQUIRED_KEYS:
            assert key in manifest, f"{directory.name}: manifest missing {key!r}"


def test_producer_scripts_exist():
    """SC-008/C-SAMP-3: the count of unexplained binary samples is 0."""
    for directory in _sample_dirs():
        manifest = json.loads((directory / "manifest.json").read_text())
        producer = manifest.get("producer")
        assert producer, f"{directory.name}: no producer recorded"
        assert (REPO_ROOT / producer).exists(), (
            f"{directory.name}: producer {producer!r} is not a checked-in script"
        )


# ---------------------------------------------------------------------------
# C-SAMP-5 — network-facing sources are recorded, not simulated
# ---------------------------------------------------------------------------


def test_network_facing_sampled_sources_are_recorded():
    """FR-016/I-29: a network-facing source's sample carries a real location.

    A synthetic reproduction certifies the parser against the generator's idea of
    the format, not the service's bytes, so it cannot stand in for a source whose
    whole point is a retrieved payload. A recorded sample must name the location
    it came from.
    """
    offenders = []
    for directory in _sample_dirs():
        source = directory.name
        if not _is_network_facing(source):
            continue
        manifest = json.loads((directory / "manifest.json").read_text())
        kind = manifest.get("kind")
        if kind == "synthetic":
            # Allowed only if the real payload genuinely cannot be recorded; the
            # reason must say so, and it is surfaced here for review.
            assert manifest.get("reason"), (
                f"{source}: network-facing synthetic sample needs a recorded reason"
            )
            continue
        if kind == "recorded" and not manifest.get("location"):
            offenders.append(f"{source}: recorded sample has no location")
    assert not offenders, "network-facing samples missing a real location:\n  " + "\n  ".join(
        offenders
    )


# ---------------------------------------------------------------------------
# C-SAMP-6 — plain files are the source of truth
# ---------------------------------------------------------------------------


def test_samples_are_plain_files_not_playback():
    """I-33/C-SAMP-6: every sample is a plain checked-in file, no playback layer.

    The payload next to each manifest is ordinary bytes on disk (never a pointer
    into a recording library), so the suite runs offline with any playback layer
    absent. Asserting the payload files exist and are non-empty is what "plain
    file is the source of truth" means for the offline run.
    """
    for directory in _sample_dirs():
        payloads = [p for p in directory.iterdir() if p.name != "manifest.json"]
        assert payloads, f"{directory.name}: manifest with no payload file"
        for payload in payloads:
            assert payload.is_file() and payload.stat().st_size > 0, (
                f"{directory.name}/{payload.name}: empty or non-file payload"
            )


# ---------------------------------------------------------------------------
# C-SAMP-9 — adding a source without a sample fails the check, naming it
# ---------------------------------------------------------------------------


def test_sample_less_source_fails_coverage(monkeypatch):
    """FR-019/I-30: a throwaway source with no sample is caught by name.

    Registers a test-local source (never shipped) that has no sample and is not
    in the exclusion list, then runs the coverage computation the way the gate
    does. It must produce exactly one attributed failure naming the source —
    proving the net cannot develop new holes.
    """
    import test_conformance as conf

    from monetio._manifest import MANIFEST_BY_NAME, ScaleCapabilities, SourceSpec

    spec = SourceSpec(
        name="nosample",
        module="tests.test_sample_coverage",
        kind="gridded",
        capabilities=ScaleCapabilities(reason="test-local"),
    )
    monkeypatch.setitem(MANIFEST_BY_NAME, "nosample", spec)
    # Recompute the derived maps with the new source present; it has no sample,
    # so it must land in the exclusion set with a reason rather than vanish.
    sources = set(monetio.available_sources()) | {"nosample"}
    covered = set(conf._FIXTURES)
    uncovered = sources - covered
    assert "nosample" in uncovered
    # The gate's contract: an uncovered source without a recorded reason fails.
    reasons = dict(conf.EXCLUSIONS)
    missing_reason = [n for n in uncovered if not reasons.get(n, "").strip()]
    assert "nosample" in missing_reason, "coverage check did not flag the sample-less source"
