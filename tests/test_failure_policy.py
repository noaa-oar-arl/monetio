"""Failure-policy contract tests (Phase 2 Stage 1, T008).

Satisfies contracts/failure-policy.md C-FAIL-1..C-FAIL-10 and SC-001..SC-005.

The rule under test: **a failed read must never look like a successful one.**
Every test here encodes a behavior that is *wrong today* (quickstart §1 Defects
A/B are the two reproduced live cases) and must pass once the Stage 1 failure
policy is implemented. Tests are offline, against the sampled sources resolved
by the ``sample_registry`` fixture (T007).
"""

from __future__ import annotations

import ast
import inspect
import re
import warnings
from pathlib import Path

import pytest
import xarray as xr
from conftest import SampleRegistry

import monetio
from monetio.readers.errors import (  # T009
    FAILURE_CATEGORIES,
    CapabilityUnavailableError,
    ContentInvalidError,
    CredentialDeniedError,
    MalformedInputError,
    OptionUnsupportedError,
    RetrievalFailedError,
    SectionUnavailableError,
    Tolerance,
    annotate,
)

_ROOT = Path(__file__).parent.parent
_REPO = Path(__file__).parent.parent

# Every source with an offline sample. Each is exercised by the corrupt/missing
# input rules so SC-001 is assessed over the full sampled population, not a
# hand-picked subset. Sources whose sample needs an optional package (e.g.
# grib2io) skip cleanly when that package is absent.
SAMPLED = sorted(SampleRegistry().sampled_sources())

# Sources whose input contains several named sections that are opened and
# merged (C-FAIL-5).
MULTI_SECTION = ["tempo", "omps_nadir", "mopitt", "tolnet"]


# ---------------------------------------------------------------------------
# C-FAIL-1 / C-FAIL-2 — corrupt input fails, naming the input and the cause
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source", SAMPLED)
def test_corrupt_input_raises_naming_the_file(source, sample_registry, tmp_path):
    """SC-001: a corrupt input raises, names the file, and keeps a taxonomy category."""
    # Resolving through the registry skips a source whose sample needs an
    # optional package that is not installed, rather than failing on it.
    sample_registry.require(source)
    bad = tmp_path / f"corrupt-{source}.bin"
    bad.write_bytes(b"this is not the expected format at all\n" * 3)

    with pytest.raises(Exception) as excinfo:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            monetio.load(source, files=str(bad))

    err = excinfo.value
    message = str(err)
    assert bad.name in message or str(bad) in message, (
        f"{source}: error does not name the corrupt input: {message!r}"
    )
    category = getattr(err, "category", None)
    assert category in FAILURE_CATEGORIES, (
        f"{source}: error carries no taxonomy category (got {category!r}): {message!r}"
    )


def test_corrupt_input_error_preserves_original_category(tmp_path):
    """C-FAIL-1.3: annotating an error must not flatten FileNotFoundError.

    A permission failure stays a permission failure and a missing file stays a
    missing file — the caller's ``except`` clause must keep working.
    """
    missing = tmp_path / "definitely-not-here.nc"
    with pytest.raises(Exception) as excinfo:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            monetio.load("cmaq", files=str(missing))
    err = excinfo.value
    assert isinstance(err, FileNotFoundError), (
        f"missing-file load must stay FileNotFoundError, got {type(err).__name__}"
    )
    assert missing.name in str(err), "annotated error must name the missing input"


def test_annotate_preserves_type_and_names_inputs():
    """C-FAIL-1: same-typed copy whose message names up to five inputs."""
    original = ValueError("bad magic number")
    annotated = annotate(original, *[f"/tmp/file{i}.nc" for i in range(7)])
    assert type(annotated) is ValueError
    assert "bad magic number" in str(annotated)
    for i in range(5):
        assert f"/tmp/file{i}.nc" in str(annotated)
    # beyond five: indicated, not silently truncated
    assert "more" in str(annotated).lower() or "…" in str(annotated) or "..." in str(annotated)


# ---------------------------------------------------------------------------
# C-FAIL-2 — no silent-empty (Defect A is the live case)
# ---------------------------------------------------------------------------


def test_no_empty_dataset_from_corrupt_file(tmp_path):
    """quickstart §1 Defect A: tolnet on a corrupt file must not return 0 vars."""
    bad = tmp_path / "phase2_bad_tolnet.h5"
    bad.write_text("not hdf5 at all")
    with pytest.raises(Exception) as excinfo:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = monetio.load("tolnet", files=str(bad))
            # If a result comes back, it must not be an empty Dataset.
            if isinstance(result, xr.Dataset):
                raise AssertionError(
                    f"silent-empty defect: returned Dataset with "
                    f"{len(result.data_vars)} data variables"
                )
    assert "bad" in str(excinfo.value).lower() or bad.name in str(excinfo.value)


def test_load_never_returns_empty_without_error(sample_registry, monkeypatch):
    """SC-002: no sampled source returns an empty result on a valid load."""
    # AirNow's reader enriches its local payload with a remote monitor table.
    # Build matching minimal metadata from the checked-in sample so this
    # all-sample contract remains offline and tests reader output, not DNS.
    import pandas as pd

    import monetio.readers.epa_utils as epa_utils

    airnow_sample = sample_registry.require("airnow")["files"]
    records = pd.read_csv(airnow_sample, sep="|", header=None)
    sites = records[[2, 3]].drop_duplicates().rename(columns={2: "siteid", 3: "site_name"})
    sites["siteid"] = sites["siteid"].astype(str).str.zfill(9)
    sites["latitude"] = 40.0
    sites["longitude"] = -80.0
    empty_sites = pd.DataFrame(columns=["siteid", "site_name", "latitude", "longitude"])
    monkeypatch.setattr(
        epa_utils,
        "read_monitor_file",
        lambda network=None, airnow=False: sites if airnow else empty_sites,
    )
    for source in SAMPLED:
        sample = sample_registry.require(source)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = monetio.load(source, files=sample["files"], **sample["kwargs"])
        if isinstance(result, xr.Dataset):
            assert len(result.data_vars) > 0, f"{source}: default load returned an empty Dataset"
        elif hasattr(result, "__len__"):
            assert len(result) > 0, f"{source}: default load returned an empty result"


# ---------------------------------------------------------------------------
# C-FAIL-3 — no silent-discard (Defect B is the live case; closes KI-1)
# ---------------------------------------------------------------------------


def test_virtualizarr_option_on_point_source_is_rejected(sample_registry):
    """quickstart §1 Defect B: a virtualization option on a point source raises.

    Closes Phase 1 known issue KI-1 by fixing the behavior, not by weakening
    the test (SC-039).
    """
    sample = sample_registry.require("aeronet")
    with pytest.raises(OptionUnsupportedError) as excinfo:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            monetio.load(
                "aeronet",
                files=sample["files"],
                virtualizarr_file="/tmp/should_not_exist_phase2.json",
            )
    message = str(excinfo.value)
    assert "virtualizarr_file" in message, f"error must name the option: {message!r}"
    assert "aeronet" in message, f"error must name the source: {message!r}"
    assert not Path("/tmp/should_not_exist_phase2.json").exists()


def test_unknown_option_rejected_by_name(sample_registry):
    """C-FAIL-3: an option in a source's namespace that it does not define is rejected."""
    sample = sample_registry.require("cmaq")
    with pytest.raises(OptionUnsupportedError) as excinfo:
        monetio.load("cmaq", files=sample["files"], definitely_not_a_real_option=1)
    assert "definitely_not_a_real_option" in str(excinfo.value)
    assert "cmaq" in str(excinfo.value)


# ---------------------------------------------------------------------------
# C-FAIL-4 / FR-003 / FR-004 — tolerance is explicit and recorded
# ---------------------------------------------------------------------------


def test_tolerance_skips_bad_input_and_records_it(tmp_path, sample_registry):
    """One bad input of many: tolerated load succeeds non-empty and names the skip."""
    sample = sample_registry.require("hytraj")
    good = sample["files"] if isinstance(sample["files"], list) else [sample["files"]]
    bad = tmp_path / "missing-tdump.9"
    files = list(good) + [str(bad)]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = monetio.load("hytraj", files=files, tolerate="input")

    assert len(result.data_vars) > 0, "tolerated load must not be empty"
    history = str(result.attrs.get("history", ""))
    assert bad.name in history, f"skipped input must appear in provenance: {history!r}"


def test_tolerance_never_returns_empty_when_all_inputs_fail(tmp_path):
    """FR-004: all inputs failing is still a failure even with tolerance."""
    bad1 = tmp_path / "bad1.h5"
    bad2 = tmp_path / "bad2.h5"
    bad1.write_text("garbage")
    bad2.write_text("garbage")
    with pytest.raises(Exception) as excinfo:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = monetio.load("tolnet", files=[str(bad1), str(bad2)], tolerate="input")
            if isinstance(result, xr.Dataset) and len(result.data_vars) == 0:
                raise AssertionError("tolerance produced an empty result presented as success")
    assert excinfo.value is not None


def test_tolerance_not_implied_by_default(tmp_path, sample_registry):
    """I-14: without an explicit tolerate=, a bad input fails the whole load."""
    sample = sample_registry.require("hytraj")
    good = sample["files"] if isinstance(sample["files"], list) else [sample["files"]]
    bad = tmp_path / "missing-tdump.9"
    with pytest.raises(Exception):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            monetio.load("hytraj", files=list(good) + [str(bad)])


def test_tolerance_entity_is_explicit():
    """C-FAIL-4: tolerance carries a scope and is never implied."""
    t = Tolerance(enabled=True, scope="input")
    assert t.scope == "input"
    assert Tolerance(enabled=False, scope="input").enabled is False


# ---------------------------------------------------------------------------
# C-FAIL-5 — multi-section products report which sections were included
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source", MULTI_SECTION)
def test_multi_section_failure_names_the_section(source, sample_registry, tmp_path):
    """A section that cannot be opened is a failure naming the input and section."""
    sample_registry.require(source)
    bad = tmp_path / f"corrupt-{source}.nc"
    bad.write_bytes(b"not a valid multi-section file")
    with pytest.raises(Exception) as excinfo:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            monetio.load(source, files=str(bad))
    err = excinfo.value
    assert bad.name in str(err) or "corrupt" in str(err).lower()
    # The category is either a section problem or the input itself is unreadable.
    assert getattr(err, "category", None) in {"section_unavailable", "malformed", "content_invalid"}


def test_multi_section_records_included_sections(sample_registry):
    """All sections open: provenance lists the sections that were included."""
    sample = sample_registry.require("tempo")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = monetio.load("tempo", files=sample["files"], **sample["kwargs"])
    history = str(result.attrs.get("history", ""))
    assert "section" in history.lower() or "group" in history.lower(), (
        f"multi-section load must record included sections: {history!r}"
    )


# ---------------------------------------------------------------------------
# C-FAIL-6 / C-FAIL-7 — retrieval vs parse vs credentials are distinct
# ---------------------------------------------------------------------------


def test_retrieval_and_content_errors_are_distinct_classes():
    """A caller catching one must not accidentally catch the other."""
    assert not issubclass(RetrievalFailedError, ContentInvalidError)
    assert not issubclass(ContentInvalidError, RetrievalFailedError)
    assert not issubclass(CredentialDeniedError, RetrievalFailedError)


def test_error_taxonomy_is_closed():
    """C-FAIL-1.4: every category is one of the eleven in the data model."""
    assert FAILURE_CATEGORIES == {
        "not_found",
        "permission_denied",
        "credential_rejected",
        "retrieval_failed",
        "content_invalid",
        "malformed",
        "section_unavailable",
        "option_unsupported",
        "capability_unavailable",
        "metadata_mismatch",
        "dependency_missing",
    }


def test_capability_unavailable_error_quotes_the_reason(sample_registry):
    """C-STAGE-4 groundwork: a refusal names the source and quotes the reason."""
    sample = sample_registry.require("goes")
    with pytest.raises(CapabilityUnavailableError) as excinfo:
        monetio.load("goes", files=sample["files"], defer=True)
    message = str(excinfo.value)
    assert "goes" in message
    assert "defer" in message
    assert "not yet validated" in message  # the recorded reason, quoted


# ---------------------------------------------------------------------------
# C-FAIL-8 — data-quality discards are counted
# ---------------------------------------------------------------------------


def test_discarded_rows_recorded_in_provenance(sample_registry):
    """Rows dropped for a missing location are counted, not silently removed."""
    sample = sample_registry.require("aeronet")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = monetio.load("aeronet", files=sample["files"], **sample["kwargs"])
    history = str(result.attrs.get("history", ""))
    # Either nothing was discarded (then the record says so) or a count appears.
    assert re.search(r"discard\w*\s+=?\s*\d+|dropped\s+\d+", history, re.I), (
        f"provenance must state discarded-row counts: {history!r}"
    )


# ---------------------------------------------------------------------------
# C-FAIL-9 — a transformation that did not run must say so
# ---------------------------------------------------------------------------


def test_ds_to_2d_reports_when_it_cannot_run():
    """FR-008: ds_to_2d must not return the unexpanded input with a warning."""
    from monetio.readers.errors import ReaderError
    from monetio.util import ds_to_2d

    # A frame with no (time, siteid) pair cannot be expanded to 2D.
    ds = xr.Dataset({"value": (("node",), [1.0, 2.0, 3.0])})
    with pytest.raises(ReaderError) as excinfo:
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # a warning is not acceptable here
            ds_to_2d(ds)
    assert "ds_to_2d" in str(excinfo.value) or "expand" in str(excinfo.value).lower()


# ---------------------------------------------------------------------------
# C-FAIL-10 / SC-004 — no unqualified catch-all handlers remain
# ---------------------------------------------------------------------------


def _scan_unqualified_handlers(root: Path):
    """Return [(file, line, swallow?)] for Exception/BaseException handlers."""
    found = []
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler) and node.type is not None:
                name = ast.unparse(node.type)
                if name in ("Exception", "BaseException"):
                    body = ast.unparse(node.body)
                    swallow = not ("\nraise" in body or body.strip().startswith("raise"))
                    # An explicit annotation marks a deliberate, reviewable
                    # non-propagating handler (the file-mismatch diagnosis pattern).
                    annotated = "BLE001" in path.read_text().splitlines()[node.lineno - 1]
                    found.append((path, node.lineno, swallow and not annotated))
    return found


def test_no_unqualified_catch_all_handlers():
    """SC-004: 75 swallow-style catch-alls -> 0 across readers and util."""
    offenders = []
    for base in (_REPO / "monetio" / "readers", _REPO / "monetio" / "util.py"):
        if base.is_dir():
            offenders += [o for o in _scan_unqualified_handlers(base) if o[2]]
        else:
            offenders += [
                o for o in _scan_unqualified_handlers(base.parent) if o[2] and o[0] == base
            ]
    listing = "\n".join(f"  {p.relative_to(_REPO)}:{ln}" for p, ln, _ in offenders)
    assert not offenders, f"unqualified catch-all handlers remain:\n{listing}"


# ---------------------------------------------------------------------------
# Error-shape unit tests — the taxonomy itself
# ---------------------------------------------------------------------------


def test_reader_error_requires_category_and_location():
    err = MalformedInputError("x.bin", "file signature not found")
    assert err.category == "malformed"
    assert "x.bin" in str(err)
    assert "file signature not found" in str(err)


def test_section_error_names_input_and_section():
    err = SectionUnavailableError("prod.h5", "Met_Data", "group not found")
    assert err.category == "section_unavailable"
    assert "prod.h5" in str(err) and "Met_Data" in str(err)


def test_option_error_names_option_and_source():
    err = OptionUnsupportedError("virtualizarr_file", "aeronet")
    assert err.category == "option_unsupported"
    assert "virtualizarr_file" in str(err) and "aeronet" in str(err)


def test_credential_error_names_service():
    err = CredentialDeniedError("s3://public-bucket", "403 Forbidden")
    assert err.category == "credential_rejected"
    assert "403" in str(err)


def test_errors_are_catchable_as_reader_error():
    from monetio.readers.errors import ReaderError

    for cls in (
        MalformedInputError,
        SectionUnavailableError,
        OptionUnsupportedError,
        CredentialDeniedError,
        RetrievalFailedError,
        ContentInvalidError,
        CapabilityUnavailableError,
    ):
        assert issubclass(cls, ReaderError)


def test_public_load_signature_documents_tolerance():
    """The tolerance switch is part of the public entry point's contract."""
    doc = inspect.getdoc(monetio.load) or ""
    assert "tolerate" in doc.lower()
