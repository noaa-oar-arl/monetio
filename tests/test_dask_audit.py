"""Audit ledger integrity and event probe tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
from dask_audit_support import AuditTrace, TaskTrace, audit_stage

from scripts.validate_dask_audit import _source_digest, validate

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "tests/data/dask_reader_audit.json"


def _ledger():
    return json.loads(LEDGER.read_text())


def test_initial_ledger_matches_recursive_inventory():
    errors = validate(_ledger(), ROOT, strict=False)
    assert errors == []


def test_unknown_schema_version_is_rejected():
    ledger = _ledger()
    ledger["schema_version"] = 99
    assert any("unknown schema_version" in e for e in validate(ledger, ROOT, strict=False))


def test_missing_module_is_reported():
    ledger = _ledger()
    ledger["modules"].pop()
    assert any("reader inventory mismatch" in e for e in validate(ledger, ROOT, strict=False))


def test_new_uninventoried_reader_module_is_reported():
    ledger = _ledger()
    ledger["modules"].append(
        {
            "module_path": "monetio/readers/new_reader.py",
            "kind": "reader",
            "disposition": "pending",
            "public_paths": ["NewReader.open_dataset"],
            "affected_mode_ids": [],
        }
    )
    assert any(
        "reader inventory mismatch" in error for error in validate(ledger, ROOT, strict=False)
    )


def test_missing_public_output_mode_is_reported():
    ledger = _ledger()
    ledger["modes"] = [
        mode
        for mode in ledger["modes"]
        if mode["mode_id"] != "airnow:open_dataset:wide_dataset:source_default:defer=false"
    ]
    assert any(
        "airnow: audit mode missing for wide_dataset, defer=false" in error
        for error in validate(ledger, ROOT, strict=False)
    )


def test_mode_must_name_its_backend_path():
    ledger = _ledger()
    mode = ledger["modes"][0]
    mode.pop("backend")
    assert any(
        f"{mode['mode_id']}: missing fields backend" in error
        for error in validate(ledger, ROOT, strict=False)
    )


def test_reader_module_must_name_loading_entrypoint():
    ledger = _ledger()
    module = next(m for m in ledger["modules"] if m["kind"] == "reader")
    module["public_paths"] = []
    assert any(
        "must enumerate their public loading paths" in e
        for e in validate(ledger, ROOT, strict=False)
    )


def test_distinct_gfs_family_classes_have_separate_ledger_modes():
    from monetio.readers.gfs import GDASReader, GEFSReader, GFSReader

    ledger = _ledger()
    modes = [
        mode
        for mode in ledger["modes"]
        if mode["mode_id"].split(":", 1)[0] in {"gfs", "gefs", "gdas"}
        and mode["options"].get("defer") is False
    ]
    assert {mode["mode_id"].split(":", 1)[0] for mode in modes} == {"gfs", "gefs", "gdas"}
    assert len({mode["mode_id"] for mode in modes}) == 3
    assert len({GFSReader, GEFSReader, GDASReader}) == 3


def test_duplicate_mode_id_is_reported():
    ledger = _ledger()
    ledger["modes"].append(copy.deepcopy(ledger["modes"][0]))
    assert "duplicate mode IDs" in validate(ledger, ROOT, strict=False)


def test_duplicate_evidence_id_is_reported():
    ledger = _ledger()
    ledger["evidence"].append(copy.deepcopy(ledger["evidence"][0]))
    assert any("duplicate evidence id" in error for error in validate(ledger, ROOT, strict=False))


def test_invalid_mode_shape_and_non_exact_tolerance_are_reported():
    ledger = _ledger()
    ledger["modes"][0]["output_form"] = "unknown"
    ledger["modes"][1]["tolerance_policy"]["default"] = "approximate"
    errors = validate(ledger, ROOT, strict=False)
    assert any("invalid output_form" in e for e in errors)
    assert any("tolerance_policy must default to exact" in e for e in errors)


def test_missing_capability_assessment_is_reported():
    ledger = _ledger()
    del ledger["modes"][0]["capabilities"]["distributed"]
    assert any("missing capability assessments" in e for e in validate(ledger, ROOT, strict=False))


def test_evidence_must_cover_each_mode_that_references_it():
    ledger = _ledger()
    mode = ledger["modes"][0]
    evidence_id = ledger["evidence"][0]["case_id"]
    mode["capabilities"]["deferred"]["status"] = "unsupported"
    mode["capabilities"]["deferred"]["evidence_ids"] = [evidence_id]
    assert any("does not cover this mode" in e for e in validate(ledger, ROOT, strict=False))


def test_findings_cannot_reference_unknown_omd_sources():
    ledger = _ledger()
    ledger["findings"].append(
        {
            "finding_id": "finding-test",
            "mode_ids": [],
            "requirement": "FR-001",
            "observed_behavior": "test",
            "user_impact": "test",
            "reproduction": "test",
            "omd_reference_ids": ["missing-omd"],
            "status": "candidate",
            "proposed_resolution": "test",
            "verification_evidence_ids": [],
        }
    )
    assert any("dangling OMD reference" in e for e in validate(ledger, ROOT, strict=False))


def test_stale_fixture_hash_is_reported():
    ledger = _ledger()
    mode = next(m for m in ledger["modes"] if m["fixture_hashes"])
    mode["fixture_hashes"][0]["sha256"] = "0" * 64
    assert any("payload hash is stale" in e for e in validate(ledger, ROOT, strict=False))


def test_stale_generated_fixture_factory_hash_is_reported():
    ledger = _ledger()
    mode = next(
        m for m in ledger["modes"] if any(f.get("kind") == "generated" for f in m["fixture_hashes"])
    )
    fixture = next(f for f in mode["fixture_hashes"] if f.get("kind") == "generated")
    fixture["factory_source_sha256"] = "0" * 64
    assert any("factory source hash is stale" in e for e in validate(ledger, ROOT, strict=False))


def test_stale_code_hash_is_reported():
    ledger = _ledger()
    case = next(e for e in ledger["evidence"] if e.get("code_hashes"))
    case["code_hashes"][0]["sha256"] = "0" * 64
    assert any("code hash is stale" in error for error in validate(ledger, ROOT, strict=False))


def test_source_hash_is_independent_of_checkout_line_endings(tmp_path):
    source = tmp_path / "source.py"
    source.write_bytes(b"first line\nsecond line\n")
    lf_digest = _source_digest(source)
    source.write_bytes(b"first line\r\nsecond line\r\n")
    assert _source_digest(source) == lf_digest


def test_passed_evidence_requires_code_hashes():
    ledger = _ledger()
    case = next(e for e in ledger["evidence"] if e.get("run_status") == "passed")
    case.pop("code_hashes", None)
    assert any(
        "code_hashes must be a nonempty array" in error
        for error in validate(ledger, ROOT, strict=False)
    )


def test_stale_dependency_fingerprint_is_reported():
    ledger = _ledger()
    case = next(e for e in ledger["evidence"] if e.get("dependency_fingerprint"))
    case["dependency_fingerprint"] = "0" * 64
    assert any(
        "dependency fingerprint is stale" in error for error in validate(ledger, ROOT, strict=False)
    )


def test_unavailable_dependency_cannot_support_a_capability():
    ledger = _ledger()
    mode = ledger["modes"][0]
    case = copy.deepcopy(ledger["evidence"][0])
    case["case_id"] = "unavailable-test-backend"
    case["affected_modes"] = [mode["mode_id"]]
    case["run_status"] = "unavailable_dependency"
    ledger["evidence"].append(case)
    claim = mode["capabilities"]["deferred"]
    claim.update(claimed=True, status="supported", evidence_ids=[case["case_id"]])
    assert any(
        "supported deferred lacks passing behavioral evidence" in error
        for error in validate(ledger, ROOT, strict=True)
    )


def test_unsupported_capability_requires_passing_rejection_evidence():
    ledger = _ledger()
    mode = ledger["modes"][0]
    claim = mode["capabilities"]["distributed"]
    claim.update(claimed=False, status="unsupported", reason="not supported", evidence_ids=[])
    assert any(
        "unsupported distributed lacks passing rejection evidence" in error
        for error in validate(ledger, ROOT, strict=True)
    )


def test_rejected_defer_mode_cannot_leave_dependent_capabilities_pending():
    ledger = _ledger()
    mode = next(
        m
        for m in ledger["modes"]
        if m["options"].get("defer") is True
        and m["capabilities"]["deferred"]["status"] == "unsupported"
    )
    mode["capabilities"]["distributed"].update(
        status="pending", claimed=False, reason="not checked", evidence_ids=[]
    )
    assert any(
        f"{mode['mode_id']}: defer=True rejection is evidenced; distributed cannot remain pending"
        in error
        for error in validate(ledger, ROOT, strict=False)
    )


def test_dangling_evidence_and_exception_ids_are_reported():
    ledger = _ledger()
    mode = ledger["modes"][0]
    mode["fixture_ids"] = ["missing-fixture.csv"]
    mode["exception_ids"] = ["missing-exception"]
    errors = validate(ledger, ROOT, strict=False)
    assert any("missing-fixture.csv" in e for e in errors)
    assert any("missing-exception" in e for e in errors)


def test_pending_mode_fails_final_validation():
    ledger = _ledger()
    mode = ledger["modes"][0]
    cap = mode["capabilities"]["distributed"]
    cap.update(status="pending", claimed=False, reason="test pending disposition", evidence_ids=[])
    assert any("remains pending" in e for e in validate(ledger, ROOT, strict=True))


def test_supported_capability_needs_passing_evidence():
    ledger = _ledger()
    mode = ledger["modes"][0]
    cap = next(iter(mode["capabilities"].values()))
    cap.update(claimed=True, status="supported", evidence_ids=[])
    assert any(
        "lacks passing behavioral evidence" in e for e in validate(ledger, ROOT, strict=True)
    )


def test_trace_records_open_prepare_evaluate_and_cleanup_events():
    trace = AuditTrace()
    for stage in ("open", "prepare", "evaluate", "cleanup"):
        with audit_stage(trace, stage):
            trace.record(stage, "test", "fixture.nc")
    assert [e.stage for e in trace.events] == ["open", "prepare", "evaluate", "cleanup"]
    assert trace.input_passes("prepare", "fixture.nc") == 1


def test_task_trace_observes_data_tasks():
    import dask
    import dask.array as da

    trace = AuditTrace()
    arr = da.from_array(np.arange(12), chunks=4)
    with TaskTrace(trace, "evaluate"):
        dask.compute(arr.sum())
    assert trace.stage_events("evaluate")
    assert all(event.process_id for event in trace.stage_events("evaluate"))


def test_terminal_finding_requires_a_passing_verification_case():
    ledger = _ledger()
    finding = next(f for f in ledger["findings"] if f["status"] == "verified_fixed")
    case_id = finding["verification_evidence_ids"][0]
    evidence = next(e for e in ledger["evidence"] if e["case_id"] == case_id)
    evidence["run_status"] = "failed"
    assert any(
        f"verification evidence {case_id} did not pass" in error
        for error in validate(ledger, ROOT, strict=False)
    )


def test_accepted_limitation_requires_rejected_capability():
    ledger = _ledger()
    finding = ledger["findings"][0]
    finding["status"] = "accepted_limitation"
    finding["status_history"] = ["candidate", "reproduced", "accepted_limitation"]
    mode = next(m for m in ledger["modes"] if m["mode_id"] == finding["mode_ids"][0])
    for assessment in mode["capabilities"].values():
        assessment["status"] = "pending"
    assert any(
        "must reject at least one capability" in error
        for error in validate(ledger, ROOT, strict=False)
    )


def test_finding_history_enforces_only_documented_lifecycle_transitions():
    ledger = _ledger()
    finding = next(f for f in ledger["findings"] if f["status"] == "verified_fixed")
    finding["status_history"] = ["candidate", "reproduced", "remediation_pending", "verified_fixed"]
    assert not any(
        "invalid finding transition" in error for error in validate(ledger, ROOT, strict=False)
    )
    finding["status_history"] = ["candidate", "verified_fixed"]
    assert any(
        "invalid finding transition candidate -> verified_fixed" in error
        for error in validate(ledger, ROOT, strict=False)
    )

    finding["status"] = "dismissed_with_evidence"
    finding["status_history"] = ["candidate", "dismissed_with_evidence"]
    assert not any(
        "invalid finding transition" in error for error in validate(ledger, ROOT, strict=False)
    )


def test_final_finding_requires_lifecycle_history():
    ledger = _ledger()
    finding = next(f for f in ledger["findings"] if f["status"] == "verified_fixed")
    finding.pop("status_history")
    assert any(
        f"{finding['finding_id']}: final finding disposition requires status_history" in error
        for error in validate(ledger, ROOT, strict=True)
    )


def test_report_has_status_counts_and_unavailable_mode_links():
    from scripts.validate_dask_audit import _build_report

    ledger = _ledger()
    mode_id = ledger["modes"][0]["mode_id"]
    ledger["modes"][0]["capabilities"]["distributed"].update(
        status="pending", claimed=False, reason="test pending disposition", evidence_ids=[]
    )
    ledger["evidence"].append(
        {
            "case_id": "test-unavailable-backend",
            "affected_modes": [mode_id],
            "run_status": "unavailable_dependency",
        }
    )
    report = _build_report(ledger, [])
    assert mode_id in report["unavailable_modes"]
    assert set(report["unavailable_modes"]) == {
        mode_id,
        *(
            mode
            for case in ledger["evidence"]
            if case["run_status"] == "unavailable_dependency"
            for mode in case["affected_modes"]
        ),
    }
    assert report["evidence_status_counts"]["unavailable_dependency"] == sum(
        case["run_status"] == "unavailable_dependency" for case in ledger["evidence"]
    )
    assert report["capability_status_counts"]["distributed"]["pending"] > 0
    assert report["exceptions"] == ledger["exceptions"]
    assert report["findings"] == ledger["findings"]


def test_validator_cli_exit_codes_and_json_report(tmp_path):
    import subprocess
    import sys

    valid_ledger = tmp_path / "ledger.json"
    valid_ledger.write_text(LEDGER.read_text())
    report_path = tmp_path / "report.json"
    script = ROOT / "scripts" / "validate_dask_audit.py"

    structural = subprocess.run(
        [
            sys.executable,
            str(script),
            "--ledger",
            str(valid_ledger),
            "--structural-only",
            "--report",
            str(report_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert structural.returncode == 0, structural.stderr
    report = json.loads(report_path.read_text())
    assert {
        "exceptions",
        "findings",
        "capability_status_counts",
        "evidence_status_counts",
    } <= report.keys()

    pending_ledger = tmp_path / "pending-ledger.json"
    pending = json.loads(valid_ledger.read_text())
    pending["modes"][0]["capabilities"]["distributed"].update(
        status="pending", claimed=False, reason="test pending disposition", evidence_ids=[]
    )
    pending_ledger.write_text(json.dumps(pending))
    strict = subprocess.run(
        [sys.executable, str(script), "--ledger", str(pending_ledger)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert strict.returncode == 1

    malformed = tmp_path / "malformed.json"
    malformed.write_text("{")
    invalid = subprocess.run(
        [sys.executable, str(script), "--ledger", str(malformed)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert invalid.returncode == 2
