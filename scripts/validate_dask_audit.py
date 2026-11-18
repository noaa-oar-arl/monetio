#!/usr/bin/env python3
"""Validate the reader Dask evidence ledger without importing reader modules."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from monetio._manifest import SOURCE_MANIFEST

ROOT = Path(__file__).resolve().parents[1]
STATUSES = {"supported", "unsupported", "pending"}
RUN_STATUSES = {"not_run", "passed", "failed", "unavailable_dependency"}
FINDING_STATUSES = {
    "candidate",
    "reproduced",
    "remediation_pending",
    "verified_fixed",
    "dismissed_with_evidence",
    "accepted_limitation",
}
FINDING_TRANSITIONS = {
    "candidate": {"reproduced", "dismissed_with_evidence"},
    "reproduced": {"remediation_pending", "accepted_limitation"},
    "remediation_pending": {"verified_fixed"},
}
REQUIRED_CAPABILITIES = {
    "eager",
    "lazy_dask",
    "deferred",
    "chunk_partition_controls",
    "selective_reads",
    "distributed",
    "deferred_output_conversion",
}


def _fixture_errors(records, label: str, root: Path) -> list[str]:
    """Validate stable hashes for payload fixtures and generated-fixture sources."""
    errors = []
    if not isinstance(records, list):
        return [f"{label}: fixture_hashes must be an array"]
    for i, rec in enumerate(records):
        if not isinstance(rec, dict) or not isinstance(rec.get("fixture_id"), str):
            errors.append(f"{label}: fixture_hashes[{i}] requires fixture_id")
            continue
        fid = rec["fixture_id"]
        kind = rec.get("kind")
        if kind == "file":
            path = rec.get("path")
            if not isinstance(path, str) or Path(path).is_absolute() or ".." in Path(path).parts:
                errors.append(f"{label}: {fid} has an invalid repository-relative path")
                continue
            target = root / path
            if not target.is_file():
                errors.append(f"{label}: {fid} payload does not exist: {path}")
                continue
            digest = hashlib.sha256(target.read_bytes()).hexdigest()
            if rec.get("sha256") != digest:
                errors.append(f"{label}: {fid} payload hash is stale or missing")
        elif kind in {"generated", "generated_subset"}:
            for field in ("factory", "factory_source_path", "factory_source_sha256", "parameters"):
                if field not in rec:
                    errors.append(f"{label}: {fid} generated fixture missing {field}")
            source = rec.get("factory_source_path")
            if isinstance(source, str) and (root / source).is_file():
                digest = _source_digest(root / source)
                if rec.get("factory_source_sha256") != digest:
                    errors.append(f"{label}: {fid} factory source hash is stale")
            elif source is not None:
                errors.append(f"{label}: {fid} factory source does not exist")
            if kind == "generated_subset":
                source = rec.get("source_path")
                if not isinstance(source, str) or not (root / source).is_file():
                    errors.append(f"{label}: {fid} subset source does not exist")
                elif (
                    rec.get("source_sha256")
                    != hashlib.sha256((root / source).read_bytes()).hexdigest()
                ):
                    errors.append(f"{label}: {fid} subset source hash is stale or missing")
        else:
            errors.append(f"{label}: {fid} has unsupported fixture kind {kind!r}")
    return errors


def _code_hash_errors(records, label: str, root: Path) -> list[str]:
    """Check the repository code files that a passing result depends on."""
    errors = []
    if not isinstance(records, list) or not records:
        return [f"{label}: code_hashes must be a nonempty array"]
    seen = set()
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            errors.append(f"{label}: code_hashes[{i}] must be an object")
            continue
        path = rec.get("path")
        if not isinstance(path, str) or Path(path).is_absolute() or ".." in Path(path).parts:
            errors.append(f"{label}: code_hashes[{i}] has an invalid repository-relative path")
            continue
        if path in seen:
            errors.append(f"{label}: duplicate code hash path {path}")
            continue
        seen.add(path)
        target = root / path
        if not target.is_file():
            errors.append(f"{label}: hashed code file does not exist: {path}")
            continue
        if rec.get("sha256") != _source_digest(target):
            errors.append(f"{label}: code hash is stale for {path}")
    return errors


def _source_digest(path: Path) -> str:
    """Hash source text consistently across checkouts with different line endings."""
    normalized = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(normalized).hexdigest()


REQUIRED_TOP = {
    "schema_version",
    "baseline",
    "modules",
    "modes",
    "exceptions",
    "findings",
    "evidence",
    "references",
}


def validate(ledger: object, root: Path, strict: bool) -> list[str]:
    errors: list[str] = []
    if not isinstance(ledger, dict):
        return ["ledger root must be a JSON object"]
    missing = REQUIRED_TOP - ledger.keys()
    if missing:
        errors.append(f"ledger missing required keys: {', '.join(sorted(missing))}")
    if ledger.get("schema_version") != 1:
        errors.append(f"unknown schema_version {ledger.get('schema_version')!r}; expected 1")
    if not isinstance(ledger.get("baseline"), dict):
        errors.append("baseline must be an object")
    elif not all(
        ledger["baseline"].get(k)
        for k in ("git_revision", "python", "platform", "date", "dependencies")
    ):
        errors.append("baseline requires git_revision, python, platform, date, and dependencies")
    for key in ("modules", "modes", "exceptions", "findings", "evidence", "references"):
        if not isinstance(ledger.get(key), list):
            errors.append(f"{key} must be an array")
    if errors:
        return errors

    actual = {p.relative_to(root).as_posix() for p in (root / "monetio/readers").rglob("*.py")}
    modules = ledger["modules"]
    paths: list[str] = []
    module_kinds = {
        "reader",
        "shared driver",
        "parser",
        "helper",
        "specification",
        "package initialization",
    }
    for i, rec in enumerate(modules):
        if not isinstance(rec, dict):
            errors.append(f"modules[{i}] must be an object")
            continue
        path = rec.get("module_path")
        if (
            not isinstance(path, str)
            or not path.startswith("monetio/readers/")
            or ".." in Path(path).parts
        ):
            errors.append(f"modules[{i}] has invalid module_path {path!r}")
        else:
            paths.append(path)
        if rec.get("kind") not in module_kinds:
            errors.append(f"{path}: invalid module kind {rec.get('kind')!r}")
        if rec.get("disposition") not in {"audited", "pending"}:
            errors.append(f"{path}: disposition must be audited or pending")
        if not isinstance(rec.get("public_paths"), list) or not isinstance(
            rec.get("affected_mode_ids"), list
        ):
            errors.append(f"{path}: public_paths and affected_mode_ids must be arrays")
        elif rec.get("kind") == "reader" and not rec["public_paths"]:
            errors.append(f"{path}: reader modules must enumerate their public loading paths")
        if rec.get("disposition") == "pending" and strict:
            errors.append(f"{path}: module disposition remains pending")
    if len(paths) != len(set(paths)):
        errors.append("duplicate module paths")
    if set(paths) != actual:
        errors.append(
            f"reader inventory mismatch: missing={sorted(actual - set(paths))}; stale={sorted(set(paths) - actual)}"
        )

    evidence = ledger["evidence"]
    evidence_by_id: dict[str, dict] = {}
    for i, rec in enumerate(evidence):
        if not isinstance(rec, dict) or not isinstance(rec.get("case_id"), str):
            errors.append(f"evidence[{i}] requires a case_id")
            continue
        cid = rec["case_id"]
        if cid in evidence_by_id:
            errors.append(f"duplicate evidence id {cid}")
        evidence_by_id[cid] = rec
        if rec.get("run_status") not in RUN_STATUSES:
            errors.append(f"{cid}: invalid run_status")
        if rec.get("run_status") == "passed" and not all(
            rec.get(k)
            for k in (
                "command",
                "report",
                "code_revision",
                "dependency_versions",
                "dependency_fingerprint",
            )
        ):
            errors.append(
                f"{cid}: passed evidence requires command, report, code_revision, "
                "dependency_versions, and dependency_fingerprint"
            )
        if rec.get("dependency_fingerprint"):
            expected = hashlib.sha256(
                json.dumps(
                    rec.get("dependency_versions"), sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
            if rec["dependency_fingerprint"] != expected:
                errors.append(f"{cid}: dependency fingerprint is stale")
        if rec.get("run_status") == "passed":
            errors.extend(_code_hash_errors(rec.get("code_hashes"), cid, root))
        for field in ("affected_modes", "requirements", "fixture_provenance", "expected_behavior"):
            if field not in rec:
                errors.append(f"{cid}: missing validation case field {field}")
        if not isinstance(rec.get("affected_modes", []), list):
            errors.append(f"{cid}: affected_modes must be an array")
        if (
            rec.get("run_status") == "passed"
            and not rec.get("no_payload_fixture", False)
            and not rec.get("fixture_hashes")
        ):
            errors.append(
                f"{cid}: passing evidence requires fixture hashes or no_payload_fixture=true"
            )
        errors.extend(_fixture_errors(rec.get("fixture_hashes", []), cid, root))

    references = ledger["references"]
    reference_ids: set[str] = set()
    for i, rec in enumerate(references):
        if not isinstance(rec, dict) or not isinstance(rec.get("reference_id"), str):
            errors.append(f"references[{i}] requires a reference_id")
            continue
        rid = rec["reference_id"]
        if rid in reference_ids:
            errors.append(f"duplicate reference ID {rid}")
        reference_ids.add(rid)
        for field in (
            "library",
            "version",
            "revision",
            "source",
            "start_line",
            "end_line",
            "url",
            "requirement_ids",
        ):
            if field not in rec:
                errors.append(f"{rid}: missing OMD reference field {field}")

    exceptions = ledger["exceptions"]
    exception_ids = [x.get("exception_id") for x in exceptions if isinstance(x, dict)]
    if len(exception_ids) != len(set(exception_ids)):
        errors.append("duplicate preparation exception IDs")
    exception_id_set = set(exception_ids)
    for exc in exceptions:
        if not isinstance(exc, dict):
            errors.append("preparation exception must be an object")
            continue
        for field in (
            "exception_id",
            "mode_ids",
            "stage",
            "purpose",
            "trigger",
            "data_touched",
            "payload_reads",
            "input_cost_bound",
            "output_cost_bound",
            "expected_passes",
            "later_reread",
            "user_disclosure",
            "evidence_ids",
        ):
            if field not in exc:
                errors.append(
                    f"{exc.get('exception_id', '<unknown>')}: missing exception field {field}"
                )
    finding_ids = [x.get("finding_id") for x in ledger["findings"] if isinstance(x, dict)]
    if len(finding_ids) != len(set(finding_ids)):
        errors.append("duplicate finding IDs")
    for finding in ledger["findings"]:
        if not isinstance(finding, dict):
            errors.append("finding must be an object")
            continue
        for field in (
            "finding_id",
            "mode_ids",
            "requirement",
            "observed_behavior",
            "user_impact",
            "reproduction",
            "omd_reference_ids",
            "status",
            "proposed_resolution",
            "verification_evidence_ids",
        ):
            if field not in finding:
                errors.append(
                    f"{finding.get('finding_id', '<unknown>')}: missing finding field {field}"
                )
        if finding.get("status") not in FINDING_STATUSES:
            errors.append(f"{finding.get('finding_id', '<unknown>')}: invalid finding status")
        for ref in finding.get("omd_reference_ids", []):
            if ref not in reference_ids:
                errors.append(
                    f"{finding.get('finding_id', '<unknown>')}: dangling OMD reference {ref}"
                )
        status = finding.get("status")
        history = finding.get("status_history")
        if strict and history is None:
            errors.append(
                f"{finding.get('finding_id')}: final finding disposition requires status_history"
            )
        if history is not None:
            if (
                not isinstance(history, list)
                or not history
                or any(item not in FINDING_STATUSES for item in history)
            ):
                errors.append(
                    f"{finding.get('finding_id')}: status_history must be a nonempty list of valid statuses"
                )
            else:
                if history[-1] != status:
                    errors.append(
                        f"{finding.get('finding_id')}: status_history must end with current status"
                    )
                for previous, current in zip(history, history[1:]):
                    if current not in FINDING_TRANSITIONS.get(previous, set()):
                        errors.append(
                            f"{finding.get('finding_id')}: invalid finding transition {previous} -> {current}"
                        )
        evidence_refs = finding.get("verification_evidence_ids", [])
        if status in {"verified_fixed", "dismissed_with_evidence", "accepted_limitation"}:
            if not evidence_refs:
                errors.append(
                    f"{finding.get('finding_id')}: terminal finding status requires evidence"
                )
            for ref in evidence_refs:
                evidence_rec = evidence_by_id.get(ref)
                if evidence_rec is None:
                    errors.append(
                        f"{finding.get('finding_id')}: missing verification evidence {ref}"
                    )
                elif evidence_rec.get("run_status") != "passed":
                    errors.append(
                        f"{finding.get('finding_id')}: verification evidence {ref} did not pass"
                    )
        if status == "accepted_limitation":
            for mode_id in finding.get("mode_ids", []):
                mode_rec = next((m for m in ledger["modes"] if m.get("mode_id") == mode_id), None)
                if mode_rec is None or not any(
                    cap.get("status") == "unsupported"
                    for cap in mode_rec.get("capabilities", {}).values()
                    if isinstance(cap, dict)
                ):
                    errors.append(
                        f"{finding.get('finding_id')}: accepted limitation {mode_id} "
                        "must reject at least one capability"
                    )
                elif strict and not mode_rec.get("documentation"):
                    errors.append(
                        f"{finding.get('finding_id')}: accepted limitation {mode_id} "
                        "requires linked user guidance"
                    )
    mode_ids: list[str] = []
    mode_keys = {
        "mode_id",
        "source",
        "aliases",
        "module",
        "entrypoint",
        "output_form",
        "backend",
        "options",
        "capabilities",
        "fixture_ids",
        "exception_ids",
        "resource_policy",
        "tolerance_policy",
        "documentation",
    }
    for i, rec in enumerate(ledger["modes"]):
        if not isinstance(rec, dict):
            errors.append(f"modes[{i}] must be an object")
            continue
        mid = rec.get("mode_id")
        if not isinstance(mid, str):
            errors.append(f"modes[{i}] requires a mode_id")
            continue
        mode_ids.append(mid)
        missing_mode = mode_keys - rec.keys()
        if missing_mode:
            errors.append(f"{mid}: missing fields {', '.join(sorted(missing_mode))}")
        if rec.get("output_form") not in {
            "table",
            "long_dataset",
            "wide_dataset",
            "gridded_dataset",
        }:
            errors.append(f"{mid}: invalid output_form")
        if not isinstance(rec.get("aliases"), list) or not isinstance(rec.get("options"), dict):
            errors.append(f"{mid}: aliases must be an array and options must be an object")
        if (
            not isinstance(rec.get("fixture_ids"), list)
            or not isinstance(rec.get("fixture_hashes"), list)
            or not isinstance(rec.get("exception_ids"), list)
        ):
            errors.append(f"{mid}: fixture_ids/fixture_hashes and exception_ids must be arrays")
        errors.extend(_fixture_errors(rec.get("fixture_hashes", []), mid, root))
        hash_ids = {
            fixture.get("fixture_id")
            for fixture in rec.get("fixture_hashes", [])
            if isinstance(fixture, dict)
        }
        if not set(rec.get("fixture_ids", [])) <= hash_ids:
            errors.append(f"{mid}: fixture_ids lack corresponding fixture hashes")
        if (
            not isinstance(rec.get("tolerance_policy"), dict)
            or rec.get("tolerance_policy", {}).get("default") != "exact"
        ):
            errors.append(f"{mid}: tolerance_policy must default to exact comparison")
        if strict and not rec.get("resource_policy"):
            errors.append(f"{mid}: final disposition requires a resource ownership/cleanup policy")
        if rec.get("backend") == "*" and not rec.get("equivalent_path_justification"):
            errors.append(f"{mid}: wildcard backend requires an equivalent-path justification")
        for ref in rec.get("fixture_ids", []):
            if not isinstance(ref, str) or not ref:
                errors.append(f"{mid}: fixture IDs must be nonempty strings")
            elif ref.startswith("sample:"):
                name = ref.removeprefix("sample:")
                if not any(
                    (root / base / name / "manifest.json").is_file()
                    for base in ("tests/data/samples", "tests/data/synthetic")
                ):
                    errors.append(f"{mid}: fixture {ref} has no sample manifest")
            elif ref.startswith("generated:"):
                if ref not in {
                    fixture.get("fixture_id")
                    for fixture in rec.get("fixture_hashes", [])
                    if isinstance(fixture, dict)
                }:
                    errors.append(f"{mid}: generated fixture {ref} has no factory/hash record")
            elif not (root / ref).exists():
                errors.append(f"{mid}: fixture path does not exist: {ref}")
        for ref in rec.get("exception_ids", []):
            if ref not in exception_id_set:
                errors.append(f"{mid}: dangling exception ID {ref}")
        caps = rec.get("capabilities")
        if not isinstance(caps, dict) or not caps:
            errors.append(f"{mid}: capabilities must be a nonempty object")
            continue
        missing_caps = REQUIRED_CAPABILITIES - caps.keys()
        if missing_caps:
            errors.append(
                f"{mid}: missing capability assessments: {', '.join(sorted(missing_caps))}"
            )
        for name, claim in caps.items():
            if not isinstance(claim, dict) or claim.get("status") not in STATUSES:
                errors.append(f"{mid}: {name} must have supported/unsupported/pending status")
                continue
            if not isinstance(claim.get("claimed"), bool):
                errors.append(f"{mid}: {name}.claimed must be boolean")
            if claim["status"] == "unsupported" and not isinstance(claim.get("reason"), str):
                errors.append(f"{mid}: unsupported {name} must include a reason")
            refs = claim.get("evidence_ids", [])
            if not isinstance(refs, list):
                errors.append(f"{mid}: {name}.evidence_ids must be an array")
                continue
            for ref in refs:
                if ref not in evidence_by_id:
                    errors.append(f"{mid}: {name} references missing evidence {ref}")
                elif mid not in evidence_by_id[ref].get("affected_modes", []):
                    errors.append(
                        f"{mid}: {name} references evidence {ref} that does not cover this mode"
                    )
            if strict and claim["status"] == "pending":
                errors.append(f"{mid}: {name} capability remains pending")
            if strict and claim["status"] == "supported":
                if not refs or any(evidence_by_id[r].get("run_status") != "passed" for r in refs):
                    errors.append(f"{mid}: supported {name} lacks passing behavioral evidence")
                if not rec.get("fixture_ids"):
                    errors.append(f"{mid}: supported {name} requires fixture provenance")
            if strict and claim["status"] == "unsupported":
                if not refs or any(evidence_by_id[r].get("run_status") != "passed" for r in refs):
                    errors.append(f"{mid}: unsupported {name} lacks passing rejection evidence")
        if rec.get("options", {}).get("defer") is True:
            deferred_assessment = caps.get("deferred", {})
            rejection_evidenced = (
                isinstance(deferred_assessment, dict)
                and deferred_assessment.get("status") == "unsupported"
                and any(
                    ref in evidence_by_id
                    and evidence_by_id[ref].get("run_status") == "passed"
                    and mid in evidence_by_id[ref].get("affected_modes", [])
                    for ref in deferred_assessment.get("evidence_ids", [])
                )
            )
            if rejection_evidenced:
                for name, assessment in caps.items():
                    if assessment.get("status") == "pending":
                        errors.append(
                            f"{mid}: defer=True rejection is evidenced; {name} cannot remain pending"
                        )
    if len(mode_ids) != len(set(mode_ids)):
        errors.append("duplicate mode IDs")
    mode_id_set = set(mode_ids)
    # Reconcile the required public source/output/deferral matrix with the
    # import-free manifest. This catches omitted unsupported paths as well as
    # missing supported cases without importing any reader implementation.
    for spec in SOURCE_MANIFEST:
        public_names = (spec.name, *spec.aliases)
        expected_forms = (
            {"gridded_dataset"} if spec.kind == "gridded" else {"table", "wide_dataset"}
        )
        for public_name in public_names:
            for output_form in expected_forms:
                for defer_value in (False, True):
                    found = any(
                        mode.get("mode_id", "").startswith(f"{public_name}:")
                        and mode.get("output_form") == output_form
                        and mode.get("options", {}).get("defer") is defer_value
                        for mode in ledger["modes"]
                        if isinstance(mode, dict)
                    )
                    if not found:
                        errors.append(
                            f"{public_name}: audit mode missing for {output_form}, "
                            f"defer={str(defer_value).lower()}"
                        )
    for exc in exceptions:
        if isinstance(exc, dict):
            for ref in exc.get("mode_ids", []):
                if ref not in mode_id_set:
                    errors.append(f"{exc.get('exception_id')}: dangling mode ID {ref}")
            for ref in exc.get("evidence_ids", []):
                if ref not in evidence_by_id:
                    errors.append(f"{exc.get('exception_id')}: dangling evidence ID {ref}")
    for finding in ledger["findings"]:
        if isinstance(finding, dict):
            for ref in finding.get("mode_ids", []):
                if ref not in mode_id_set:
                    errors.append(f"{finding.get('finding_id')}: dangling mode ID {ref}")
            for ref in finding.get("verification_evidence_ids", []):
                if ref not in evidence_by_id:
                    errors.append(f"{finding.get('finding_id')}: dangling evidence ID {ref}")
    for rec in modules:
        if not isinstance(rec, dict):
            continue
        for ref in rec.get("affected_mode_ids", []):
            if ref not in mode_id_set:
                errors.append(f"{rec.get('module_path')}: dangling mode ID {ref}")
    return errors


def _build_report(ledger: dict, errors: list[str]) -> dict:
    """Build a machine-readable coverage report without inferring passes."""
    modes = ledger.get("modes", [])
    modes = modes if isinstance(modes, list) else []
    evidence = ledger.get("evidence", [])
    evidence = evidence if isinstance(evidence, list) else []
    capabilities: dict[str, dict[str, int]] = {}
    for mode in modes:
        if not isinstance(mode, dict):
            continue
        for capability, assessment in mode.get("capabilities", {}).items():
            if isinstance(assessment, dict):
                status = assessment.get("status", "invalid")
                counts = capabilities.setdefault(capability, {key: 0 for key in STATUSES})
                if status in counts:
                    counts[status] += 1
    evidence_statuses = {status: 0 for status in RUN_STATUSES}
    unavailable_modes = set()
    for case in evidence:
        if not isinstance(case, dict):
            continue
        status = case.get("run_status")
        if status in evidence_statuses:
            evidence_statuses[status] += 1
        if status == "unavailable_dependency":
            unavailable_modes.update(case.get("affected_modes", []))
    modules = ledger.get("modules", [])
    modules = modules if isinstance(modules, list) else []
    exceptions = ledger.get("exceptions", [])
    exceptions = exceptions if isinstance(exceptions, list) else []
    findings = ledger.get("findings", [])
    findings = findings if isinstance(findings, list) else []
    return {
        "schema_version": ledger.get("schema_version"),
        "baseline": ledger.get("baseline"),
        "totals": {
            "modules": len(modules),
            "modes": len(modes),
            "exceptions": len(exceptions),
            "findings": len(findings),
            "evidence": len(evidence),
        },
        "capability_status_counts": capabilities,
        "evidence_status_counts": evidence_statuses,
        "exceptions": exceptions,
        "findings": findings,
        "unavailable_modes": sorted(unavailable_modes),
        "consistency_errors": errors,
        "result": "pass" if not errors else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument(
        "--structural-only",
        action="store_true",
        help="allow pending dispositions while constructing evidence",
    )
    args = parser.parse_args()
    try:
        ledger = json.loads(args.ledger.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"invalid ledger: {exc}", file=sys.stderr)
        return 2
    errors = validate(ledger, ROOT, strict=not args.structural_only)
    report = _build_report(ledger if isinstance(ledger, dict) else {}, errors)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
