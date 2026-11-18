"""Create and validate per-source offline-sample manifests.

An offline sample lives at ``tests/data/samples/<source>/`` (a payload recorded
from a remote service) or ``tests/data/synthetic/<source>/`` (a file generated
to reproduce a format that cannot be redistributed). Next to the payload sits a
``manifest.json`` that makes the sample auditable rather than an opaque blob: it
records where the bytes came from, when, a hash of the bytes, the checked-in
procedure that produced them, and — for a synthetic sample — why it is synthetic.

This module is the single definition of that schema. The capture and generation
scripts import :func:`write_manifest` so a sample can never be written without a
valid manifest, and the coverage test imports :func:`validate_manifest` and
:func:`content_hash` to assert the checked-in manifests are well-formed and that
each hash still matches the payload on disk.

Manifest fields
---------------
``location``
    The remote URL or object-store path the payload was retrieved from, or
    ``None`` for a synthetic sample (nothing was retrieved).
``captured``
    The ISO date the payload was recorded, or ``None`` for a synthetic sample.
``content_hash``
    ``sha256`` of the payload bytes, computed by :func:`content_hash` over the
    sorted payload files. This is the staleness detector: a refresh run that
    re-contacts the service compares this hash to the current bytes and reports
    drift (the check needs network access, so it is a documented on-demand
    procedure, not an offline CI gate).
``producer``
    The checked-in script that produced the sample, repo-relative. Reproducible
    means a reviewer can re-run this path; the coverage gate asserts the named
    script exists.
``reason``
    For a synthetic sample, why the real data is not used (format not
    redistributable, etc.). May be ``""`` for a recorded sample.

Optional, non-schema fields the loaders may read
------------------------------------------------
``kind``
    ``"recorded"`` or ``"synthetic"``; derived from the directory when absent.
``load_kwargs``
    Extra arguments the conformance suite passes to ``monetio.load`` alongside
    ``files=`` (for example a ``group`` for a nested HDF-EOS5 layout).
``layout``
    Which claimed product/layout this sample covers, for a multi-layout reader.
``requires``
    The name of an optional package (an xarray decode engine, typically) that
    must be importable for the sample to load. The test suite skips the
    load-dependent rules for such a sample with a stated reason when the
    package is absent, instead of failing on an uninstalled optional
    dependency.

Usage::

    python scripts/make_sample_manifest.py write <dir> --producer <script> \
        [--location URL] [--kind recorded|synthetic] [--reason TEXT] \
        [--layout NAME] [--load-kwarg key=value ...]
    python scripts/make_sample_manifest.py validate <dir> [<dir> ...]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path

# The repo root, so ``producer`` can be stored repo-relative and resolved by the
# coverage gate regardless of the current working directory.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Required by the sample-coverage contract; every manifest must carry all five.
REQUIRED_KEYS = ("location", "captured", "content_hash", "producer", "reason")

# The manifest file is metadata, never part of the hashed payload.
MANIFEST_NAME = "manifest.json"


def payload_files(directory: Path) -> list[Path]:
    """The payload files of a sample directory, sorted, manifest excluded.

    A sample directory holds the manifest plus one or more payload files. The
    payload set is every entry that is not the manifest or a grib2io index
    cache sidecar, sorted by name so the hash is stable regardless of
    filesystem order.
    """
    directory = Path(directory)
    return sorted(
        p
        for p in directory.iterdir()
        if p.is_file() and p.name != MANIFEST_NAME and not p.name.endswith(".grib2ioidx")
    )


def content_hash(directory: Path) -> str:
    """``sha256`` over the sample's payload bytes, in a stable order.

    Feeding the file names into the digest (not just the bytes) means a rename
    or a change in the number of payload files is detected as a new hash, not a
    collision of concatenated content.
    """
    h = hashlib.sha256()
    for path in payload_files(directory):
        h.update(path.name.encode("utf-8"))
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def _kind_from_dir(directory: Path) -> str:
    """Infer ``recorded``/``synthetic`` from the sample's location in the tree."""
    parent = Path(directory).resolve().parent.name
    return "synthetic" if parent == "synthetic" else "recorded"


def validate_manifest(directory: Path, *, check_hash: bool = True) -> list[str]:
    """Return a list of problems with a sample's manifest (empty means valid).

    Checks, in order: the manifest exists and parses; every required field is
    present; a synthetic sample records a reason and no location while a
    recorded sample records a location; the producer names a checked-in script
    that actually exists; and, when ``check_hash`` is set, the stored
    ``content_hash`` still matches the payload bytes on disk.
    """
    directory = Path(directory)
    problems: list[str] = []
    mpath = directory / MANIFEST_NAME
    if not mpath.exists():
        return [f"{directory}: no {MANIFEST_NAME}"]
    try:
        manifest = json.loads(mpath.read_text())
    except json.JSONDecodeError as e:  # pragma: no cover - malformed checked-in file
        return [f"{directory}: manifest is not valid JSON: {e}"]
    if not isinstance(manifest, dict):  # pragma: no cover - malformed checked-in file
        return [f"{directory}: manifest is not a JSON object"]

    for key in REQUIRED_KEYS:
        if key not in manifest:
            problems.append(f"{directory}: manifest missing required field {key!r}")

    kind = manifest.get("kind") or _kind_from_dir(directory)
    reason = manifest.get("reason")
    location = manifest.get("location")
    if kind == "synthetic":
        if not reason:
            problems.append(f"{directory}: synthetic sample must record a reason")
        if location is not None:
            problems.append(f"{directory}: synthetic sample must not record a location")
    elif kind == "recorded":
        if not location:
            problems.append(f"{directory}: recorded sample must record a location")

    producer = manifest.get("producer")
    if producer and not (REPO_ROOT / producer).exists():
        problems.append(f"{directory}: producer script not checked in: {producer}")

    if check_hash and "content_hash" in manifest:
        actual = content_hash(directory)
        if actual != manifest["content_hash"]:
            problems.append(
                f"{directory}: content_hash does not match payload "
                f"(recorded {manifest['content_hash'][:12]}…, on disk {actual[:12]}…)"
            )
    return problems


def write_manifest(
    directory: Path,
    *,
    producer: str,
    kind: str | None = None,
    location: str | None = None,
    reason: str | None = None,
    layout: str | None = None,
    load_kwargs: dict | None = None,
    captured: str | None = None,
    requires: str | None = None,
) -> dict:
    """Write ``directory/manifest.json`` for the payload already on disk.

    The payload files must already exist so the hash can be computed. ``kind``
    defaults to the value inferred from the directory name; a recorded sample
    stamps ``captured`` with today's date unless one is supplied. Returns the
    manifest that was written.
    """
    directory = Path(directory)
    kind = kind or _kind_from_dir(directory)
    if kind == "recorded" and captured is None:
        captured = date.today().isoformat()
    manifest = {
        "location": location,
        "captured": captured,
        "content_hash": content_hash(directory),
        "producer": producer,
        "reason": reason or "",
        "kind": kind,
    }
    if layout is not None:
        manifest["layout"] = layout
    if load_kwargs:
        manifest["load_kwargs"] = dict(load_kwargs)
    if requires is not None:
        manifest["requires"] = requires
    (directory / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def _parse_kv(pairs: list[str]) -> dict:
    """Turn ``["group=a/b", "nrows=3"]`` into a dict, leaving values as strings."""
    out = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"--load-kwarg expects key=value, got {pair!r}")
        k, _, v = pair.partition("=")
        out[k] = v
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("write", help="write a manifest for a sample directory")
    w.add_argument("directory", type=Path)
    w.add_argument("--producer", required=True, help="repo-relative checked-in script name")
    w.add_argument("--kind", choices=["recorded", "synthetic"])
    w.add_argument("--location", default=None)
    w.add_argument("--reason", default=None)
    w.add_argument("--layout", default=None)
    w.add_argument("--captured", default=None)
    w.add_argument("--requires", default=None, metavar="MODULE")
    w.add_argument("--load-kwarg", action="append", default=[], metavar="KEY=VALUE")

    v = sub.add_parser("validate", help="validate one or more sample directories")
    v.add_argument("directories", type=Path, nargs="+")
    v.add_argument("--no-hash", action="store_true", help="skip the content-hash check")

    args = parser.parse_args(argv)
    if args.cmd == "write":
        manifest = write_manifest(
            args.directory,
            producer=args.producer,
            kind=args.kind,
            location=args.location,
            reason=args.reason,
            layout=args.layout,
            captured=args.captured,
            requires=args.requires,
            load_kwargs=_parse_kv(args.load_kwarg),
        )
        print(f"wrote {args.directory / MANIFEST_NAME} (kind={manifest['kind']})")
        return 0

    failures = 0
    for directory in args.directories:
        problems = validate_manifest(directory, check_hash=not args.no_hash)
        if problems:
            failures += 1
            for problem in problems:
                print(problem, file=sys.stderr)
        else:
            print(f"ok: {directory}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
