"""Capture offline samples of remote payloads for the conformance suite.

The rule this serves (sample-coverage contract C-SAMP-5): a source whose data is
normally retrieved over a network must have its *retrieved bytes* checked in, so
that both the location construction and the parser run offline against what the
service actually returns. A synthetic file cannot certify the real payload
quirks; only a recording can.

This script performs the one-time capture. It is run with network access during
sample creation (and again to refresh a stale sample), never from the offline
test suite. Each captured payload is written under
``tests/data/samples/<source>/`` together with a ``manifest.json`` produced by
``scripts/make_sample_manifest.py`` recording the location, capture date, and a
content hash — the hash being how staleness is detected on a later refresh run.

Only core dependencies are used (``requests`` for HTTP(S), ``fsspec`` for
``s3://``/``ftp://``), matching the constraint that capture adds no new
dependency.

Procedures
----------
Capture (one-time, needs network)::

    python scripts/record_samples.py <source> [<source> ...]
    python scripts/record_samples.py --all

Refresh a stale sample (needs network)::

    python scripts/record_samples.py --refresh <source>
    # re-fetches; if the content hash changed, the manifest is rewritten and the
    # golden for that source should be re-recorded and the diff reviewed.

The ``CAPTURES`` tables under ``scripts/sample_specs/`` hold the per-source
knowledge: the exact remote location(s) to fetch (so a change to date
normalization that builds a wrong URL is caught), the local file name to store
each under, any ``load_kwargs`` the reader needs beyond ``files=`` (a nested
``group``, a ``product`` selector), the ``layout`` for a multi-layout source,
and an optional line cap to keep a text payload minimal. Adding a source means
adding an entry to the right family module and verifying the load exercises the
reader (a 0-variable result means the sample is not sufficient — see the
MOPITT/TOLNet precedents).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Repo root and the samples tree, so the script works from any cwd.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from make_sample_manifest import write_manifest  # noqa: E402

SAMPLES_DIR = REPO_ROOT / "tests" / "data" / "samples"


def _captures(family: str | None) -> dict[str, dict]:
    """The CAPTURES table to work from, optionally scoped to one family module.

    Scoping matters when several sources are captured side by side by separate
    workers: importing the whole ``sample_specs`` package imports every family
    module, so a module another worker is mid-edit on would break an unrelated
    capture. Naming the family loads only that module's table.
    """
    if family is None:
        from sample_specs import CAPTURES as _ALL

        return dict(_ALL)
    import importlib

    mod = importlib.import_module(f"sample_specs.{family}")
    return dict(getattr(mod, "CAPTURES", {}))


# The producer recorded in each manifest: a reviewer re-runs this script.
PRODUCER = "scripts/record_samples.py"


def _fetch(url: str, dest: Path) -> None:
    """Retrieve ``url`` to ``dest`` using only core dependencies.

    HTTP(S) goes through ``requests``; ``s3://``/``ftp://`` and other fsspec
    protocols through ``fsspec`` (anonymous for public object stores). The bytes
    are written verbatim so the recorded payload is exactly what the service
    served.
    """
    if url.startswith(("http://", "https://")):
        import requests

        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
    else:
        import fsspec

        anon = url.startswith("s3://")
        with fsspec.open(url, "rb", anon=anon) if anon else fsspec.open(url, "rb") as f:
            dest.write_bytes(f.read())


def _fetch_first_grib2_message(url: str, dest: Path) -> None:
    """Retrieve only the leading GRIB2 record of ``url``.

    A GRIB2 file is a concatenation of self-contained messages, each opening
    with the four-byte magic ``GRIB`` and carrying its total length in the
    eight-byte field at offset 8 of section 0. Operational forecast products are
    tens to hundreds of megabytes because they stack hundreds of fields, but a
    single message is a complete, valid file that exercises the full decode path
    (grid definition, packing, projection, coordinate generation). Streaming
    until the first message's declared length is satisfied keeps the recorded
    payload to one field -- the minimum that still runs every load stage --
    without downloading the whole product. The retrieved bytes are a verbatim
    prefix of what the service served, so the sample remains a real recording.
    """
    import struct

    anon = url.startswith("s3://")
    reader = (
        __import__("fsspec").open(url, "rb", anon=anon)
        if anon
        else __import__("fsspec").open(url, "rb")
    )
    total = None
    buf = bytearray()
    with reader as f:
        while True:
            chunk = f.read(1 << 18)
            if not chunk:
                break
            buf += chunk
            if total is None:
                if len(buf) >= 16 and buf[:4] == b"GRIB":
                    total = struct.unpack(">Q", bytes(buf[8:16]))[0]
                else:
                    continue
            if total is not None and len(buf) >= total:
                break
    if total is None:
        raise ValueError(f"{url}: no GRIB2 section-0 header found")
    message = bytes(buf[:total])
    if message[-4:] != b"7777":
        raise ValueError(f"{url}: truncated message missing 7777 end marker")
    dest.write_bytes(message)


def _unpack(raw: Path, dest: Path, how: str) -> None:
    """Extract the single member of a zip/gz archive into ``dest``.

    Several archives ship one payload file; the reader parses a plain file
    through the same code path as the archive, so a trimmed plain file keeps the
    sample minimal. The raw archive is removed afterward.
    """
    if how == "zip":
        import zipfile

        with zipfile.ZipFile(raw) as z:
            dest.write_bytes(z.read(z.namelist()[0]))
    elif how == "gz":
        import gzip

        with gzip.open(raw, "rb") as g:
            dest.write_bytes(g.read())
    else:  # pragma: no cover - guarded by the CAPTURES table
        raise ValueError(f"unknown unpack kind {how!r}")
    raw.unlink()


def _trim_text(path: Path, max_lines: int) -> None:
    """Keep the first ``max_lines`` lines of a text payload (C-SAMP-12 minimality).

    Only used for line-oriented data where a header plus a few rows exercises
    every parse stage; never for binary or grouped formats, where an arbitrary
    truncation would not be a valid file.
    """
    lines = path.read_text(errors="replace").splitlines(keepends=True)
    if len(lines) > max_lines:
        path.write_text("".join(lines[:max_lines]))


def capture(source: str, *, refresh: bool = False, family: str | None = None) -> Path | None:
    """Capture one source's payload(s) and write its manifest.

    Returns the sample directory, or ``None`` when the source has no ``CAPTURES``
    entry. An existing sample is left untouched unless ``refresh`` is set, so a
    routine run never silently rewrites a checked-in payload.
    """
    spec = _captures(family).get(source)
    if spec is None:
        print(f"  {source}: no CAPTURES entry — skipping", file=sys.stderr)
        return None
    outdir = SAMPLES_DIR / source
    if outdir.exists() and not refresh:
        existing = outdir / "manifest.json"
        if existing.exists():
            print(f"  {source}: sample already present (use --refresh to re-capture)")
            return outdir
    outdir.mkdir(parents=True, exist_ok=True)

    unpack = spec.get("unpack", {})
    max_lines = spec.get("max_lines", {})
    transform = spec.get("transform")
    for url, name in spec["urls"]:
        dest = outdir / name
        if transform == "first-grib2-message":
            print(f"  fetching first GRIB2 message of {url} -> {name}")
            _fetch_first_grib2_message(url, dest)
            continue
        print(f"  fetching {url} -> {name}")
        if name in unpack:
            raw = outdir / (name + ".raw")
            _fetch(url, raw)
            _unpack(raw, dest, unpack[name])
        else:
            _fetch(url, dest)
        if name in max_lines:
            _trim_text(dest, max_lines[name])

    write_manifest(
        outdir,
        producer=PRODUCER,
        kind="recorded",
        location=spec["urls"][0][0],
        reason=spec.get("note"),
        layout=spec.get("layout"),
        load_kwargs=spec.get("load_kwargs"),
        requires=spec.get("requires"),
    )
    return outdir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sources", nargs="*", help="source names to capture")
    parser.add_argument("--all", action="store_true", help="capture every CAPTURES source")
    parser.add_argument(
        "--refresh", action="store_true", help="re-fetch even when a sample already exists"
    )
    parser.add_argument(
        "--family",
        default=None,
        help="only consult this sample_specs module (keeps concurrent workers isolated)",
    )
    args = parser.parse_args(argv)

    table = _captures(args.family)
    sources = list(table) if args.all else args.sources
    if not sources:
        parser.error("name sources to capture or pass --all")
    failed = []
    for source in sources:
        print(f"capturing {source} ...")
        try:
            if capture(source, refresh=args.refresh, family=args.family) is None:
                failed.append(source)
        except Exception as e:  # noqa: BLE001 - a capture run reports per-source failures
            print(f"  {source}: FAILED {type(e).__name__}: {e}", file=sys.stderr)
            failed.append(source)
    if failed:
        print(f"\n{len(failed)} source(s) not captured: {sorted(failed)}", file=sys.stderr)
        return 1
    print("\ndone")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
