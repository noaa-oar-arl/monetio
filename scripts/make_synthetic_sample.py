"""Generate synthetic offline samples for formats that cannot be redistributed.

Some sources read a model output format whose real files are either too large to
check in, produced by a research model nobody may redistribute, or simply not
hosted at a stable public URL. For those, the sample-coverage contract (C-SAMP-11,
FR-022) accepts a *synthetic reproduction of the format* as long as the generator
is checked in and the reason is recorded in the manifest — the same discipline the
60-file multi-file gridded fixture already follows (``scripts/make_multifile_fixture.py``).

A synthetic sample certifies the parser against the format *as this generator
understands it*, not against a live service, so it cannot detect upstream format
drift; where a real payload can be recorded, prefer ``scripts/record_samples.py``.

Each generator builds the smallest file that drives every stage of the reader
(header/grid decode, variable harmonization, coordinate attach, shape conversion,
provenance), deterministically (seeded RNG), and writes it with a manifest under
``tests/data/synthetic/<source>/``.

Usage::

    python scripts/make_synthetic_sample.py <source> [<source> ...]
    python scripts/make_synthetic_sample.py --list
    python scripts/make_synthetic_sample.py --all

Adding a source means adding a ``build_<source>`` function and a ``SYNTHETICS``
entry naming it, its reason, and the ``load_kwargs`` (if any) the reader needs,
in the appropriate family module under ``scripts/sample_specs/``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from make_sample_manifest import write_manifest  # noqa: E402

SYNTHETIC_DIR = REPO_ROOT / "tests" / "data" / "synthetic"
PRODUCER = "scripts/make_synthetic_sample.py"


def _synthetics(family: str | None) -> dict[str, tuple]:
    """The SYNTHETICS table, optionally scoped to one family module.

    See the matching note in ``record_samples.py``: scoping keeps workers
    capturing different families from failing on a module one of them is editing.
    """
    if family is None:
        from sample_specs import SYNTHETICS as _ALL

        return dict(_ALL)
    import importlib

    mod = importlib.import_module(f"sample_specs.{family}")
    return dict(getattr(mod, "SYNTHETICS", {}))


def generate(source: str, *, family: str | None = None) -> Path | None:
    """Generate one source's synthetic sample and write its manifest."""
    entry = _synthetics(family).get(source)
    if entry is None:
        print(f"  {source}: no synthetic builder — skipping", file=sys.stderr)
        return None
    builder, reason, load_kwargs = entry
    outdir = SYNTHETIC_DIR / source
    outdir.mkdir(parents=True, exist_ok=True)
    builder(outdir)
    write_manifest(
        outdir,
        producer=PRODUCER,
        kind="synthetic",
        reason=reason,
        load_kwargs=load_kwargs,
    )
    return outdir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sources", nargs="*")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--list", action="store_true", help="list generated sources and exit")
    parser.add_argument(
        "--family",
        default=None,
        help="only consult this sample_specs module (keeps concurrent workers isolated)",
    )
    args = parser.parse_args(argv)

    table = _synthetics(args.family)
    if args.list:
        for name in sorted(table):
            print(name)
        return 0
    sources = list(table) if args.all else args.sources
    if not sources:
        parser.error("name sources to generate or pass --all")
    failed = []
    for source in sources:
        print(f"generating {source} ...")
        try:
            if generate(source, family=args.family) is None:
                failed.append(source)
        except Exception as e:  # noqa: BLE001 - a generation run reports per-source failures
            print(f"  {source}: FAILED {type(e).__name__}: {e}", file=sys.stderr)
            failed.append(source)
    if failed:
        print(f"\n{len(failed)} source(s) not generated: {sorted(failed)}", file=sys.stderr)
        return 1
    print("\ndone")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
