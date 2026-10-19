"""Record the pre-change behavior baseline.

For every source with a bundled local fixture, calls ``monetio.load(...)`` with
*today's* arguments and stores the result as a golden reference under
``tests/data/baseline/``. ``tests/test_bc_baseline.py`` re-runs the same
calls after the refactor and asserts byte/structural equality — this is the
additive-only gate.

Two artifacts per source:

* ``<source>.pkl``    — the exact returned object (pickle), for faithful
  ``assert_identical`` / ``assert_frame_equal`` comparisons.
* ``<source>.json``  — a human-reviewable structural fingerprint (return type,
  variable names + dtypes, dims + shapes, coordinate names, attrs) with
  ``history`` timestamps normalized, so a diff shows *what* changed without
  loading the pickle.

Timestamp normalization: ``history`` provenance lines embed wall-clock times
(``2026-09-17 00:13:37: ...``), which are legitimately non-deterministic across
runs. They are replaced with ``<TS>`` in BOTH the fingerprint and the pickle
comparison so the gate checks *content*, not the clock.

Run BEFORE any code change::

    python scripts/record_bc_baseline.py
"""

from __future__ import annotations

import json
import pickle
import re
from pathlib import Path

import pandas as pd
import xarray as xr

ROOT = Path(__file__).parent.parent
OUT = ROOT / "tests" / "data" / "baseline"

# (source -> load kwargs) that load offline, derived from the shared sample
# registry (conftest.SampleRegistry) so the golden set automatically covers every
# source that has a sample — the 12 Phase 1 fixtures plus every recorded payload
# under ``tests/data/samples/`` and every synthetic file under
# ``tests/data/synthetic/``. Recording goldens *at sample-creation time* (research
# R10) is what keeps ``breaks.md`` enumerable: a newly sampled source's golden is
# captured before its failure-policy edits land, so the diff reflects only those
# edits. Verified against the conda/venv environment; every entry returns.
import sys  # noqa: E402

sys.path.insert(0, str(ROOT / "tests"))
from conftest import SampleRegistry as _SampleRegistry  # noqa: E402


def _build_bc_sources() -> dict[str, dict]:
    registry = _SampleRegistry()
    table: dict[str, dict] = {}
    for source in sorted(registry.sampled_sources()):
        found = registry.resolve(source)
        if found is None:
            continue
        files = found["files"]

        # The recorded table is checked in, so every path is stored relative to
        # the repo root: an absolute path would name this machine's checkout and
        # resolve to nothing anywhere else. Globs are expanded (the payload set
        # is fixed at record time) and the results made relative too.
        def _rel(path) -> str:
            resolved = Path(path) if Path(path).is_absolute() else ROOT / path
            return resolved.resolve().relative_to(ROOT.resolve()).as_posix()

        if isinstance(files, str) and "*" in files:
            files = sorted(_rel(p) for p in (ROOT / Path(files).parent).glob(Path(files).name))
        elif isinstance(files, list):
            files = [_rel(f) for f in files]
        else:
            files = _rel(files)
        kwargs = {"files": files, **found.get("kwargs", {})}
        table[source] = kwargs
    return table


BC_SOURCES: dict[str, dict] = _build_bc_sources()

_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?::\d{2})?")


def _norm(value):
    """Recursively normalize wall-clock timestamps out of provenance strings."""
    if isinstance(value, str):
        return _TS_RE.sub("<TS>", value)
    if isinstance(value, dict):
        return {k: _norm(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_norm(v) for v in value]
    return value


def normalize_attrs(obj):
    """Return a copy of ``obj`` materialized in-memory with normalized history attrs.

    ``.load()`` swaps h5py/lazy-backed arrays for numpy *without* changing
    values, dtypes, coords, or attrs — required because the raw Dataset holds
    open h5py handles that cannot be pickled. Timestamps in provenance attrs
    are then replaced so the pickled golden comparison is clock-independent.
    """
    if isinstance(obj, xr.Dataset):
        obj = obj.load().copy()
        obj.attrs = _norm(dict(obj.attrs))
        for name in obj.variables:
            obj[name].attrs = _norm(dict(obj[name].attrs))
        return obj
    if isinstance(obj, pd.DataFrame):
        obj = obj.copy()
        obj.attrs = _norm(dict(obj.attrs))
        return obj
    if isinstance(obj, pd.Series):  # point readers may return a frame per file
        obj = obj.copy()
        obj.attrs = _norm(dict(obj.attrs))
        return obj
    return obj


def fingerprint(obj) -> dict:
    fp = {"type": type(obj).__name__}
    if isinstance(obj, xr.Dataset):
        fp["dims"] = {str(k): int(v) for k, v in obj.sizes.items()}
        fp["data_vars"] = {str(k): str(v.dtype) for k, v in obj.data_vars.items()}
        fp["coords"] = {str(k): str(v.dtype) for k, v in obj.coords.items()}
        fp["attrs"] = _norm({str(k): _scalar_repr(v) for k, v in obj.attrs.items()})
        fp["var_attrs"] = {
            str(k): {str(a): _scalar_repr(v) for a, v in obj[k].attrs.items()}
            for k in list(obj.variables)
        }
    elif isinstance(obj, pd.DataFrame):
        fp["n_rows"] = int(len(obj))
        fp["n_cols"] = int(obj.shape[1])
        fp["columns"] = {str(c): str(obj[c].dtype) for c in obj.columns}
        fp["attrs"] = _norm({str(k): _scalar_repr(v) for k, v in obj.attrs.items()})
    else:
        fp["repr"] = _norm(repr(obj))[:2000]
    return fp


def _scalar_repr(v):
    """Deterministic, JSON-safe rendering of an attr value."""
    if isinstance(v, str | int | float | bool) or v is None:
        return v
    if isinstance(v, list | tuple):
        return [_scalar_repr(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _scalar_repr(x) for k, x in v.items()}
    try:
        return ["ndarray", str(v.dtype), list(v.shape), _norm(repr(v.tolist()))]
    except Exception:
        return _norm(repr(v))[:500]


def main(argv: list[str] | None = None) -> None:
    import argparse

    import monetio  # noqa: E402  (import after cwd is set; project package)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help=(
            "Only record goldens for sampled sources that do not already have a "
            "<source>.pkl under tests/data/baseline/. Existing goldens — the "
            "pre-change regression reference — are left untouched and merged into "
            "the rewritten index.json/calls.json."
        ),
    )
    args = parser.parse_args(argv)

    OUT.mkdir(parents=True, exist_ok=True)

    existing_index: dict = {}
    existing_calls: dict = {}
    index_path = OUT / "index.json"
    calls_path = OUT / "calls.json"
    if args.missing_only:
        if index_path.exists():
            existing_index = json.loads(index_path.read_text())
        if calls_path.exists():
            existing_calls = json.loads(calls_path.read_text())

    index = dict(existing_index)
    calls = dict(existing_calls)
    recorded = 0
    for source, kwargs in BC_SOURCES.items():
        if args.missing_only and (OUT / f"{source}.pkl").exists():
            # Keep the pre-change golden as the regression reference; just make
            # sure the derived tables still account for it.
            index.setdefault(source, existing_index.get(source, {}))
            calls.setdefault(source, kwargs)
            continue
        print(f"recording {source} ...", flush=True)
        result = monetio.load(source, **kwargs)
        normalized = normalize_attrs(result)
        (OUT / f"{source}.pkl").write_bytes(pickle.dumps(normalized))
        index[source] = fingerprint(normalized)
        calls[source] = kwargs
        recorded += 1
        print(f"  -> {index[source]['type']}")

    index_path.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
    calls_path.write_text(json.dumps(calls, indent=2) + "\n")
    print(
        f"\nrecorded {recorded} new golden(s); {len(calls)} total in "
        f"index.json + calls.json under {OUT}"
    )


if __name__ == "__main__":
    main()
