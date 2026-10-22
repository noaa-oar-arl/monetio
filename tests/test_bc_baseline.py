"""Backward-compatibility gate (tasks.md T027; SC-019, C-SCALE-0, FR-004/FR-034).

Every fixture-backed source is re-loaded with *today's* argument set and
compared to the golden recorded before the refactor
(``scripts/record_bc_baseline.py``). This is the additive-only guarantee:
omitting every NEW scale option (``defer``/``parallel``/``concurrent_files``/
``npartitions``/``scheduler``) MUST reproduce the pre-change result exactly.

Run after every later change (Phase 4 onward) — it is the regression net for
the whole feature. The comparison normalizes wall-clock ``history`` timestamps
(the one legitimately non-deterministic part) exactly as the recorder does.
"""

from __future__ import annotations

import pickle
import re
import sys
from pathlib import Path

import pandas as pd
import pytest
import xarray as xr

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT / "scripts"))

from record_bc_baseline import normalize_attrs  # noqa: E402  (reuse the recorder's logic)

import monetio  # noqa: E402

_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?::\d{2})?")


def _golden(path: Path):
    return pickle.loads(path.read_bytes())


def _recorded_sources() -> list[str]:
    """The sources present in ``calls.json`` (the recorder's derived table).

    Read at collection time so a newly sampled source is gated the moment its
    golden exists, without editing this file. Falls back to the sample registry
    when ``calls.json`` has not been recorded yet.
    """
    calls = _ROOT / "tests" / "data" / "baseline" / "calls.json"
    if calls.exists():
        return sorted(__import__("json").loads(calls.read_text()))
    import sys

    sys.path.insert(0, str(_ROOT / "tests"))
    from conftest import SampleRegistry

    return sorted(SampleRegistry().sampled_sources())


@pytest.mark.parametrize("source", _recorded_sources())
def test_bc_baseline_sources_covered(source, bc_calls, baseline_dir):
    """The golden set covers every source the recorder captured."""
    assert (baseline_dir / f"{source}.pkl").exists(), f"no golden for {source!r}"
    assert source in bc_calls, f"{source!r} golden has no entry in calls.json"


def test_bc_baseline_no_unlisted_goldens(bc_calls, baseline_dir):
    """No golden exists that calls.json does not account for."""
    recorded = {p.stem for p in baseline_dir.glob("*.pkl")}
    assert recorded == set(bc_calls), (
        f"baseline goldens {sorted(recorded)} disagree with calls.json {sorted(bc_calls)}"
    )


@pytest.mark.parametrize("source", _recorded_sources())
def test_bc_baseline_equal(source, bc_calls, load_baseline_factory):
    """``monetio.load(source, **today's args)`` equals the pre-refactor golden."""
    if source not in bc_calls:
        pytest.skip(f"no recorded call for {source!r}")
    import sys

    sys.path.insert(0, str(_ROOT / "tests"))
    from conftest import SampleRegistry

    module = SampleRegistry().requires(source)
    if module:
        import importlib.util

        if importlib.util.find_spec(module) is None:
            pytest.skip(f"{source}: sample requires {module!r} (not installed)")
    golden = load_baseline_factory(source)

    kwargs = dict(bc_calls[source])
    files = kwargs.get("files")
    if isinstance(files, str) and "*" in files:
        matches = sorted((_ROOT / Path(files).parent).glob(Path(files).name))
        if not matches:
            pytest.skip(f"fixture glob matched nothing: {files}")
        kwargs["files"] = [str(m) for m in matches]
    elif isinstance(files, list):
        kwargs["files"] = [str(_ROOT / f) for f in files]
    elif isinstance(files, str):
        kwargs["files"] = str(_ROOT / files)

    try:
        result = monetio.load(source, **kwargs)
    except FileNotFoundError as e:  # fixture not present in this checkout
        pytest.skip(f"fixture missing for {source!r}: {e}")

    result = normalize_attrs(result)

    if isinstance(golden, xr.Dataset):
        assert isinstance(result, xr.Dataset), f"{source}: type changed"
        # Identical (structure + attrs, timestamps already normalized) — the
        # strictest form of "nothing about the default output changed".
        xr.testing.assert_identical(golden, result)
    elif isinstance(golden, pd.DataFrame):
        assert isinstance(result, pd.DataFrame), f"{source}: type changed"
        pd.testing.assert_frame_equal(golden, result)
    else:
        assert result == golden, f"{source}: value changed"


def test_new_scale_options_are_absent_from_defaults():
    """The default call path never injects the new scale kwargs.

    Guards against a reader silently defaulting ``defer``/``parallel``/
    ``concurrent_files`` to True and changing behavior for callers that omit
    them (FR-034).
    """
    import inspect

    from monetio.readers.base import GriddedReader, PointReader

    for cls in (GriddedReader, PointReader):
        sig = inspect.signature(cls.open_dataset)
        for opt in ("defer", "parallel", "concurrent_files", "npartitions", "scheduler"):
            if opt in sig.parameters:
                assert (
                    sig.parameters[opt].default is False or sig.parameters[opt].default is None
                ), (
                    f"{cls.__name__}.open_dataset: {opt!r} must default to a no-op "
                    f"(got {sig.parameters[opt].default!r})"
                )
