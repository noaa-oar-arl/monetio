"""US1 lazy-import acceptance tests (tasks.md T010).

Every assertion runs in a **fresh interpreter** (``run_in_fresh_python`` /
C-LAZY-7): in-process, a second ``import monetio`` is a dict lookup and the
module-count/timing signals are meaningless. These tests encode the three
success criteria for the lazy-resolution feature:

* SC-001 — a bare ``import monetio`` pulls in zero reader modules and zero
  legacy facade submodules.
* SC-002 — it also pulls in none of the optional third-party top-levels
  (C-LAZY-1), whether or not they happen to be installed.
* SC-003 — wall-clock import time is at least 50% below the recorded
  pre-change baseline (``tests/data/baseline_import.json``).
"""

from __future__ import annotations

import statistics

import pytest

# The optional top-levels that must never appear after a bare import
# (contracts/lazy-resolution.md C-LAZY-1). Kept in sync with
# scripts/record_import_baseline.py::OPTIONAL_TOP_LEVELS. The experimental
# lazy-array backend removed in FR-039 is deliberately absent — it is no longer
# a "kept lazy" dependency at all.
OPTIONAL_TOP_LEVELS = [
    "s3fs",
    "timezonefinder",
    "pyhdf",
    "scipy",
    "virtualizarr",
    "icechunk",
    "obstore",
    "ujson",
    "zarr",
    "netCDF4",
    "h5py",
    "h5netcdf",
    "distributed",
    "click",
    "statsmodels",
]

_FACADE_PACKAGES = ("obs", "models", "sat", "profile")


def _count_expr(prefixes):
    # child-side expression producing an int from `mods` (list of str)
    conds = " or ".join(f"m.startswith({p!r})" for p in prefixes)
    return f"len([m for m in mods if {conds}])"


def test_sc001_bare_import_loads_no_readers_or_facades(run_in_fresh_python):
    """SC-001: ``import monetio`` imports zero readers and zero facade submodules."""
    code = (
        "print(json.dumps({"
        + f"'readers': {_count_expr(['monetio.readers.'])}, "
        + "'facades': "
        + _count_expr([f"monetio.{p}." for p in _FACADE_PACKAGES])
        + "}))"
    )
    result = run_in_fresh_python(code)
    assert result["readers"] == 0, result
    assert result["facades"] == 0, result


def test_sc002_bare_import_loads_no_optional_deps(run_in_fresh_python):
    """SC-002: none of the optional third-party top-levels load on bare import."""
    code = (
        "opt = " + repr(OPTIONAL_TOP_LEVELS) + "\n"
        "present = [n for n in opt if any(m == n or m.startswith(n + '.') for m in mods)]\n"
        "print(json.dumps({'present': present}))"
    )
    result = run_in_fresh_python(code)
    assert result["present"] == [], f"optional deps loaded on bare import: {result['present']}"


def test_sc003_import_time_reduced_vs_baseline(run_in_fresh_python, baseline_import):
    """SC-003: median bare-import time is >=50% below the recorded baseline."""
    median_before = baseline_import["median_seconds"]

    reps = 10
    times = []
    for _ in range(reps):
        result = run_in_fresh_python("print(json.dumps({'s': import_seconds}))", time_import=True)
        times.append(result["s"])

    median_after = statistics.median(times)
    reduction = 1 - median_after / median_before
    assert reduction >= 0.5, (
        f"SC-003 not met: median after={median_after:.3f}s vs before={median_before:.3f}s "
        f"(only {reduction * 100:.0f}% reduction, need >=50%)"
    )


@pytest.mark.parametrize("facade", _FACADE_PACKAGES)
def test_importing_one_facade_attr_stays_lazy(run_in_fresh_python, facade):
    """Touching a single facade attribute must not import its siblings.

    Guards the whole point of the lazy facades (C-LAZY-4): resolving
    ``monetio.<facade>.<one>`` imports exactly that submodule, not all of them.
    """
    # airnow/cmaq/goes/tolnet are the first submodule of each facade package.
    attr = {"obs": "airnow", "models": "cmaq", "sat": "goes", "profile": "tolnet"}[facade]
    code = (
        f"importlib.import_module('monetio.{facade}').{attr}\n"
        "print(json.dumps({" + f"'facades': {_count_expr([f'monetio.{facade}.'])}" + "}))"
    )
    result = run_in_fresh_python("import importlib\n" + code)
    # exactly the one facade submodule resolved (plus any it imports itself);
    # crucially NOT the full package (which had 20/12/12/5 at baseline).
    assert result["facades"] < {"obs": 20, "models": 12, "sat": 12, "profile": 5}[facade]
