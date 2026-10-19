"""Record the pre-change ``import monetio`` baseline.

Measures, in FRESH subprocesses (in-process measurement is invalid — a second
``import monetio`` is a dict lookup), per the lazy-resolution contract:

* wall time of ``import monetio`` (``time.perf_counter`` immediately around it),
* total loaded module count,
* count of loaded ``monetio.readers.*`` modules,
* count of loaded legacy facade submodules (obs/models/sat/profile),
* presence of optional third-party top-levels after a bare import.

Writes ``tests/data/baseline_import.json``. Run BEFORE any code change; the
post-change tests (tests/test_lazy_import.py) compare against this file.

Usage::

    python scripts/record_import_baseline.py [--reps 10] [--out tests/data/baseline_import.json]
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
from pathlib import Path

# Optional top-levels that MUST NOT be present after a bare import.
# cubed is intentionally absent from this list: it was removed
# entirely, so it is not a "kept lazy" dependency any more.
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

_CHILD = """
import json, sys, time
t0 = time.perf_counter()
import monetio
t1 = time.perf_counter()
import sys as s
mods = list(s.modules)
out = {
    "seconds": t1 - t0,
    "n_modules": len(mods),
    "n_readers": len([m for m in mods if m.startswith("monetio.readers.")]),
    "n_obs": len([m for m in mods if m.startswith("monetio.obs.")]),
    "n_models": len([m for m in mods if m.startswith("monetio.models.")]),
    "n_sat": len([m for m in mods if m.startswith("monetio.sat.")]),
    "n_profile": len([m for m in mods if m.startswith("monetio.profile.")]),
    "optional_present": [
        name for name in json.loads(sys.argv[1])
        if any(m == name or m.startswith(name + ".") for m in mods)
    ],
}
print(json.dumps(out))
"""


def measure(reps: int) -> dict:
    runs = []
    for i in range(reps):
        proc = subprocess.run(
            [sys.executable, "-W", "ignore", "-c", _CHILD, json.dumps(OPTIONAL_TOP_LEVELS)],
            capture_output=True,
            text=True,
            check=True,
        )
        runs.append(json.loads(proc.stdout.strip().splitlines()[-1]))
        print(f"  run {i + 1:2d}/{reps}: {runs[-1]['seconds']:.3f}s", flush=True)

    times = sorted(r["seconds"] for r in runs)
    median = statistics.median(times)
    rep = min(runs, key=lambda r: abs(r["seconds"] - median))
    return {
        "python": sys.version.split()[0],
        "executable": sys.executable,
        "reps": reps,
        "median_seconds": median,
        "min_seconds": times[0],
        "max_seconds": times[-1],
        "n_modules": rep["n_modules"],
        "monetio_readers_loaded": rep["n_readers"],
        "facade_submodules_loaded": {
            "obs": rep["n_obs"],
            "models": rep["n_models"],
            "sat": rep["n_sat"],
            "profile": rep["n_profile"],
        },
        "optional_present_after_bare_import": sorted(
            {o for r in runs for o in r["optional_present"]}
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--reps", type=int, default=10)
    ap.add_argument(
        "--out", default=str(Path(__file__).parent.parent / "tests/data/baseline_import.json")
    )
    args = ap.parse_args()

    print(f"Measuring import monetio in {args.reps} fresh subprocesses ({sys.executable})")
    data = measure(args.reps)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps(data, indent=2))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
