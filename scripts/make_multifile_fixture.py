"""Generate the multi-file gridded fixture set.

Creates ``tests/data/multifile/`` with ``--count`` (default 60) small,
deterministic NetCDF files sharing the same dimensions/coordinates and one
varying variable, used by the scale tests:

* ``parallel=True`` metadata parsing vs serial — needs
  enough files that the difference is measurable.
* ``concurrent_files`` per-file fan-out.
* evaluating a single time slice reads only the required files
  (each file carries a distinct time, so file-access counting is meaningful).

Deterministic: seeded ``numpy`` RNG, so the set is reproducible from this
checked-in script rather than committed as opaque binaries.

Usage::

    python scripts/make_multifile_fixture.py [--count 60] [--outdir tests/data/multifile]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

NROW, NCOL = 8, 6


def build_file(index: int, start: str, outdir: Path) -> Path:
    """One file = one timestep, IOAPI-ish grid, deterministic data."""
    rng = np.random.default_rng(1234 + index)  # per-file distinct, reproducible
    time = pd.Timestamp(start) + pd.Timedelta(hours=index)

    y = np.arange(NROW, dtype="float64")
    x = np.arange(NCOL, dtype="float64")
    data = rng.random((1, NROW, NCOL), dtype="float32")

    ds = xr.Dataset(
        {
            "O3": (
                ("time", "y", "x"),
                data,
                {"units": "ppbV", "long_name": "Ozone"},
            ),
        },
        coords={
            "time": [time],
            "y": ("y", y, {"units": "m", "standard_name": "projection_y_coordinate"}),
            "x": ("x", x, {"units": "m", "standard_name": "projection_x_coordinate"}),
        },
        attrs={
            "IOAPI_VERSION": "3.2",
            "EXEC_ID": f"multifile_{index:03d}",
            # Full IOAPI grid attributes so the CMAQ reader can decode the grid
            # (grid_from_dataset / _ioapi_grid_from_dataset): a simple
            # Lambert-Conformal projection (GDTYP=2) over the central US.
            "GRIDDESC": "test-grid",
            "GDTYP": 2,
            "P_ALP": 33.0,
            "P_BET": 45.0,
            "P_GAM": -97.0,
            "XCENT": -97.0,
            "YCENT": 40.0,
            "XORIG": -100.0,
            "YORIG": 35.0,
            "XCELL": 1.0,
            "YCELL": 1.0,
            "NCOLS": 6,
            "NROWS": 8,
            "NLAYS": 1,
            "VGTOP": 0.0,
            "VGTYP": 7,
        },
    )
    path = outdir / f"multifile_{index:03d}.nc"
    ds.to_netcdf(path, engine="netcdf4")
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--count", type=int, default=60)
    ap.add_argument(
        "--outdir",
        default=str(Path(__file__).parent.parent / "tests" / "data" / "multifile"),
    )
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    made = [build_file(i, "2023-06-01 00:00", outdir) for i in range(args.count)]
    print(f"wrote {len(made)} files to {outdir}")


if __name__ == "__main__":
    main()
