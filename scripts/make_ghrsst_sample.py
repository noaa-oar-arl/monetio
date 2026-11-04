"""Generate the tiny synthetic GHRSST sample used by offline conformance tests."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "tests" / "data" / "samples" / "ghrsst" / "ghrsst_ncei_avhrr_anl.nc"


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    sst = np.array([[[280.0, 281.0, np.nan], [282.0, 283.0, 284.0]]], dtype="f4")
    ds = xr.Dataset(
        {
            "analysed_sst": (("time", "lat", "lon"), sst, {"units": "K"}),
            "analysis_error": (("time", "lat", "lon"), np.full_like(sst, 0.25)),
            "mask": (("time", "lat", "lon"), np.ones_like(sst, dtype="i1")),
            "sea_ice_fraction": (("time", "lat", "lon"), np.zeros_like(sst)),
        },
        coords={
            "time": pd.DatetimeIndex(["2024-02-28"]),
            "lat": ("lat", np.array([-0.125, 0.125], dtype="f4"), {"units": "degrees_north"}),
            "lon": (
                "lon",
                np.array([10.125, 10.375, 10.625], dtype="f4"),
                {"units": "degrees_east"},
            ),
        },
        attrs={
            "title": "GHRSST Level 4 NCEI AVHRR_OI synthetic contract sample",
            "product_id": "ghrsst_ncei_avhrr_anl",
        },
    )
    ds["analysed_sst"].encoding.update(
        {"dtype": "int16", "scale_factor": 0.01, "add_offset": 273.15, "_FillValue": -32768}
    )
    ds.to_netcdf(OUT, engine="h5netcdf")
    ds.close()


if __name__ == "__main__":
    main()
