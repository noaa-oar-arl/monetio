import numpy as np
import xarray as xr

from monetio.readers.rrfs import RRFSReader, rrfs_preprocess


def test_rrfs_build_urls():
    reader = RRFSReader()
    urls = reader.build_urls(
        dates="2026-03-28", hour=0, lead_time=1, product="prslev.3km", domain="conus"
    )
    assert len(urls) == 1
    assert "rrfs.20260328" in urls[0]
    assert "rrfs.t00z.prslev.3km.f001.conus.grib2" in urls[0]
    assert "noaa-rrfs-pds" in urls[0]


def test_rrfs_eager_lazy_consistency():
    """Verify rrfs_preprocess and harmonize consistency between NumPy and Dask."""
    # Create mock dataset with NCEP-style names
    data = np.random.rand(5, 5)
    ds_eager = xr.Dataset(
        {
            "TMP": (("lat", "lon"), data),
            "O3MR": (("lat", "lon"), data * 1e-6),
        },
        coords={
            "lat": np.linspace(30, 40, 5),
            "lon": np.linspace(-100, -90, 5),
        },
    )
    # Add attributes that need cleaning
    ds_eager["TMP"].attrs["units"] = " K "
    ds_eager["O3MR"].attrs["units"] = " kg/kg "
    ds_eager.attrs["history"] = "Original"

    # 1. Eager Processing
    reader = RRFSReader()
    ds_eager_proc = rrfs_preprocess(ds_eager.copy())
    ds_eager_proc = reader.harmonize(ds_eager_proc)

    # 2. Lazy Processing
    ds_lazy = ds_eager.chunk({"lat": 2, "lon": 2})
    ds_lazy_proc = rrfs_preprocess(ds_lazy.copy())
    ds_lazy_proc = reader.harmonize(ds_lazy_proc)

    # Verify Laziness
    assert ds_lazy_proc["temperature"].chunks is not None

    # Compare results
    xr.testing.assert_allclose(ds_eager_proc, ds_lazy_proc.compute())

    # Verify Harmonization (NCEP standards from gfs.py)
    assert "latitude" in ds_eager_proc.coords
    assert "longitude" in ds_eager_proc.coords
    assert "temperature" in ds_eager_proc.data_vars
    assert "ozone" in ds_eager_proc.data_vars

    # Verify Hygiene
    assert ds_eager_proc["temperature"].attrs["units"] == "K"

    # Verify History
    assert "Preprocessed RRFS data." in ds_eager_proc.attrs["history"]
    assert "Original" in ds_eager_proc.attrs["history"]
