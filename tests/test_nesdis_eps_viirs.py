import dask.array as da
import numpy as np
import xarray as xr

from monetio.readers.nesdis_eps_viirs import NESDISEPSVIIRSReader, nesdis_eps_viirs_preprocess


def test_nesdis_eps_viirs_preprocess():
    # Create dummy dataset
    ds = xr.Dataset(
        {
            "aot_ip_out": (("y", "x"), np.array([[0.1, 0.2], [-0.1, 0.5]])),
        },
        attrs={
            "time_coverage_start": "2024-01-01T12:00:00Z",
        },
    )
    # Important: standardize_satellite_coords expects 'y' and 'x' dimensions
    # if they are already present, or will rename them from 'Rows'/'Columns' etc.
    # In nesdis_eps_viirs_preprocess, it specifically looks for y and x sizes
    # but the way I constructed it here it should be fine.

    ds_out = nesdis_eps_viirs_preprocess(ds)

    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert "time" in ds_out.coords
    assert "aod_550" in ds_out.data_vars

    # Check masking of negative values
    # For aod_550, it might be (time, y, x)
    aod = ds_out.aod_550.squeeze()
    assert np.isnan(aod.values[1, 0])
    assert aod.values[0, 0] == 0.1

    # Check standard attributes
    assert ds_out.aod_550.attrs["units"] == "1"


def test_nesdis_eps_viirs_eager_lazy_consistency():
    # Create dummy dataset
    data = np.random.rand(10, 10).astype("f4")
    ds_eager = xr.Dataset(
        {
            "aot_ip_out": (("y", "x"), data),
        },
        attrs={
            "time_coverage_start": "2024-01-01T12:00:00Z",
        },
    )

    ds_lazy = ds_eager.chunk({"y": 5, "x": 5})

    # Run preprocess
    out_eager = nesdis_eps_viirs_preprocess(ds_eager)
    out_lazy = nesdis_eps_viirs_preprocess(ds_lazy)

    # Check consistency
    xr.testing.assert_allclose(out_eager, out_lazy.compute())
    assert isinstance(out_lazy.aod_550.data, da.Array)


def test_nesdis_eps_viirs_build_urls():
    reader = NESDISEPSVIIRSReader()
    urls = reader.build_urls("2024-01-01")
    assert len(urls) == 1
    assert "2024/npp_eaot_ip_gridded_0.25_20240101.high.nc" in urls[0]
