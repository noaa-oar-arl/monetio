import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.omps import omps_preprocess


@pytest.mark.parametrize("lazy", [True, False])
def test_omps_l2_consistency(lazy):
    """Verify Eager and Lazy consistency for OMPS L2 preprocessing."""
    # Create dummy L2 data
    # Dimensions: scanline, ground_pixel
    n_scan = 3
    n_pixel = 4

    # TAI93: seconds since 1993-01-01
    tai93_start = (pd.Timestamp("2024-01-01") - pd.Timestamp("1993-01-01")).total_seconds()
    time_raw = np.full(n_scan, tai93_start) + np.arange(n_scan) * 100.0

    ds = xr.Dataset(
        {
            "ColumnAmountO3": (
                ("scanline", "ground_pixel"),
                np.array(
                    [
                        [300.0, 400.0, 50.0, 750.0],
                        [300.0, 300.0, 300.0, 300.0],
                        [0.0, 0.0, 0.0, 0.0],
                    ],
                    dtype=np.float32,
                ),
                {"units": " DU ", "long_name": " Total Ozone "},
            ),
            "Time": (("scanline",), time_raw),
            "Latitude": (("scanline", "ground_pixel"), np.zeros((n_scan, n_pixel))),
            "Longitude": (("scanline", "ground_pixel"), np.zeros((n_scan, n_pixel))),
            "RadiativeCloudFraction": (("scanline", "ground_pixel"), np.zeros((n_scan, n_pixel))),
            "QualityFlags": (
                ("scanline", "ground_pixel"),
                np.zeros((n_scan, n_pixel), dtype=np.int32),
            ),
        }
    )

    if lazy:
        ds = ds.chunk({"scanline": 1})

    ds_out = omps_preprocess(ds, product="nmto3_l2")

    # Verify coordinates
    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert "time" in ds_out.coords
    assert ds_out.y.size == n_scan
    assert ds_out.x.size == n_pixel

    # Verify time conversion
    expected_time = pd.Timestamp("2024-01-01")
    assert ds_out.time.values[0] == np.datetime64(expected_time)

    # Verify masking (L2: 50 <= ozone <= 700)
    ozone = ds_out.ozone_column.values
    assert ozone[0, 0] == 300.0
    assert ozone[0, 1] == 400.0
    assert ozone[0, 2] == 50.0
    assert np.isnan(ozone[0, 3])  # 750 > 700

    # Verify Scientific Hygiene (whitespace stripping)
    assert ds_out.ozone_column.attrs["units"] == "DU"
    assert ds_out.ozone_column.attrs["long_name"] == "Total Ozone"

    if lazy:
        assert ds_out.ozone_column.chunks is not None


@pytest.mark.parametrize("lazy", [True, False])
def test_omps_l3_consistency(lazy):
    """Verify Eager and Lazy consistency for OMPS L3 preprocessing."""
    # Create dummy L3 data
    # Dimensions: lat, lon
    n_lat = 180
    n_lon = 360

    ds = xr.Dataset(
        {
            "ColumnAmountO3": (
                ("lat", "lon"),
                np.ones((n_lat, n_lon), dtype=np.float32) * 300.0,
                {"units": "DU"},
            ),
            "Latitude": (("lat",), np.linspace(-90, 90, n_lat)),
            "Longitude": (("lon",), np.linspace(-180, 180, n_lon)),
        },
        attrs={"Date": "2024-01-01"},
    )

    if lazy:
        ds = ds.chunk({"lat": 90})

    ds_out = omps_preprocess(ds, product="nmto3_l3")

    # Verify coordinates (L3 should have 2D lat/lon assigned)
    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert ds_out.latitude.ndim == 2
    assert ds_out.longitude.ndim == 2

    # Verify time assignment from Date attribute
    assert "time" in ds_out.coords
    assert ds_out.time.values[0] == np.datetime64("2024-01-01")

    # Verify masking (L3: ozone >= 0)
    assert not np.isnan(ds_out.ozone_column.values).any()

    if lazy:
        assert ds_out.ozone_column.chunks is not None


def test_omps_l2_quality_masking():
    """Verify L2 quality flag masking."""
    ds = xr.Dataset(
        {
            "ColumnAmountO3": (("scanline", "ground_pixel"), [[300.0, 300.0], [300.0, 300.0]]),
            "Time": (("scanline",), [0.0, 100.0]),
            "Latitude": (("scanline", "ground_pixel"), np.zeros((2, 2))),
            "Longitude": (("scanline", "ground_pixel"), np.zeros((2, 2))),
            "RadiativeCloudFraction": (("scanline", "ground_pixel"), [[0.0, 0.4], [0.0, 0.0]]),
            "QualityFlags": (("scanline", "ground_pixel"), [[0, 0], [137, 138]]),
        }
    )

    ds_out = omps_preprocess(ds, product="nmto3_l2")
    ozone = ds_out.ozone_column.values

    assert ozone[0, 0] == 300.0
    assert np.isnan(ozone[0, 1])  # Cloud fraction 0.4 > 0.3
    assert ozone[1, 0] == 300.0  # Flag 137 < 138
    assert np.isnan(ozone[1, 1])  # Flag 138 is bad
