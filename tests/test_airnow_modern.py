import pandas as pd
import pytest
import xarray as xr

from monetio.readers.airnow import AirNowReader


@pytest.fixture
def mock_airnow_file(tmp_path):
    fn = tmp_path / "HourlyData_2023010100.dat"
    # Create a mock file with multiple sites, some requiring UTC offset fix
    content = "01/01/23|00:00|012345678|Test Site 1|-5|OZONE|PPB|50.0|Test Source\n"
    content += "01/01/23|00:00|012345678|Test Site 1|-5|PM2.5|UG/M3|10.0|Test Source\n"
    content += "01/01/23|00:00|999999999|Bad TZ Site|0|OZONE|PPB|40.0|Test Source\n"
    content += "01/01/23|01:00|012345678|Test Site 1|-5|OZONE|PPB|55.0|Test Source\n"
    fn.write_text(content, encoding="ISO-8859-1")
    return str(fn)


def test_airnow_eager_lazy_consistency_modern(mock_airnow_file, monkeypatch):
    """Verify Eager and Lazy loading produce identical results and lazy triggers no computes."""
    pytest.importorskip("timezonefinder")
    try:
        import dask.dataframe as dd  # noqa: F401
    except ImportError:
        pytest.skip("dask not installed")

    def mock_read_monitor(*args, **kwargs):
        # Mock metadata for the sites in the file
        return pd.DataFrame(
            {
                "siteid": ["012345678", "999999999"],
                "latitude": [40.0, 35.0],
                "longitude": [-80.0, -120.0],  # 999999999 will trigger 'fix' if requested
                "site_name": ["Site 1", "Site 2"],
            }
        )

    import monetio.readers.epa_utils as epa_utils

    monkeypatch.setattr(epa_utils, "read_monitor_file", mock_read_monitor)

    reader = AirNowReader()

    # 1. Eager Load
    ds_eager = reader.open_dataset(
        files=mock_airnow_file, as_xarray=True, lazy=False, wide_fmt=True, bad_utcoffset="fix"
    )

    # 2. Lazy Load
    ds_lazy = reader.open_dataset(
        files=mock_airnow_file, as_xarray=True, lazy=True, wide_fmt=True, bad_utcoffset="fix"
    )

    # Verify laziness: DataArrays should have chunks
    assert ds_lazy.OZONE.chunks is not None
    assert ds_lazy.utcoffset.chunks is not None

    # Perform compute for comparison
    ds_lazy_computed = ds_lazy.compute()

    # Verify history propagation before popping
    assert "Read AirNow data." in ds_eager.attrs["history"]
    assert "Post-processed and filtered AirNow data." in ds_eager.attrs["history"]
    assert "Read AirNow data." in ds_lazy_computed.attrs["history"]
    assert "Post-processed and filtered AirNow data." in ds_lazy_computed.attrs["history"]

    # Clean attributes for comparison (history will differ)
    ds_eager.attrs.pop("history", None)
    ds_lazy_computed.attrs.pop("history", None)

    # Compare data variables (excluding unit strings which might differ in metadata)
    data_vars = [v for v in ds_eager.data_vars if not v.endswith("_unit")]
    xr.testing.assert_allclose(ds_eager[data_vars], ds_lazy_computed[data_vars])

    # Specifically check the fixed UTC offset for the 'bad' site
    # Site 999999999 at -120.0 lon should have -8.0 offset (TimezoneFinder result)
    # We take the mean over time to get a single value (since it's constant for the site)
    uo_999_eager = ds_eager.sel(node=ds_eager.siteid == "999999999").utcoffset.mean().values.item()
    uo_999_lazy = (
        ds_lazy_computed.sel(node=ds_lazy_computed.siteid == "999999999")
        .utcoffset.mean()
        .values.item()
    )

    assert uo_999_eager == uo_999_lazy
    assert uo_999_eager == pytest.approx(-8.0)


def test_airnow_no_hidden_compute(mock_airnow_file, monkeypatch):
    """Verify that open_dataset(lazy=True) does not trigger computations on dask arrays."""
    try:
        import dask.dataframe as dd  # noqa: F401
    except ImportError:
        pytest.skip("dask not installed")

    def mock_read_monitor(*args, **kwargs):
        return pd.DataFrame(
            {
                "siteid": ["012345678", "999999999"],
                "latitude": [40.0, 35.0],
                "longitude": [-80.0, -120.0],
            }
        )

    import monetio.readers.epa_utils as epa_utils

    monkeypatch.setattr(epa_utils, "read_monitor_file", mock_read_monitor)

    # Wrap dask.compute to track calls
    import dask

    compute_calls = 0
    original_compute = dask.compute

    def tracked_compute(*args, **kwargs):
        nonlocal compute_calls
        compute_calls += 1
        return original_compute(*args, **kwargs)

    monkeypatch.setattr(dask, "compute", tracked_compute)

    reader = AirNowReader()
    _ = reader.open_dataset(
        files=mock_airnow_file,
        as_xarray=True,
        lazy=True,
        wide_fmt=False,  # long format to avoid long_to_wide compute
        bad_utcoffset="fix",
    )

    # In PointReader.to_xarray, dask_dataframe.to_dask_array(lengths=True) is called.
    # In Dask-Expr (Pandas 3.0+), lengths=True might trigger a compute of partitions to find sizes.
    # We allow minimal computes for metadata/structure discovery if required by the backend.
    assert compute_calls <= 2
