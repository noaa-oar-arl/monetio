"""Integration tests with real NOAA PREPBUFR reports and NCEPLIBS-bufr."""

import numpy as np
import pytest
import xarray as xr

import monetio

pytest.importorskip("ncepbufr")


@pytest.fixture
def native_reports(data_dir):
    return str(data_dir / "samples/prepbufr/reports.bufr")


@pytest.mark.parametrize(
    "subset,levels,siteid,pressure,temperature",
    [
        ("ADPUPA", 57, "68842", 100900.0, 23.1),
        ("ADPSFC", 1, "03354", 98480.0, np.nan),
        ("SFCSHP", 1, "OSCFR05", 100940.0, np.nan),
        ("AIRCFT", 1, "THY4", 23620.0, -54.0),
        ("AIRCAR", 1, "ZJBUOGJA", 56330.0, -15.9),
    ],
)
def test_native_report_values(native_reports, subset, levels, siteid, pressure, temperature):
    """Assert decoded station strings, levels, units, masks, and known values."""
    ds = monetio.load("prepbufr", files=native_reports, subsets=subset)
    assert ds.sizes == {"node": levels}
    assert ds.siteid[0].item() == siteid
    np.testing.assert_allclose(ds.pressure[0], pressure)
    np.testing.assert_allclose(ds.temperature[0], temperature)
    assert ds.pressure.attrs["units"] == "Pa"


@pytest.mark.parametrize("scheduler", ["threads", "processes"])
def test_native_dask_equivalence(native_reports, tmp_path, scheduler):
    """Exercise multi-file native decoding in thread and process workers."""
    from pathlib import Path

    second = tmp_path / "second.bufr"
    second.write_bytes(Path(native_reports).read_bytes())
    files = [native_reports, str(second)]
    eager = monetio.load("prepbufr", files=files)
    lazy = monetio.load("prepbufr", files=files, use_dask=True)
    assert hasattr(lazy.temperature.data, "dask")
    assert hasattr(lazy.wind_speed.data, "dask")
    assert hasattr(lazy.rel_humidity.data, "dask")
    assert eager.sizes["node"] == 122
    computed = lazy.compute(scheduler=scheduler)
    # Timestamped provenance is the sole volatile field.
    eager.attrs.pop("history")
    computed.attrs.pop("history")
    xr.testing.assert_identical(eager, computed)
