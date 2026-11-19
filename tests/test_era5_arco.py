"""ARCO ERA5 selection and CFS archive reader tests."""

import sys
from types import ModuleType

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.cfs import CFSRReader, CFSv2Reader
from monetio.readers.era5 import ERA5Reader
from monetio.readers.ncep_reanalysis import NCEPReanalysisReader


def _arco_dataset():
    return xr.Dataset(
        {
            "2m_temperature": (("time", "latitude", "longitude"), np.ones((2, 3, 4))),
            "temperature": (("time", "level", "latitude", "longitude"), np.ones((2, 2, 3, 4))),
        },
        coords={
            "time": pd.date_range("2020-01-01", periods=2, freq="h"),
            "level": [500, 850],
            "latitude": [90, 0, -90],
            "longitude": [0, 90, 180, 270],
        },
    )


def test_arco_reader_selects_variables_time_levels_and_bbox(monkeypatch):
    source = _arco_dataset()
    seen = {}

    def open_zarr(store, **kwargs):
        seen.update(store=store, **kwargs)
        return source.copy()

    monkeypatch.setattr(xr, "open_zarr", open_zarr)
    result = ERA5Reader().open_dataset(
        source="arco",
        dates=slice("2020-01-01", "2020-01-01 01:00"),
        variables=["temperature_2m", "temperature"],
        levels=[850],
        bbox=(0, -10, 180, 90),
        use_dask=True,
    )

    assert seen["chunks"] == {}
    assert seen["storage_options"] == {"token": "anon"}
    assert "temperature_2m" in result
    assert "temperature" in result
    assert result.sizes["time"] == 2
    assert result.sizes["z"] == 1
    assert result.sizes["x"] == 3
    assert result.sizes["y"] == 2
    assert result.temperature_2m.dims == ("time", "y", "x")


def test_arco_requires_dates_and_known_variables(monkeypatch):
    monkeypatch.setattr(xr, "open_zarr", lambda *args, **kwargs: _arco_dataset())
    with pytest.raises(ValueError, match="requires both dates and variables"):
        ERA5Reader().open_dataset(source="arco", variables="temperature_2m")
    with pytest.raises(ValueError, match="Unsupported ARCO"):
        ERA5Reader().open_dataset(source="arco", dates="2020-01-01", variables="not-a-variable")


def test_aws_icechunk_reader_opens_selected_groups(monkeypatch):
    calls = []
    single = xr.Dataset(
        {"t2m": (("valid_time", "latitude", "longitude"), np.ones((2, 3, 4)))},
        coords={
            "valid_time": pd.date_range("2020-01-01", periods=2, freq="h"),
            "latitude": [90, 0, -90],
            "longitude": [0, 90, 180, 270],
        },
    )
    pressure = xr.Dataset(
        {"t": (("valid_time", "pressure_level", "latitude", "longitude"), np.ones((2, 2, 3, 4)))},
        coords={
            "valid_time": pd.date_range("2020-01-01", periods=2, freq="h"),
            "pressure_level": [500, 850],
            "latitude": [90, 0, -90],
            "longitude": [0, 90, 180, 270],
        },
    )

    class Repository:
        @staticmethod
        def open(storage):
            calls.append(("open", storage))
            return Repository()

        @staticmethod
        def readonly_session(branch):
            calls.append(("session", branch))
            return type("Session", (), {"store": object()})()

    icechunk = ModuleType("icechunk")
    icechunk.s3_storage = lambda **kwargs: kwargs
    icechunk.Repository = Repository
    monkeypatch.setitem(sys.modules, "icechunk", icechunk)
    monkeypatch.setitem(sys.modules, "pcodec", ModuleType("pcodec"))

    def open_zarr(store, **kwargs):
        calls.append(("zarr", kwargs))
        return single.copy() if kwargs["group"] == "single/spatial" else pressure.copy()

    monkeypatch.setattr(xr, "open_dataset", open_zarr)
    result = ERA5Reader().open_dataset(
        source="aws",
        dates=slice("2020-01-01", "2020-01-01 01:00"),
        variables=["temperature_2m", "temperature"],
        levels=[850],
        bbox=(0, -10, 180, 90),
        use_dask=True,
    )

    groups = [call[1]["group"] for call in calls if call[0] == "zarr"]
    assert groups == ["single/spatial", "pressure/spatial"]
    assert all(call[1]["chunks"] == {} for call in calls if call[0] == "zarr")
    assert "temperature_2m" in result
    assert "temperature" in result
    assert result.sizes["z"] == 1
    assert result.sizes["x"] == 3
    assert result.sizes["y"] == 2


@pytest.mark.parametrize(
    "reader,expected",
    [
        (
            CFSRReader(),
            "https://www.ncei.noaa.gov/oa/prod-cfs-reanalysis/6-hourly-by-pressure-level/"
            "2020/202001/20200102/pgbh06.gdas.2020010206.grb2",
        ),
        (
            CFSv2Reader(),
            "https://www.ncei.noaa.gov/oa/prod-cfs-operational-analysis/6-hourly-by-pressure/"
            "2020/202001/20200102/cdas1.t06z.pgrbh00.grib2",
        ),
    ],
)
def test_cfs_archive_url_builders(reader, expected):
    assert reader.build_urls("2020-01-02", hour=6) == [expected]


def test_cfs_archive_url_builder_validates_cycle():
    with pytest.raises(ValueError, match="hour"):
        CFSRReader().build_urls("2020-01-02", hour=3)


def test_ncep_reanalysis_routes_cfs_product(monkeypatch):
    expected = xr.Dataset({"value": ("x", [1])})
    captured = {}

    def open_dataset(self, **kwargs):
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(CFSv2Reader, "open_dataset", open_dataset)
    result = NCEPReanalysisReader().open_dataset(product="cfsv2", dates="2020-01-02", hour=6)

    assert result is expected
    assert captured["dates"] == "2020-01-02"
    assert captured["hour"] == 6
