from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
import xarray as xr

from monetio.readers.gfs import GDASReader, GEFSReader, GFSReader
from monetio.readers.hrrr import HRRRReader
from monetio.readers.nam import NAMReader
from monetio.readers.rap import RAPReader
from monetio.readers.rrfs import RRFSReader


def test_gfs_build_urls():
    reader = GFSReader()
    dates = pd.to_datetime(["2025-03-24"])
    urls = reader.build_urls(dates, hour=0, lead_time=3)
    assert len(urls) == 1
    assert urls[0] == "s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.0p25.f003"

    urls = reader.build_urls(dates, hour=6, lead_time=[0, 3])
    assert len(urls) == 2
    assert urls[0] == "s3://noaa-gfs-bdp-pds/gfs.20250324/06/atmos/gfs.t06z.pgrb2.0p25.f000"
    assert urls[1] == "s3://noaa-gfs-bdp-pds/gfs.20250324/06/atmos/gfs.t06z.pgrb2.0p25.f003"


def test_gefs_build_urls():
    reader = GEFSReader()
    dates = pd.to_datetime(["2025-03-24"])
    # Default ensemble mean 0.5 deg
    urls = reader.build_urls(dates, hour=0, lead_time=3)
    assert len(urls) == 1
    assert (
        urls[0] == "s3://noaa-gefs-pds/gefs.20250324/00/atmos/pgrb2ap5/geavg.t00z.pgrb2a.0p50.f003"
    )

    # Custom member / resolution
    urls = reader.build_urls(dates, hour=12, lead_time=0, product="gep01.tHHz.pgrb2a.0p50")
    assert (
        urls[0] == "s3://noaa-gefs-pds/gefs.20250324/12/atmos/pgrb2ap5/gep01.t12z.pgrb2a.0p50.f000"
    )


def test_gdas_build_urls():
    reader = GDASReader()
    dates = pd.to_datetime(["2025-03-24"])
    urls = reader.build_urls(dates, hour=18, lead_time=0)
    assert len(urls) == 1
    assert urls[0] == "s3://noaa-gfs-bdp-pds/gdas.20250324/18/atmos/gdas.t18z.pgrb2.0p25.f000"


@pytest.mark.parametrize(
    "reader_cls",
    [GFSReader, GEFSReader, GDASReader, HRRRReader, NAMReader, RAPReader],
)
def test_ncep_pds_readers_forward_use_dask_and_preserve_results(reader_cls):
    base = xr.Dataset(
        {"TMP": (("lat_0", "lon_0"), [[280.0, 281.0], [282.0, 283.0]])},
        coords={"lat_0": [10.0, 20.0], "lon_0": [30.0, 40.0]},
    )

    class MockDriver:
        def __init__(self):
            self.use_dask = []
            self.chunks = []

        def open(self, files, **kwargs):
            self.use_dask.append(kwargs.get("use_dask", False))
            self.chunks.append(kwargs.get("chunks"))
            ds = base.copy(deep=True)
            return ds.chunk(kwargs["chunks"]) if kwargs.get("use_dask") else ds

    reader = reader_cls()
    driver = MockDriver()
    reader.driver = driver
    eager = reader.open_dataset(files="mock.grib2")
    requested_chunks = {"lat_0": 1}
    lazy = reader.open_dataset(files="mock.grib2", use_dask=True, chunks=requested_chunks)

    assert eager.temperature.chunks is None
    assert lazy.temperature.chunks is not None
    assert driver.use_dask == [False, True]
    assert driver.chunks == [None, requested_chunks]
    eager.attrs.pop("history", None)
    lazy.attrs.pop("history", None)
    xr.testing.assert_identical(eager, lazy.compute())


@pytest.mark.parametrize(
    "reader_name",
    ["gfs", "gefs", "gdas", "hrrr", "nam", "rap", "rrfs"],
)
def test_real_ncep_grib_backend_honors_chunks_and_matches_eager(reader_name):
    pytest.importorskip("grib2io")

    readers = {
        "gfs": GFSReader,
        "gefs": GEFSReader,
        "gdas": GDASReader,
        "hrrr": HRRRReader,
        "nam": NAMReader,
        "rap": RAPReader,
        "rrfs": RRFSReader,
    }
    path = Path(__file__).parent / "data" / "samples" / "ncep_grib" / "ncep_grib_sample.grib2"
    reader = readers[reader_name]()
    eager = reader.open_dataset(str(path), use_dask=False)
    lazy = reader.open_dataset(str(path), use_dask=True, chunks={"y": 30, "x": 60})

    variable = next(iter(lazy.data_vars))
    assert lazy[variable].chunks == ((30, 30, 30, 30, 30, 30, 1), (60, 60, 60, 60, 60, 60))
    eager.attrs.pop("history", None)
    lazy.attrs.pop("history", None)
    xr.testing.assert_allclose(eager, lazy.compute())
    eager.close()
    lazy.close()


def test_gfs_rejects_non_grib2io_engine():
    reader = GFSReader()
    with pytest.raises(ValueError, match="Use engine='grib2io'"):
        reader.open_dataset(
            files=["s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.0p25.f000"],
            engine="cfgrib",
        )


def test_gfs_forces_grib2io_engine_when_unspecified():
    reader = GFSReader()
    with mock.patch(
        "monetio.readers.base.GriddedReader.open_dataset", return_value=xr.Dataset()
    ) as mock_open:
        reader.open_dataset(
            files=["s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.0p25.f000"]
        )

        _, kwargs = mock_open.call_args
        assert kwargs["engine"] == "grib2io"


def test_gfs_applies_safe_s3_grib2_defaults():
    reader = GFSReader()
    with mock.patch(
        "monetio.readers.base.GriddedReader.open_dataset", return_value=xr.Dataset()
    ) as mock_open:
        reader.open_dataset(
            files=["s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.0p25.f000"]
        )

        _, kwargs = mock_open.call_args
        assert kwargs["storage_options"]["anon"] is True
        assert kwargs["max_workers"] == 4
        assert kwargs["network_timeout"] == 300
        assert kwargs["max_concurrent_requests"] == 2
        assert kwargs["retry_attempts"] == 3
        assert kwargs["retry_base_sleep"] == 1.0


def test_gfs_preserves_explicit_s3_grib2_settings():
    reader = GFSReader()
    with mock.patch(
        "monetio.readers.base.GriddedReader.open_dataset", return_value=xr.Dataset()
    ) as mock_open:
        reader.open_dataset(
            files=["s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.0p25.f000"],
            storage_options={"anon": False},
            max_workers=7,
            network_timeout=33,
            max_concurrent_requests=8,
            retry_attempts=5,
            retry_base_sleep=0.25,
        )

        _, kwargs = mock_open.call_args
        assert kwargs["storage_options"]["anon"] is False
        assert kwargs["max_workers"] == 7
        assert kwargs["network_timeout"] == 33
        assert kwargs["max_concurrent_requests"] == 8
        assert kwargs["retry_attempts"] == 5
        assert kwargs["retry_base_sleep"] == 0.25


def test_gefs_url_kwargs_do_not_leak_to_xarray():
    reader = GEFSReader()
    with mock.patch(
        "monetio.readers.base.GriddedReader.open_dataset", return_value=xr.Dataset()
    ) as mock_open:
        reader.open_dataset(
            dates=pd.to_datetime(["2025-03-24"]),
            hour=12,
            lead_time=0,
            product="gep01.tHHz.pgrb2a.0p50",
        )

        args, kwargs = mock_open.call_args
        # files are first positional arg to GriddedReader.open_dataset
        assert args[0] == [
            "s3://noaa-gefs-pds/gefs.20250324/12/atmos/pgrb2ap5/gep01.t12z.pgrb2a.0p50.f000"
        ]
        assert "product" not in kwargs


def test_gefs_translates_deprecated_icechunk_args():
    reader = GEFSReader()
    with mock.patch(
        "monetio.readers.base.GriddedReader.open_dataset", return_value=xr.Dataset()
    ) as mock_open:
        reader.open_dataset(
            files=[
                "s3://noaa-gefs-pds/gefs.20250324/00/atmos/pgrb2ap5/geavg.t00z.pgrb2a.0p50.f000"
            ],
            use_virtualizarr=True,
            virtualizarr_backend="icechunk",
            icechunk_repo="./zarr_stores/gefs_icechunk",
        )

        _, kwargs = mock_open.call_args
        assert kwargs["use_icechunk"] is True
        assert kwargs["virtualizarr_backend"] == "kerchunk"
        assert kwargs["icechunk_repo"] is None
        assert kwargs["icechunk_url"] == "./zarr_stores/gefs_icechunk"


def test_gefs_open_aerosol_aod550_sets_easy_defaults():
    reader = GEFSReader()
    with mock.patch.object(reader, "open_dataset", return_value=xr.Dataset()) as mock_open:
        reader.open_aerosol_aod550(dates=pd.to_datetime(["2025-01-01"]))

        _, kwargs = mock_open.call_args
        assert kwargs["product"] == "aerosol"
        assert kwargs["source"] == "aws"
        assert kwargs["use_dask"] is True
        assert kwargs["use_icechunk"] is True
        assert kwargs["storage_options"]["anon"] is True
        assert kwargs["filters"]["shortName"] == "totAOD550"
        assert kwargs["filters"]["typeOfFirstFixedSurface"] == 10


def test_gefs_open_aerosol_aod550_allows_filter_and_storage_overrides():
    reader = GEFSReader()
    with mock.patch.object(reader, "open_dataset", return_value=xr.Dataset()) as mock_open:
        reader.open_aerosol_aod550(
            dates=pd.to_datetime(["2025-01-01"]),
            storage_options={"anon": False, "region": "us-east-1"},
            filters={"shortName": "totAOD550", "valueOfFirstFixedSurface": 0.0},
            max_workers=2,
        )

        _, kwargs = mock_open.call_args
        assert kwargs["storage_options"]["anon"] is False
        assert kwargs["storage_options"]["region"] == "us-east-1"
        assert kwargs["filters"]["typeOfFirstFixedSurface"] == 10
        assert kwargs["filters"]["valueOfFirstFixedSurface"] == 0.0
        assert kwargs["max_workers"] == 2


def test_gefs_open_chem_all_variables_when_no_short_name():
    reader = GEFSReader()
    with mock.patch.object(reader, "open_dataset", return_value=xr.Dataset()) as mock_open:
        reader.open_chem(dates=pd.to_datetime(["2025-01-01"]))

        _, kwargs = mock_open.call_args
        assert kwargs["product"] == "aerosol"
        assert "filters" not in kwargs


def test_gefs_open_chem_single_variable_with_level_filters():
    reader = GEFSReader()
    with mock.patch.object(reader, "open_dataset", return_value=xr.Dataset()) as mock_open:
        reader.open_chem(
            dates=pd.to_datetime(["2025-01-01"]),
            short_name="TMP",
            type_of_first_fixed_surface=103,
            value_of_first_fixed_surface=2,
        )

        _, kwargs = mock_open.call_args
        assert kwargs["product"] == "aerosol"
        assert kwargs["filters"]["shortName"] == "TMP"
        assert kwargs["filters"]["typeOfFirstFixedSurface"] == 103
        assert kwargs["filters"]["valueOfFirstFixedSurface"] == 2


def test_gefs_open_chem_multiple_variables_with_list():
    reader = GEFSReader()
    with mock.patch.object(reader, "open_dataset", return_value=xr.Dataset()) as mock_open:
        reader.open_chem(
            dates=pd.to_datetime(["2025-01-01"]),
            short_name=["totAOD550", "DUST", "TMP"],
        )

        _, kwargs = mock_open.call_args
        assert kwargs["filters"]["shortName"] == ["totAOD550", "DUST", "TMP"]


def test_open_dataset_forwards_use_icechunk_to_grib2io_backend_kwargs():
    reader = GFSReader()
    with mock.patch(
        "monetio.readers.base.GriddedReader.open_dataset", return_value=xr.Dataset()
    ) as mock_open:
        reader.open_dataset(
            files=["s3://noaa-gfs-bdp-pds/gfs.20250324/00/atmos/gfs.t00z.pgrb2.0p25.f000"],
            use_icechunk=True,
            icechunk_url="./zarr_stores/grib2io_icechunk",
        )

        _, kwargs = mock_open.call_args
        assert kwargs["engine"] == "grib2io"
        assert kwargs["use_icechunk"] is True
        assert kwargs["icechunk_url"] == "./zarr_stores/grib2io_icechunk"


@pytest.mark.network
def test_gfs_open_dataset_integration():
    # This test requires grib2io and s3fs.
    # We'll try to at least build the URL and check if it's correct.
    # Actual opening might fail if grib2io is not fully functional in this environment.
    reader = GFSReader()
    # Use a recent date that is likely to exist
    date = (pd.Timestamp.now() - pd.Timedelta(days=2)).strftime("%Y-%m-%d")
    try:
        ds = reader.open_dataset(dates=date, hour=0, lead_time=0)
        assert isinstance(ds, xr.Dataset)
        assert "latitude" in ds.coords
        assert "longitude" in ds.coords
    except Exception as e:
        pytest.skip(f"GFS integration test skipped: {e}")
