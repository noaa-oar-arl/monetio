import json
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from monetio.readers.grib2 import Grib2Reader


def test_grib2_reader_eager_use_dask_parity(tmp_path):
    path = tmp_path / "grib2-shaped.nc"
    source = xr.Dataset(
        {"tmp": (("lat_0", "lon_0"), np.arange(12, dtype="f4").reshape(3, 4))},
        coords={"lat_0": [-30.0, 0.0, 30.0], "lon_0": [0.0, 90.0, 180.0, 270.0]},
    )
    source.to_netcdf(path, engine="h5netcdf")

    reader = Grib2Reader()
    eager = reader.open_dataset(str(path), engine="h5netcdf")
    lazy = reader.open_dataset(
        str(path), engine="h5netcdf", use_dask=True, chunks={"lat_0": 1, "lon_0": 2}
    )

    assert eager.tmp.chunks is None
    assert lazy.tmp.chunks is not None
    assert lazy.tmp.chunks == ((1, 1, 1), (2, 2))
    eager.attrs.pop("history", None)
    lazy.attrs.pop("history", None)
    xr.testing.assert_identical(eager, lazy.compute())
    eager.close()
    lazy.close()


@pytest.mark.parametrize("reader_name", ["grib2", "ncep_grib"])
def test_real_grib2_backend_honors_chunks_and_matches_eager(reader_name):
    pytest.importorskip("grib2io")

    from monetio.readers.ncep_grib import NCEPGribReader

    sample_root = Path(__file__).parent / "data" / "samples"
    if reader_name == "grib2":
        path = sample_root / "grib2" / "grib2_sample.grib2"
        reader = Grib2Reader()
    else:
        path = sample_root / "ncep_grib" / "ncep_grib_sample.grib2"
        reader = NCEPGribReader()

    eager = reader.open_dataset(str(path), use_dask=False)
    lazy = reader.open_dataset(str(path), use_dask=True, chunks={"y": 30, "x": 60})
    variable = next(iter(lazy.data_vars))
    assert lazy[variable].chunks == ((1,), (30, 30, 30, 30, 30, 30, 1), (60, 60, 60, 60, 60, 60))
    eager.attrs.pop("history", None)
    lazy.attrs.pop("history", None)
    xr.testing.assert_allclose(eager, lazy.compute())
    eager.close()
    lazy.close()


@pytest.mark.parametrize("reader_name", ["grib2", "ncep_grib"])
def test_real_grib2_backend_filters_messages_before_payload_reads(
    tmp_path, monkeypatch, reader_name
):
    grib2io = pytest.importorskip("grib2io")
    import grib2io.xarray_backend as grib_backend

    from monetio.readers.ncep_grib import NCEPGribReader

    sample = Path(__file__).parent / "data" / "samples" / "ncep_grib" / "ncep_grib_sample.grib2"
    with grib2io.open(str(sample), _xarray_backend=True) as stream:
        source = stream[0]
        values = source.data.copy()
        second_section4 = source.section4.copy()
        second_section4[2] = 0  # discipline category: temperature
        second_section4[3] = 0  # temperature parameter
        second = grib2io.Grib2Message(
            section0=source.section0.copy(),
            section1=source.section1.copy(),
            section2=source.section2,
            section3=source.section3.copy(),
            section4=second_section4,
            section5=source.section5.copy(),
        )
        second.data = (values + 10.0).astype("f4")
        second.pack()

    path = tmp_path / "two-variable.grib2"
    path.write_bytes(sample.read_bytes() + second._msg)
    if reader_name == "grib2":
        reader = Grib2Reader()
    else:
        reader = NCEPGribReader()
    filters = {"shortName": "PRMSL"}
    eager = reader.open_dataset(str(path), filters=filters, use_dask=False)
    assert len(eager.data_vars) == 1

    data_reads = []
    original_data = grib_backend._data

    def counted_data(filehandle, message, *args, **kwargs):
        data_reads.append(message.shortName)
        return original_data(filehandle, message, *args, **kwargs)

    monkeypatch.setattr(grib_backend, "_data", counted_data)
    lazy = reader.open_dataset(str(path), filters=filters, use_dask=True, chunks={})
    assert len(lazy.data_vars) == 1
    assert next(iter(lazy.data_vars)) in eager.data_vars
    computed = lazy.compute()
    assert data_reads
    assert set(data_reads) == {"PRMSL"}
    eager.attrs.pop("history", None)
    computed.attrs.pop("history", None)
    xr.testing.assert_allclose(eager, computed)
    eager.close()


@pytest.mark.parametrize("reference_backend", ["kerchunk", "icechunk"])
def test_real_grib2_virtualizarr_dask_backend_parity(tmp_path, reference_backend):
    pytest.importorskip("grib2io")
    pytest.importorskip("virtualizarr")
    if reference_backend == "icechunk":
        pytest.importorskip("icechunk")

    path = Path(__file__).parent / "data" / "samples" / "grib2" / "grib2_sample.grib2"
    reader = Grib2Reader()
    eager = reader.open_dataset(str(path), use_dask=False)
    options = {
        "use_virtualizarr": True,
        "virtualizarr_parser": "grib2",
        "use_dask": True,
        "chunks": {"y": 30, "x": 60},
    }
    if reference_backend == "kerchunk":
        ref_path = tmp_path / "grib2-refs.json"
        options["virtualizarr_file"] = str(ref_path)
    else:
        options.update(use_icechunk=True, icechunk_url=str(tmp_path / "grib2-icechunk"))

    lazy = None
    try:
        try:
            lazy = reader.open_dataset(str(path), **options)
        except NotImplementedError as exc:
            if "Reading inlined reference data is currently not supported" in str(exc):
                pytest.xfail("VirtualiZarr cannot yet read inlined GRIB2 reference chunks")
            raise
        assert hasattr(lazy.PRMSL.data, "dask")
        assert lazy.PRMSL.chunksizes["y"] == (30, 30, 30, 30, 30, 30, 1)
        # GRIB references preserve fixed-surface metadata as a singleton
        # dimension; compare the decoded scientific field after removing it.
        try:
            computed = lazy.PRMSL.squeeze("mean_sea_level", drop=True).compute()
        except NotImplementedError as exc:
            if "Reading inlined reference data is currently not supported" in str(exc):
                pytest.xfail("VirtualiZarr cannot yet read inlined GRIB2 reference chunks")
            raise
        np.testing.assert_allclose(eager.PRMSL.values, computed.values)
        if reference_backend == "kerchunk":
            assert ref_path.is_file()
            refs = json.loads(ref_path.read_text())["refs"]
            assert '"id": "grib2io"' in refs["PRMSL/.zarray"]
    finally:
        eager.close()
        if lazy is not None:
            lazy.close()


def test_grib2_reader_open():
    ds = xr.Dataset(
        {"tmp": (("lat_0", "lon_0"), np.random.rand(10, 20))},
        coords={"lat_0": np.arange(10), "lon_0": np.arange(20)},
    )

    reader = Grib2Reader()

    import unittest.mock as mock

    with mock.patch("monetio.readers.drivers.XarrayDriver.open", return_value=ds):
        res = reader.open_dataset("dummy.grib2")

    assert "latitude" in res.coords
    assert "longitude" in res.coords
    assert "history" in res.attrs
    assert "grib2io" in res.attrs["history"]


def test_grib2_reader_harmonize():
    ds = xr.Dataset(
        {"tmp": (("lat", "lon"), np.random.rand(10, 20))},
        coords={
            "lat": np.arange(10),
            "lon": np.arange(20),
            "valid_time": [np.datetime64("2023-01-01")],
        },
    )

    reader = Grib2Reader()
    res = reader.harmonize(ds)

    assert "latitude" in res.coords
    assert "longitude" in res.coords
    # valid_time should be renamed to time
    assert "time" in res.variables


def test_ncep_grib_eager_lazy(tmp_path):
    """Test NCEPGribReader with both eager and lazy backends."""
    from monetio.readers.ncep_grib import NCEPGribReader

    nlat = 10
    nlon = 20

    ds_mock = xr.Dataset(
        {
            "TMP_P0_L1_GLL0": (("lat_0", "lon_0"), np.random.rand(nlat, nlon).astype(np.float32)),
        }
    )
    ds_mock.coords["lat_0"] = np.linspace(90, -90, nlat).astype(np.float32)
    ds_mock.coords["lon_0"] = np.linspace(0, 359, nlon).astype(np.float32)

    # Add some attributes with whitespace to test hygiene
    ds_mock.attrs["TITLE"] = "  Mock NCEP GRIB Data  "
    ds_mock.TMP_P0_L1_GLL0.attrs["units"] = " K "

    fname = tmp_path / "test_ncep_grib.nc"
    ds_mock.to_netcdf(fname, engine="h5netcdf")

    reader = NCEPGribReader()

    # 1. Eager Mode
    ds_eager = reader.open_dataset(files=str(fname), lazy=False, engine="h5netcdf")

    assert isinstance(ds_eager, xr.Dataset)
    assert "latitude" in ds_eager.coords
    assert "longitude" in ds_eager.coords
    assert ds_eager.latitude.ndim == 2
    assert ds_eager.longitude.ndim == 2
    assert ds_eager.attrs["TITLE"] == "Mock NCEP GRIB Data"
    assert ds_eager.TMP_P0_L1_GLL0.attrs["units"] == "K"

    # 2. Lazy Mode
    ds_lazy = reader.open_dataset(
        files=str(fname), chunks={"lat_0": 5, "lon_0": 10}, engine="h5netcdf"
    )

    assert isinstance(ds_lazy, xr.Dataset)
    # Check if data is dask-backed
    assert hasattr(ds_lazy.TMP_P0_L1_GLL0.data, "dask")
    assert hasattr(ds_lazy.latitude.data, "dask")
    assert hasattr(ds_lazy.longitude.data, "dask")
    assert ds_lazy.TMP_P0_L1_GLL0.chunks == ((5, 5), (10, 10))

    # 3. Consistency Check
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )

    # Verify provenance
    assert "history" in ds_eager.attrs
    assert "Generated 2D latitude/longitude coordinates lazily" in ds_eager.attrs["history"]


def test_ncep_grib_variable_promotion(tmp_path):
    """Test that lat_0/lon_0 are promoted to coords if they are only variables."""
    from monetio.readers.ncep_grib import NCEPGribReader

    nlat = 5
    nlon = 10
    ds = xr.Dataset(
        {
            "TMP": (("lat_0", "lon_0"), np.random.rand(nlat, nlon).astype(np.float32)),
            "lat_0": (("lat_0",), np.linspace(90, -90, nlat).astype(np.float32)),
            "lon_0": (("lon_0",), np.linspace(0, 350, nlon).astype(np.float32)),
        }
    )
    # Note: we don't set them as coords
    fname = tmp_path / "test_promotion.nc"
    ds.to_netcdf(fname, engine="h5netcdf")

    reader = NCEPGribReader()
    ds_out = reader.open_dataset(files=str(fname), engine="h5netcdf")

    assert "latitude" in ds_out.coords
    assert "longitude" in ds_out.coords
    assert ds_out.latitude.ndim == 2
