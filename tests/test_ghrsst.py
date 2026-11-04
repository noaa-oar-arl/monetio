"""Contracts for the GHRSST Level 4 reader, including eager/Dask parity."""

from __future__ import annotations

import json
import re
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import numpy as np
import pandas as pd
import pytest
import xarray as xr

PRODUCTS = {
    "ghrsst_ncei_avhrr_anl": "NCEI AVHRR_OI",
    "ghrsst_ospo_geopolar_anl": "OSPO Geo_Polar_Blended",
}


def _product_dataset(product: str, date: str = "2024-02-28") -> xr.Dataset:
    """Build a compact GDS-like grid with packed SST and ancillary fields."""
    lat = np.array([-0.125, 0.125], dtype="f4")
    lon = np.array([10.125, 10.375, 10.625], dtype="f4")
    sst = np.array([[[280.0, 281.0, np.nan], [282.0, 283.0, 284.0]]], dtype="f4")
    time = pd.DatetimeIndex([date])
    ds = xr.Dataset(
        {
            "analysed_sst": (
                ("time", "lat", "lon"),
                sst,
                {"long_name": "analysed sea surface temperature", "units": "K"},
            ),
            "analysis_error": (("time", "lat", "lon"), np.full_like(sst, 0.25), {"units": "K"}),
            "mask": (("time", "lat", "lon"), np.ones_like(sst, dtype="i1")),
            "sea_ice_fraction": (("time", "lat", "lon"), np.zeros_like(sst)),
        },
        coords={
            "time": time,
            "lat": ("lat", lat, {"units": "degrees_north"}),
            "lon": ("lon", lon, {"units": "degrees_east"}),
        },
        attrs={
            "title": f"GHRSST Level 4 {PRODUCTS[product]}",
            "product_id": product,
            "source": "synthetic contract fixture",
        },
    )
    ds["analysed_sst"].encoding.update(
        {"dtype": "int16", "scale_factor": 0.01, "add_offset": 273.15, "_FillValue": -32768}
    )
    return ds


@pytest.fixture(params=list(PRODUCTS), ids=["ncei-avhrr-oi", "ospo-geo-polar"])
def product(request):
    return request.param


def _write_fixture(tmp_path, product, name="renamed-no-extension.nc", date="2024-02-28"):
    path = tmp_path / name
    _product_dataset(product, date).to_netcdf(path, engine="h5netcdf")
    return path


def test_reader_is_registered_and_loader_discoverable(tmp_path):
    import monetio
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, "ghrsst_ncei_avhrr_anl")
    assert monetio.source_info("ghrsst").module == ".readers.ghrsst"
    result = monetio.load(
        "ghrsst", files=str(path), product="ghrsst_ncei_avhrr_anl", engine="h5netcdf"
    )
    assert isinstance(result, xr.Dataset)
    assert isinstance(GHRSSTReader(), GHRSSTReader)


def test_capabilities_advertise_verified_dask_contract():
    import monetio

    caps = monetio.source_info("ghrsst").capabilities
    assert caps.deferred
    assert caps.distributed
    assert caps.remote
    assert "chunks" in caps.chunk_controls


def test_local_product_fields_and_decoding(tmp_path, product):
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, product)
    result = GHRSSTReader().open_dataset(str(path), product=product, engine="h5netcdf")
    assert {"analysed_sst", "analysis_error", "mask", "sea_ice_fraction"} <= set(result.data_vars)
    assert result.analysed_sst.attrs["units"] == "K"
    assert result.analysed_sst.dims == ("time", "y", "x")
    assert result.latitude.dims == ("y",)
    assert result.longitude.dims == ("x",)
    np.testing.assert_allclose(result.analysed_sst.isel(time=0, y=0, x=0), 280.0, atol=0.02)
    assert np.isnan(result.analysed_sst.isel(time=0, y=0, x=2))
    assert result.attrs["ghrsst_product"] == product
    assert str(path) in result.attrs["source_files"]


def test_dask_selective_variable_read(tmp_path, product):
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, product)
    result = GHRSSTReader().open_dataset(
        str(path),
        product=product,
        engine="h5netcdf",
        use_dask=True,
        chunks={"time": 1, "lat": 1, "lon": 2},
        drop_variables=["mask", "sea_ice_fraction"],
    )
    assert "mask" not in result
    assert "sea_ice_fraction" not in result
    assert hasattr(result.analysed_sst.data, "dask")
    result.close()


@pytest.mark.parametrize("defer", [False, True])
def test_dask_modes_are_lazy_chunked_and_match_eager(tmp_path, product, defer):
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, product)
    reader = GHRSSTReader()
    eager = reader.open_dataset(str(path), product=product, engine="h5netcdf")
    lazy = reader.open_dataset(
        str(path),
        product=product,
        engine="h5netcdf",
        use_dask=True,
        defer=defer,
        chunks={"time": 1, "lat": 1, "lon": 2},
    )
    assert hasattr(lazy.analysed_sst.data, "dask")
    assert lazy.analysed_sst.chunks == ((1,), (1, 1), (2, 1))
    computed = lazy.compute()
    eager.attrs["history"] = computed.attrs["history"]
    xr.testing.assert_identical(eager, computed)
    lazy.close()
    eager.close()


def test_use_dask_without_explicit_chunks_stays_lazy(tmp_path, product):
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, product)
    eager = GHRSSTReader().open_dataset(str(path), product=product, engine="h5netcdf")
    lazy = GHRSSTReader().open_dataset(str(path), product=product, engine="h5netcdf", use_dask=True)
    assert hasattr(lazy.analysed_sst.data, "dask")
    computed = lazy.compute()
    eager.attrs["history"] = computed.attrs["history"]
    xr.testing.assert_identical(eager, computed)
    lazy.close()
    eager.close()


@pytest.mark.parametrize("defer", [False, True])
def test_dask_multi_file_evaluates_on_distributed_scheduler(tmp_path, product, defer):
    from distributed import Client, LocalCluster

    from monetio.readers.ghrsst import GHRSSTReader

    paths = [
        str(_write_fixture(tmp_path, product, f"{day}.nc", date=f"2024-02-{day}"))
        for day in ("27", "28")
    ]
    result = GHRSSTReader().open_dataset(
        paths,
        product=product,
        engine="h5netcdf",
        defer=defer,
        use_dask=not defer,
        chunks={"time": 1, "lat": 1, "lon": 2},
    )
    assert hasattr(result.analysed_sst.data, "dask")
    with LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=True,
        dashboard_address=False,
    ) as cluster:
        with Client(cluster) as client:
            computed = client.compute(result).result()
    assert computed.sizes["time"] == 2
    result.close()


@pytest.mark.parametrize("product", list(PRODUCTS))
def test_emc_offsets_are_explicit_and_do_not_modify_source(tmp_path, product):
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, product)
    before = path.read_bytes()
    reader = GHRSSTReader()
    native = reader.open_dataset(str(path), product=product, engine="h5netcdf")
    emc = reader.open_dataset(
        str(path), product=product, engine="h5netcdf", time_mode="emc", use_dask=True
    )
    expected = 86400 if product == "ghrsst_ncei_avhrr_anl" else 43200
    delta = emc.time.values[0] - native.time.values[0]
    assert delta / np.timedelta64(1, "s") == expected
    assert emc.attrs["ghrsst_time_mode"] == "emc"
    assert emc.attrs["ghrsst_time_offset_seconds"] == expected
    for variable in ("analysed_sst", "analysis_error", "mask", "sea_ice_fraction"):
        np.testing.assert_array_equal(native[variable].values, emc[variable].compute().values)
    np.testing.assert_array_equal(native.latitude.values, emc.latitude.values)
    np.testing.assert_array_equal(native.longitude.values, emc.longitude.values)
    assert path.read_bytes() == before


def test_multifile_is_time_ordered_and_dask_backed(tmp_path, product):
    from monetio.readers.ghrsst import GHRSSTReader

    paths = [
        str(_write_fixture(tmp_path, product, f"{day}.nc", date=f"2024-02-{day}"))
        for day in ("28", "27")
    ]
    result = GHRSSTReader().open_dataset(
        paths,
        product=product,
        engine="h5netcdf",
        use_dask=True,
        chunks={"time": 1, "lat": 1, "lon": 2},
    )
    assert list(pd.to_datetime(result.time.values)) == list(
        pd.to_datetime(["2024-02-27", "2024-02-28"])
    )
    assert hasattr(result.analysed_sst.data, "dask")
    assert json.loads(result.attrs["source_files"]) == paths
    result.close()


def test_multifile_rejects_duplicate_times(tmp_path, product):
    from monetio.readers.ghrsst import GHRSSTReader

    paths = [str(_write_fixture(tmp_path, product, f"{i}.nc")) for i in range(2)]
    with pytest.raises(ValueError, match="duplicate observation times"):
        GHRSSTReader().open_dataset(paths, product=product, engine="h5netcdf")


def test_multifile_rejects_incompatible_grids(tmp_path, product):
    from monetio.readers.ghrsst import GHRSSTReader

    first = _product_dataset(product, "2024-02-27")
    second = _product_dataset(product, "2024-02-28").assign_coords(lat=[-0.25, 0.25])
    paths = [tmp_path / "first.nc", tmp_path / "second.nc"]
    first.to_netcdf(paths[0], engine="h5netcdf")
    second.to_netcdf(paths[1], engine="h5netcdf")
    with pytest.raises(ValueError, match="join|exact|conflicting|equal"):
        GHRSSTReader().open_dataset(
            [str(path) for path in paths], product=product, engine="h5netcdf"
        )


def test_required_metadata_and_optional_fields(tmp_path):
    from monetio.readers.ghrsst import GHRSSTReader

    product = "ghrsst_ncei_avhrr_anl"
    missing_sst = _product_dataset(product).drop_vars("analysed_sst")
    missing_path = tmp_path / "missing-sst.nc"
    missing_sst.to_netcdf(missing_path, engine="h5netcdf")
    with pytest.raises(ValueError, match="analysed_sst"):
        GHRSSTReader().open_dataset(str(missing_path), product=product, engine="h5netcdf")

    optional_missing = _product_dataset(product).drop_vars(
        ["analysis_error", "mask", "sea_ice_fraction"]
    )
    optional_path = tmp_path / "optional-missing.nc"
    optional_missing.to_netcdf(optional_path, engine="h5netcdf")
    result = GHRSSTReader().open_dataset(str(optional_path), product=product, engine="h5netcdf")
    assert "analysed_sst" in result
    assert "analysis_error" not in result


def test_metadata_conflict_and_unidentified_product_fail(tmp_path):
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, "ghrsst_ncei_avhrr_anl")
    with pytest.raises(ValueError, match="conflicts with metadata"):
        GHRSSTReader().open_dataset(
            str(path), product="ghrsst_ospo_geopolar_anl", engine="h5netcdf"
        )

    unknown = _product_dataset("ghrsst_ncei_avhrr_anl")
    unknown.attrs = {"title": "unknown gridded dataset"}
    unknown_path = tmp_path / "unknown.nc"
    unknown.to_netcdf(unknown_path, engine="h5netcdf")
    with pytest.raises(ValueError, match="pass product explicitly"):
        GHRSSTReader().open_dataset(str(unknown_path), engine="h5netcdf")


def test_date_url_builder_follows_archive_index_and_handles_leap_day(monkeypatch):
    from monetio.readers.ghrsst import GHRSSTReader

    root = "https://example.test/archive/"
    product = "ghrsst_ncei_avhrr_anl"
    target = "20240229"
    filename = f"{target}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc"
    pages = {
        root: '<a href="2024/">2024/</a>',
        root + "2024/": '<a href="060/">060/</a>',
        root + "2024/060/": f'<a href="{filename}">{filename}</a>',
    }

    class Response:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    def fake_get(url, timeout):
        return Response(pages[url])

    monkeypatch.setattr("monetio.readers.ghrsst.requests.get", fake_get)
    assert GHRSSTReader().build_urls("2024-02-29", product, base_url=root) == [
        root + "2024/060/" + filename
    ]


def test_date_url_builder_rejects_missing_and_ambiguous_files(monkeypatch):
    from monetio.readers.ghrsst import GHRSSTReader

    root = "https://example.test/archive/"

    class Response:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        "monetio.readers.ghrsst.requests.get",
        lambda *a, **k: Response('<a href="2024/">2024/</a>'),
    )
    with pytest.raises(FileNotFoundError, match="20240228"):
        GHRSSTReader().build_urls("2024-02-28", "ghrsst_ncei_avhrr_anl", root)

    target = "20240228"
    one = f"{target}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc"
    two = f"{target}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.2.nc"
    pages = {
        root: '<a href="2024/">2024/</a>',
        root + "2024/": '<a href="059/">059/</a>',
        root + "2024/059/": f'<a href="{one}">{one}</a><a href="{two}">{two}</a>',
    }

    def fake_get(url, timeout):
        return Response(pages[url])

    monkeypatch.setattr("monetio.readers.ghrsst.requests.get", fake_get)
    with pytest.raises(ValueError, match="Multiple .* archive files"):
        GHRSSTReader().build_urls("2024-02-28", "ghrsst_ncei_avhrr_anl", root)
    with pytest.raises(ValueError, match="Invalid GHRSST date"):
        GHRSSTReader().build_urls(pd.NaT, "ghrsst_ncei_avhrr_anl", root)


@pytest.fixture
def local_http_archive(tmp_path):
    handler = partial(SimpleHTTPRequestHandler, directory=str(tmp_path))
    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    except PermissionError:
        pytest.skip("sandbox does not allow binding a local HTTP test server")
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_https_style_remote_dask_file_read(tmp_path, local_http_archive, product):
    from monetio.readers.ghrsst import GHRSSTReader

    path = _write_fixture(tmp_path, product)
    url = local_http_archive + path.name
    eager = GHRSSTReader().open_dataset(url, product=product, engine="h5netcdf")
    lazy = GHRSSTReader().open_dataset(
        url,
        product=product,
        engine="h5netcdf",
        use_dask=True,
        chunks={"time": 1, "lat": 1, "lon": 2},
        defer=True,
    )
    assert hasattr(lazy.analysed_sst.data, "dask")
    # Two separate opens stamp their own provenance timestamps; normalize
    # only the times so a second boundary cannot flip this parity check.
    computed = lazy.compute()
    _ts_re = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?::\d{2})?")
    for result in (eager, computed):
        if "history" in result.attrs:
            result.attrs["history"] = _ts_re.sub("<timestamp>", result.attrs["history"])
    xr.testing.assert_identical(eager, computed)
    lazy.close()
    eager.close()


def test_date_resolved_remote_series_stays_dask_backed(tmp_path, local_http_archive):
    from monetio.readers.ghrsst import GHRSSTReader

    product = "ghrsst_ncei_avhrr_anl"
    root = tmp_path / "NCEI"
    for day in ("27", "28"):
        day_dir = root / "2024" / f"{int(day) + 31:03d}"
        day_dir.mkdir(parents=True, exist_ok=True)
        stamp = f"202402{day}120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc"
        _write_fixture(day_dir, product, stamp, date=f"2024-02-{day}")

    archive_url = local_http_archive + "NCEI/"
    reader = GHRSSTReader()
    resolved = reader.build_urls(["2024-02-28", "2024-02-27"], product, archive_url)
    assert len(resolved) == 2
    result = reader.open_dataset(
        files=None,
        dates=["2024-02-28", "2024-02-27"],
        product=product,
        base_url=archive_url,
        engine="h5netcdf",
        defer=True,
        chunks={"time": 1, "lat": 1, "lon": 2},
    )
    assert hasattr(result.analysed_sst.data, "dask")
    assert list(pd.to_datetime(result.time.values)) == list(
        pd.to_datetime(["2024-02-27", "2024-02-28"])
    )
    result.compute()
    result.close()
