"""Shared deferred loading contract regression tests."""

from __future__ import annotations

import dask.array as da
import dask.dataframe as dd
import netCDF4
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.aeronet import AERONETReader
from monetio.readers.airnow import AirNowReader
from monetio.readers.drivers import PandasDriver, XarrayDriver
from monetio.readers.errors import CapabilityUnavailableError, OptionUnsupportedError


def _nc(path):
    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("time", 4)
        v = ds.createVariable("v", "f4", ("time",))
        v[:] = np.arange(4)


def _point_netcdf(path):
    xr.Dataset(
        {"temperature": (("recNum",), np.array([270.0, 271.0], dtype="f4"))},
        coords={"recNum": [0, 1]},
    ).to_netcdf(path, engine="h5netcdf")


def test_defer_survives_lazy_false_and_keeps_eager_default(tmp_path):
    path = tmp_path / "small.nc"
    _nc(path)
    driver = XarrayDriver(source="cmaq")
    deferred = driver.open(str(path), defer=True, lazy=False)
    assert isinstance(deferred["v"].data, da.Array)
    eager = driver.open(str(path), defer=False, lazy=False)
    assert not isinstance(eager["v"].data, da.Array)


def test_defer_survives_use_dask_false_alias(tmp_path):
    path = tmp_path / "small.nc"
    _nc(path)
    deferred = XarrayDriver(source="cmaq").open(str(path), defer=True, use_dask=False)
    assert isinstance(deferred["v"].data, da.Array)


@pytest.mark.parametrize("multiple", [False, True])
def test_xarray_driver_rejects_unknown_chunk_dimensions(tmp_path, multiple):
    paths = []
    for index in range(2 if multiple else 1):
        path = tmp_path / f"chunk-dim-{index}.nc"
        _nc(path)
        paths.append(str(path))

    files = paths if multiple else paths[0]
    options = {"combine": "nested", "concat_dim": "time"} if multiple else {}
    with pytest.raises(ValueError, match="not_a_dimension"):
        XarrayDriver(source="cmaq").open(
            files, use_dask=True, chunks={"not_a_dimension": 1}, **options
        )


def test_actris_use_dask_does_not_eagerly_load_netcdf_path(tmp_path):
    from monetio.readers.actris import ACTRISReader

    path = tmp_path / "actris.nc"
    _nc(path)
    ds = ACTRISReader().open_dataset(str(path), use_dask=True)
    try:
        assert isinstance(ds["v"].data, da.Array)
    finally:
        ds.close()


def test_geoms_hdf5_obeys_use_dask_and_eager_defaults(tmp_path):
    import h5py

    from monetio.readers.geoms import GEOMSReader

    path = tmp_path / "geoms.h5"
    expected = np.arange(12, dtype="f4").reshape(3, 4)
    with h5py.File(path, "w") as handle:
        dataset = handle.create_dataset("value", data=expected)
        dataset.dims[0].label = "time"
        dataset.dims[1].label = "altitude"

    reader = GEOMSReader()
    eager = reader.open_dataset(str(path), rename_all=False, squeeze=False)
    lazy = reader.open_dataset(
        str(path),
        rename_all=False,
        squeeze=False,
        use_dask=True,
        chunks={"time": 1, "altitude": 2},
    )
    lazy_alias = reader.open_dataset(str(path), rename_all=False, squeeze=False, lazy=True)
    assert isinstance(eager["value"].data, np.ndarray)
    assert isinstance(lazy["value"].data, da.Array)
    assert lazy["value"].chunks == ((1, 1, 1), (2, 2))
    assert isinstance(lazy_alias["value"].data, da.Array)
    np.testing.assert_array_equal(lazy["value"].compute().values, eager["value"].values)

    hdf4 = tmp_path / "legacy.hdf"
    hdf4.touch()
    with pytest.raises(OptionUnsupportedError, match="use_dask"):
        reader.open_dataset(str(hdf4), use_dask=True)
    with pytest.raises(OptionUnsupportedError, match="chunks"):
        reader.open_dataset(str(path), use_dask=True, chunks=None)


def test_actris_netcdf_table_honors_lazy_alias(tmp_path):
    from monetio.readers.actris import ACTRISReader

    path = tmp_path / "actris-table.nc"
    _nc(path)
    reader = ACTRISReader()
    lazy_frame = reader.open_dataset(str(path), as_xarray=False, lazy=True)
    assert isinstance(lazy_frame, dd.DataFrame)
    assert lazy_frame.compute()["v"].tolist() == [0, 1, 2, 3]
    dask_alias_frame = reader.open_dataset(str(path), as_xarray=False, use_dask=True)
    assert isinstance(dask_alias_frame, dd.DataFrame)
    assert dask_alias_frame.compute()["v"].tolist() == [0, 1, 2, 3]

    eager_frame = reader.open_dataset(str(path), as_xarray=False, lazy=False)
    assert isinstance(eager_frame, pd.DataFrame)
    assert eager_frame["v"].tolist() == [0, 1, 2, 3]


def test_aeronet_use_dask_alias_returns_lazy_table():
    from pathlib import Path

    from monetio.readers.aeronet import AERONETReader

    path = Path(__file__).parent / "data" / "aeronet-AOD15-example.txt"
    reader = AERONETReader()
    frame = reader.open_dataset(str(path), as_xarray=False, use_dask=True)
    eager = reader.open_dataset(str(path), as_xarray=False, use_dask=False)

    assert isinstance(frame, dd.DataFrame)
    pd.testing.assert_frame_equal(
        frame.compute().reindex(sorted(eager.columns), axis=1),
        eager.reindex(sorted(eager.columns), axis=1),
        check_dtype=False,
    )


@pytest.mark.parametrize("reader_name", ["iagos", "tccon"])
def test_direct_netcdf_readers_preserve_dask_for_xarray_and_table(tmp_path, reader_name):
    from monetio.readers.iagos import IAGOSReader
    from monetio.readers.tccon import TCCONReader

    reader = {"iagos": IAGOSReader, "tccon": TCCONReader}[reader_name]()
    path = tmp_path / f"{reader_name}.nc"
    _nc(path)

    ds = reader.open_dataset(str(path), as_xarray=True, expand2d=False, use_dask=True)
    try:
        assert isinstance(ds["v"].data, da.Array)
    finally:
        ds.close()

    frame = reader.open_dataset(str(path), as_xarray=False, use_dask=True)
    assert isinstance(frame, dd.DataFrame)
    assert frame.compute()["v"].tolist() == [0, 1, 2, 3]

    eager_ds = reader.open_dataset(str(path), as_xarray=True, expand2d=False)
    try:
        assert not isinstance(eager_ds["v"].data, da.Array)
        np.testing.assert_array_equal(eager_ds["v"].values, [0, 1, 2, 3])
    finally:
        eager_ds.close()

    eager_frame = reader.open_dataset(str(path), as_xarray=False, use_dask=False)
    assert isinstance(eager_frame, pd.DataFrame)
    assert eager_frame["v"].tolist() == [0, 1, 2, 3]


@pytest.mark.parametrize("reader_name", ["amdar", "madis"])
def test_direct_point_netcdf_readers_preserve_use_dask(tmp_path, reader_name):
    from monetio.readers.amdar import AMDARReader
    from monetio.readers.madis import MADISReader

    reader_cls = {"amdar": AMDARReader, "madis": MADISReader}[reader_name]
    path = tmp_path / f"{reader_name}.nc"
    _point_netcdf(path)

    lazy = reader_cls().open_dataset(str(path), use_dask=True, expand2d=False)
    eager = reader_cls().open_dataset(str(path), use_dask=False, expand2d=False)
    try:
        assert isinstance(lazy["temperature"].data, da.Array)
        xr.testing.assert_equal(lazy.compute(), eager.compute())
        assert "history" in lazy.attrs and "history" in eager.attrs
    finally:
        lazy.close()
        eager.close()

    lazy_table = reader_cls().open_dataset(str(path), use_dask=True, as_xarray=False)
    eager_table = reader_cls().open_dataset(str(path), use_dask=False, as_xarray=False)
    assert isinstance(lazy_table, dd.DataFrame)
    assert lazy_table.compute().temperature.tolist() == eager_table.temperature.tolist()


@pytest.mark.parametrize("reader_name", ["pandora", "ndacc"])
def test_geoms_table_readers_offer_serializable_dask_dataframe(reader_name):
    import pickle
    from pathlib import Path

    from monetio.readers.ndacc import NDACCReader
    from monetio.readers.pandora import PandoraReader

    path = Path(__file__).parent / "data" / "pandora-uvvis-no2-boulder-20231206.h5"
    reader = {"pandora": PandoraReader, "ndacc": NDACCReader}[reader_name]()
    ds_eager = reader.open_dataset(str(path), as_xarray=True)
    assert not any(isinstance(variable.data, da.Array) for variable in ds_eager.data_vars.values())
    ds = reader.open_dataset(str(path), as_xarray=True, use_dask=True, chunks=50)
    try:
        assert any(isinstance(variable.data, da.Array) for variable in ds.data_vars.values())
        first_chunked = next(variable for variable in ds.data_vars.values() if variable.chunks)
        assert first_chunked.chunks[0] == (50, 50, 50, 50, 50, 50, 40)
        computed = ds.compute()
        eager_compare = ds_eager.copy()
        computed_compare = computed.copy()
        eager_compare.attrs.pop("history", None)
        computed_compare.attrs.pop("history", None)
        xr.testing.assert_identical(eager_compare, computed_compare)
    finally:
        ds.close()

    frame = reader.open_dataset(str(path), as_xarray=False, use_dask=True, chunks=50)
    eager_frame = reader.open_dataset(str(path), as_xarray=False, use_dask=False)
    assert isinstance(frame, dd.DataFrame)
    assert frame.npartitions == 7
    assert isinstance(eager_frame, pd.DataFrame)
    assert len(frame.columns) > 0
    pickle.dumps(frame.__dask_graph__())
    assert not frame.head(1).empty
    pd.testing.assert_frame_equal(
        eager_frame.reset_index(drop=True),
        frame.compute().reset_index(drop=True),
        check_dtype=False,
    )


def test_defer_rejects_chunks_none_before_open(tmp_path):
    path = tmp_path / "small.nc"
    _nc(path)
    driver = XarrayDriver(source="cmaq")
    with pytest.raises(OptionUnsupportedError, match="chunks=None"):
        driver.open(str(path), defer=True, chunks=None)


@pytest.mark.parametrize("options", [{"use_dask": True}, {"lazy": True}])
def test_lazy_dask_rejects_chunks_none(tmp_path, options):
    path = tmp_path / "small.nc"
    _nc(path)
    driver = XarrayDriver(source="cmaq")
    with pytest.raises(OptionUnsupportedError, match="chunks=None"):
        driver.open(str(path), chunks=None, **options)


def test_pandas_driver_rejects_array_chunks_before_path_discovery(monkeypatch):
    driver = PandasDriver(source="generic_point_reader")

    def unexpected_discovery(*args, **kwargs):
        raise AssertionError("unsupported array chunks must be rejected before discovery")

    monkeypatch.setattr("monetio.readers.drivers.FileUtility.expand_paths", unexpected_discovery)
    with pytest.raises(OptionUnsupportedError, match="generic_point_reader.*chunks"):
        driver.open("/no-such-input", lazy=True, chunks={"time": 1})


@pytest.mark.parametrize("reader_class", [AirNowReader, AERONETReader], ids=["airnow", "aeronet"])
@pytest.mark.parametrize("defer", [False, True], ids=["use-dask", "defer"])
@pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
def test_text_point_readers_reject_array_chunks_before_io(reader_class, defer, as_xarray):
    reader = reader_class()
    with pytest.raises(OptionUnsupportedError, match="chunks"):
        reader.open_dataset(
            files="/no-such-monetio-audit-input",
            chunks={"time": 1},
            use_dask=not defer,
            defer=defer,
            as_xarray=as_xarray,
        )


@pytest.mark.parametrize("width", [10, 100])
def test_lazy_point_to_xarray_shares_length_discovery_across_columns(width):
    import dask.dataframe as dd
    from dask.callbacks import Callback

    from monetio.readers.pams import PAMSReader

    rows = 40
    pdf = pd.DataFrame({f"column_{i}": np.arange(rows) + i for i in range(width)})
    frame = dd.from_pandas(pdf, npartitions=4)

    class ComputeCounter(Callback):
        def __init__(self):
            self.count = 0

        def _start(self, dsk):
            self.count += 1

    counter = ComputeCounter()
    with counter:
        ds = PAMSReader().to_xarray(frame, expand2d=False)

    assert len(ds.data_vars) == width
    assert all(isinstance(var.data, da.Array) for var in ds.data_vars.values())
    assert counter.count == 1


def test_point_defer_requires_explicit_schema_before_read(tmp_path):
    path = tmp_path / "input.txt"
    path.write_text("data\n")
    calls = []

    def reader(fn):
        calls.append(fn)
        return pd.DataFrame({"value": [1]})

    with pytest.raises(OptionUnsupportedError, match="explicit schema meta"):
        PandasDriver(source="airnow").open(str(path), read_method=reader, lazy=True, defer=True)
    assert calls == []


def test_direct_driver_rejects_source_without_defer_capability(tmp_path):
    path = tmp_path / "input.txt"
    path.write_text("data\n")
    calls = []

    def reader(fn):
        calls.append(fn)
        return pd.DataFrame({"value": [1]})

    with pytest.raises(CapabilityUnavailableError, match="aqs.*defer"):
        PandasDriver(source="aqs").open(
            str(path),
            read_method=reader,
            defer=True,
            meta=pd.DataFrame({"value": pd.Series(dtype="int64")}),
        )
    assert calls == []


def test_registered_direct_xarray_reader_rejects_unsupported_defer(tmp_path):
    from monetio.readers.madis import MADISReader

    with pytest.raises(CapabilityUnavailableError, match="madis.*defer"):
        MADISReader().open_dataset(str(tmp_path / "missing.nc"), defer=True)


def test_all_unadvertised_registered_sources_reject_defer_before_io():
    from monetio._lazy import get_reader_class
    from monetio._manifest import SOURCE_MANIFEST

    unsupported = [spec for spec in SOURCE_MANIFEST if not spec.capabilities.deferred]
    assert unsupported
    for spec in unsupported:
        reader = get_reader_class(spec.name)()
        with pytest.raises(CapabilityUnavailableError, match=f"{spec.name}.*defer"):
            reader.open_dataset("/no-such-monetio-audit-input", defer=True)


def test_explicit_meta_keeps_delayed_reader_unexecuted_until_compute(tmp_path):
    path = tmp_path / "input.txt"
    path.write_text("data\n")
    calls = []

    def reader(fn):
        calls.append(fn)
        return pd.DataFrame({"value": [1, 2]})

    frame = PandasDriver(source="airnow").open(
        str(path),
        read_method=reader,
        lazy=True,
        defer=True,
        meta=pd.DataFrame({"value": pd.Series(dtype="int64")}),
    )
    assert isinstance(frame, dd.DataFrame)
    assert calls == []
    assert frame.compute().value.tolist() == [1, 2]
    assert len(calls) == 1


def test_lazy_table_preserves_pandas_object_strings_without_mutating_dask_config(tmp_path):
    import dask

    path = tmp_path / "strings.txt"
    path.write_text("ignored\n")

    def reader(_):
        return pd.DataFrame({"station": pd.Series(["A"], dtype=object)})

    config_before = dask.config.get("dataframe.convert-string")
    frame = PandasDriver(source="example").open(str(path), read_method=reader, lazy=True)

    assert frame._meta.station.dtype == object
    result = frame.compute()
    assert result.station.dtype == object
    assert result.station.tolist() == ["A"]
    assert dask.config.get("dataframe.convert-string") == config_before


def test_generated_edge_fixtures_have_stable_shapes(tmp_path):
    from dask_audit_support import (
        make_chunked_gridded_fixture,
        make_malformed_table_fixture,
        make_mixed_schema_fixture,
        make_tabular_fixture,
    )

    paths = make_tabular_fixture(
        tmp_path / "csv", rows=3, columns=10, partitions=4, empty_first=True
    )
    assert len(paths) == 4
    assert pd.read_csv(paths[0]).empty
    assert len(pd.read_csv(paths[1]).columns) == 12
    nc = make_chunked_gridded_fixture(tmp_path / "grid.nc")
    with xr.open_dataset(nc, chunks={}) as ds:
        assert ds["value"].data.chunksize == (2, 8, 8)
    mixed = make_mixed_schema_fixture(tmp_path / "mixed")
    assert pd.read_csv(mixed[0]).columns.tolist() != pd.read_csv(mixed[1]).columns.tolist()
    malformed = make_malformed_table_fixture(tmp_path / "malformed.csv")
    with pytest.raises(pd.errors.ParserError):
        pd.read_csv(malformed)
    for width in (10, 100):
        width_file = make_tabular_fixture(
            tmp_path / f"width-{width}", rows=2, columns=width, partitions=1
        )[0]
        assert len(pd.read_csv(width_file).columns) == width + 2


def test_explicit_schema_handles_empty_first_partition(tmp_path):
    from dask_audit_support import make_tabular_fixture

    paths = make_tabular_fixture(
        tmp_path / "parts", rows=3, columns=2, partitions=3, empty_first=True
    )
    meta = pd.DataFrame(
        {
            "time": pd.Series(dtype="str"),
            "siteid": pd.Series(dtype="str"),
            "v0": pd.Series(dtype="float64"),
            "v1": pd.Series(dtype="float64"),
        }
    )
    frame = PandasDriver(source="airnow").open(
        [str(path) for path in paths],
        read_method="read_csv",
        defer=True,
        meta=meta,
    )
    assert frame.npartitions == 3
    assert len(frame.compute()) == 6


def test_mixed_schema_failure_names_the_offending_input(tmp_path):
    from dask_audit_support import make_mixed_schema_fixture

    paths = make_mixed_schema_fixture(tmp_path / "mixed")
    meta = pd.DataFrame(
        {
            "time": pd.Series(dtype="str"),
            "siteid": pd.Series(dtype="str"),
            "value": pd.Series(dtype="float64"),
        }
    )
    frame = PandasDriver(source="airnow").open(
        [str(path) for path in paths],
        read_method="read_csv",
        defer=True,
        meta=meta,
    )
    with pytest.raises(ValueError, match=r"schema-b\.csv"):
        frame.compute()


def test_delayed_reader_failure_names_the_input(tmp_path):
    path = tmp_path / "broken.csv"
    path.write_text("v\n1\n")

    def reader(fn):
        raise ValueError("invalid payload")

    frame = PandasDriver(source="example").open(
        str(path),
        read_method=reader,
        lazy=True,
        defer=True,
        meta=pd.DataFrame({"value": pd.Series(dtype="int64")}),
    )
    with pytest.raises(ValueError, match=r"invalid payload.*broken.csv"):
        frame.compute()


def test_airnow_schema_probe_reads_only_one_record_per_file(tmp_path):
    from monetio.readers.airnow import _airnow_frame_meta

    hourly = tmp_path / "hourly.dat"
    hourly.write_text("01/01/23|00:00|000000001|S|0|OZONE|PPB|5|X\n" + "x|y\n")
    daily = tmp_path / "daily.dat"
    daily.write_text("01/01/23|000000001|S|OZONE|PPB|5|24|X\n")
    assert "time" in _airnow_frame_meta([str(hourly)], daily=False)
    assert "hours" in _airnow_frame_meta([str(daily)], daily=False)
    with pytest.raises(ValueError, match="mix hourly and daily"):
        _airnow_frame_meta([str(hourly), str(daily)], daily=False)


def test_aeronet_schema_probe_reads_header_and_one_record(tmp_path):
    from monetio.readers.aeronet import _aeronet_local_schema_sample, read_aeronet_csv

    path = tmp_path / "aeronet.csv"
    path.write_text(
        "AERONET Data\nVersion 3\nAOD\nSome info\nAnother info\n"
        "Time(hh:mm:ss),Date(dd:mm:yyyy),Site,Latitude,Longitude,AOD_440nm,AOD_675nm\n"
        "00:00:00,01:01:2024,TEST,40.0,-70.0,0.1,0.05\n"
        "00:01:00,01:01:2024,TEST,40.0,-70.0,0.2,0.06\n"
    )
    sample = _aeronet_local_schema_sample(str(path))
    meta = sample.iloc[:0]
    eager = read_aeronet_csv(str(path))
    assert len(sample) == 1
    assert meta.empty
    assert list(meta.columns) == list(eager.columns)
    assert meta.dtypes.astype(str).to_dict() == eager.dtypes.astype(str).to_dict()
