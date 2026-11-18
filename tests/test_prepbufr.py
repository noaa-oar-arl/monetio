"""Offline tests of the documented NCEPLIBS-bufr Python interface."""

import sys
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

import monetio
from monetio.readers import prepbufr


@pytest.fixture
def bufr_files(tmp_path, monkeypatch):
    handles = []

    class BUFR:
        missing_value = 1e11
        msg_date = 2024010100

        def __init__(self, path, table=None):
            self.message = -1
            self.closed = False
            handles.append(self)

        def advance(self):
            self.message += 1
            self.loaded = False
            if self.message == 6:
                return 1
            self.msg_type = (*prepbufr.SUBSETS, "SATWND")[self.message]
            return 0

        def load_subset(self):
            if self.loaded:
                return 1
            self.loaded = True
            return 0

        def read_subset(self, mnemonics):
            if mnemonics == prepbufr._HEADER:
                sid = np.frombuffer(b"TEST    ", dtype="float64")[0]
                return np.ma.array([sid, 270, 40, -0.5, 120, 100])[:, None]
            assert mnemonics == " ".join(prepbufr._FIELDS)
            data = np.array(
                [
                    [1000, 850],
                    [10000, 5000],
                    [20, 10],
                    [100, 1500],
                    [3, 0],
                    [4, 0],
                    [15, self.missing_value],
                    [0, 1],
                    [2, 2],
                    [2, 2],
                    [2, 3],
                    [2, 2],
                    [2, 2],
                ],
                dtype=float,
            )
            return np.ma.masked_values(data, self.missing_value)

        def close(self):
            self.closed = True

    monkeypatch.setitem(sys.modules, "ncepbufr", SimpleNamespace(open=BUFR))
    paths = [tmp_path / "one.bufr", tmp_path / "two.bufr"]
    for path in paths:
        path.touch()
    return [str(path) for path in paths], handles


@pytest.mark.parametrize("subset", prepbufr.SUBSETS)
@pytest.mark.parametrize("use_dask", [False, True])
def test_read_types(bufr_files, subset, use_dask):
    paths, handles = bufr_files
    ds = monetio.load("prepbufr", files=paths, subsets=subset, use_dask=use_dask)
    assert ds.sizes == {"node": 4}
    if use_dask:
        assert hasattr(ds.temperature.data, "dask")
    ds = ds.compute()
    np.testing.assert_allclose(ds.pressure, [100000, 85000] * 2)
    np.testing.assert_allclose(ds.specific_humidity, [0.01, 0.005] * 2)
    np.testing.assert_allclose(ds.wind_speed, [5, 0] * 2)
    assert np.isnan(ds.wind_dir[1])
    assert ds.dewpoint[0].item() == 15
    assert np.isfinite(ds.dewpoint[1])
    assert 0 < ds.rel_humidity[1].item() < 100
    assert ds.siteid[0].item() == "TEST"
    assert ds.longitude[0].item() == -90
    assert ds.time[0].data == np.datetime64("2023-12-31T23:30:00")
    assert ds.temperature.attrs["units"] == "degC"
    assert ds.temperature_quality[1].item() == 3
    assert all(handle.closed for handle in handles)


def test_eager_lazy_parity(bufr_files):
    paths, _ = bufr_files
    reader = prepbufr.PREPBUFRReader()
    eager = reader.open_dataset(paths)
    lazy = reader.open_dataset(paths, use_dask=True).compute()
    eager.attrs.pop("history")
    lazy.attrs.pop("history")
    xr.testing.assert_identical(eager, lazy)
    assert eager.sizes["node"] == 20


def test_lazy_table_does_not_decode(bufr_files):
    paths, handles = bufr_files
    df = monetio.load("prepbufr", files=paths, use_dask=True, as_xarray=False)
    assert not handles
    assert len(df.compute()) == 20
    assert all(handle.closed for handle in handles)


def test_errors_close_handle(bufr_files, monkeypatch):
    paths, handles = bufr_files
    original = sys.modules["ncepbufr"].open

    class Broken(original):
        def read_subset(self, mnemonics):
            raise RuntimeError("decoder failed")

    monkeypatch.setitem(sys.modules, "ncepbufr", SimpleNamespace(open=Broken))
    with pytest.raises(RuntimeError, match="decoder failed"):
        prepbufr.read_prepbufr(paths[0])
    assert handles[-1].closed


def test_invalid_selection_and_layout(bufr_files):
    paths, handles = bufr_files
    with pytest.raises(ValueError, match="subsets"):
        monetio.load("prepbufr", files=paths, subsets="SATWND")
    with pytest.raises(ValueError, match="expand2d"):
        monetio.load("prepbufr", files=paths, expand2d=True)
    assert not handles


def test_missing_dependency(bufr_files, monkeypatch):
    paths, _ = bufr_files
    monkeypatch.setitem(sys.modules, "ncepbufr", None)
    with pytest.raises(ImportError, match="NCEPLIBS-bufr"):
        prepbufr.read_prepbufr(paths[0])


def test_missing_values_and_no_matching_reports(bufr_files, monkeypatch):
    paths, handles = bufr_files
    original = sys.modules["ncepbufr"].open

    class Missing(original):
        def read_subset(self, mnemonics):
            data = super().read_subset(mnemonics)
            if mnemonics == prepbufr._HEADER:
                data[3, 0] = np.ma.masked
                data[2, 0] = np.ma.masked
            return data

    monkeypatch.setitem(sys.modules, "ncepbufr", SimpleNamespace(open=Missing))
    ds = monetio.load("prepbufr", files=paths, subsets="ADPUPA", use_dask=True).compute()
    assert ds.sizes["node"] == 4
    assert np.isnat(ds.time).all()
    assert np.isnan(ds.latitude).all()

    class NoMatch(original):
        def advance(self):
            result = super().advance()
            self.msg_type = "SATWND"
            return result

    monkeypatch.setitem(sys.modules, "ncepbufr", SimpleNamespace(open=NoMatch))
    df = prepbufr.read_prepbufr(paths[0])
    assert df.empty
    assert df.dtypes.equals(prepbufr._schema().dtypes)
    assert all(handle.closed for handle in handles)
