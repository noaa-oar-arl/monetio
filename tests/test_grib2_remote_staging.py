"""Regression tests: remote GRIB2 files are staged locally for grib2io.

grib2io's lazy chunk reader opens recorded filenames with a plain
``open()``, so s3/http URLs fail at data-read time. The driver must
download remote GRIB2 files to local disk before opening them, and reuse
the staged bytes on subsequent loads.
"""

import pytest

from monetio.readers import drivers
from monetio.readers.drivers import _stage_remote_grib2_files


class _FakeFS:
    def __init__(self, payload=b"GRIB" * 16, fail=False):
        self.payload = payload
        self.fail = fail
        self.calls = []

    def get(self, rpath, lpath):
        self.calls.append((rpath, lpath))
        if self.fail:
            raise OSError("simulated download failure")
        with open(lpath, "wb") as fh:
            fh.write(self.payload)


@pytest.fixture
def stage_dir(tmp_path, monkeypatch):
    d = tmp_path / "stage"
    monkeypatch.setenv("MONETIO_GRIB2_STAGE_DIR", str(d))
    return d


def test_remote_urls_are_downloaded_to_local_paths(stage_dir, monkeypatch):
    fs = _FakeFS()
    monkeypatch.setattr(drivers.FileUtility, "get_fs", staticmethod(lambda path, opts=None: fs))

    out = _stage_remote_grib2_files(["s3://bucket/a.grib2", "https://host/b.grib2"])

    assert len(out) == 2
    for path in out:
        assert not path.startswith(("s3://", "http"))
        assert path.startswith(str(stage_dir))
        with open(path, "rb") as fh:
            assert fh.read() == fs.payload
    assert len(fs.calls) == 2


def test_local_paths_pass_through_unchanged(stage_dir):
    out = _stage_remote_grib2_files(["/data/local.grib2", "relative.grib2"])
    assert out == ["/data/local.grib2", "relative.grib2"]


def test_staged_files_are_reused(stage_dir, monkeypatch):
    fs = _FakeFS()
    monkeypatch.setattr(drivers.FileUtility, "get_fs", staticmethod(lambda path, opts=None: fs))

    first = _stage_remote_grib2_files(["s3://bucket/a.grib2"])
    second = _stage_remote_grib2_files(["s3://bucket/a.grib2"])

    assert first == second
    assert len(fs.calls) == 1  # second load reused the staged bytes


def test_failed_download_leaves_no_complete_file(stage_dir, monkeypatch):
    fs = _FakeFS(fail=True)
    monkeypatch.setattr(drivers.FileUtility, "get_fs", staticmethod(lambda path, opts=None: fs))

    with pytest.raises(OSError, match="simulated download failure"):
        _stage_remote_grib2_files(["s3://bucket/a.grib2"], attempts=1)

    assert not list(stage_dir.rglob("a.grib2"))
    assert not list(stage_dir.rglob("*.part-*"))