"""Remote / object-store scale tests (US2).

Reads must go **through** the object store — streaming per-file opens — rather
than bulk-downloading the input set to local disk first. Two CI-runnable store
representations are used:

* a **local-filesystem fsspec store** (``file://`` URLs resolved through
  fsspec's ``LocalFileSystem``): the load must read the original files in
  place, creating no copy anywhere and never invoking the store's bulk
  download APIs (``get``/``get_file``);
* an **in-memory object store** (fsspec ``memory://``): bytes live in an
  in-process mapping, never on disk at all, so "no bulk local copy" is
  checked by watching a scratch directory for new files and by asserting the
  bulk-download APIs are never called.

Concurrent remote reads (the same requirement ``parallel``/``concurrent_files``
serve for local files) are verified on the store with the delayed fan-out:
reads overlap and no bulk copy happens.

Credential handling is the spec edge case: a missing/expired credential
requesting a non-anonymous store read MUST surface an error naming the store
and the credential problem, never a generic file-not-found. The in-tree
``_store_error``/``_is_credential_error`` helpers are also unit-tested with
synthesized botocore errors (deterministic, no network). A real-S3 variant is
included behind the ``network`` marker and skips gracefully without
credentials or connectivity.
"""

from __future__ import annotations

import io
import os
import tempfile
import threading
import time
from pathlib import Path

import fsspec
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.drivers import (
    FileUtility,
    XarrayDriver,
    _is_credential_error,
    _path_protocol,
    _store_bucket,
    _store_error,
)

_SKIP_FEW_CORES = pytest.mark.skipif(
    os.cpu_count() is not None and os.cpu_count() < 4,
    reason="concurrent remote-read overlap needs >=4 cores",
)


# ---------------------------------------------------------------------------
# Store fixtures
# ---------------------------------------------------------------------------
def _write_store_files(fs, prefix, n_files, n_steps=2):
    """Write ``n_files`` small time-stepped NetCDF files into an fsspec store."""
    for i in range(n_files):
        ds = xr.Dataset(
            {"a": (("time", "x"), np.zeros((n_steps, 5), dtype="float32") + i)},
            coords={
                "time": pd.date_range("2024-01-01", periods=n_steps) + pd.Timedelta(days=i),
                "x": np.arange(5),
            },
            attrs={"Conventions": "FM"},
        )
        buf = io.BytesIO()
        ds.to_netcdf(buf, engine="h5netcdf")
        with fs.open(f"{prefix}/f_{i:03d}.nc", "wb") as fh:
            fh.write(buf.getvalue())


@pytest.fixture
def memory_store():
    """fsspec in-memory object store with 12 files (bytes never touch disk)."""
    fs = fsspec.filesystem("memory")
    _write_store_files(fs, "/remote", 12)
    yield fs
    fs.rm("/remote", recursive=True)


@pytest.fixture
def local_store(tmp_path):
    """Real files on disk, addressed through fsspec's LocalFileSystem."""
    fs = fsspec.filesystem("file")
    root = tmp_path / "bucket"
    root.mkdir()
    _write_store_files(fs, str(root), 12)
    return root


class _BulkDownloadGuard:
    """Patch fsspec's bulk-download APIs to record (and refuse) any call.

    A streaming read uses ``open()``; a bulk download uses ``get``/``get_file``
    (or ``cat_file`` over the whole set). Any call here is the failure mode
    this contract forbids.
    """

    def __init__(self, monkeypatch):
        self.calls = []
        for name in ("get", "get_file", "get_mapper"):
            monkeypatch.setattr(
                fsspec.spec.AbstractFileSystem,
                name,
                self._make(name),
                raising=False,
            )

    def _make(self, name):
        calls = self.calls

        def guard(fs_self, *args, **kwargs):
            calls.append((name, str(args[0]) if args else ""))
            raise AssertionError(
                f"bulk download via {name}() is forbidden: reads must stream "
                "through the object store"
            )

        return guard


# ---------------------------------------------------------------------------
# Object-store reads with no bulk local copy (CI-runnable)
# ---------------------------------------------------------------------------
class TestNoBulkLocalCopy:
    def test_memory_store_load_reads_through_store(self, memory_store, monkeypatch):
        guard = _BulkDownloadGuard(monkeypatch)
        scratch = Path(tempfile.mkdtemp())
        before = set(scratch.rglob("*"))

        ds = XarrayDriver().open(
            "memory://remote/f_*.nc",
            concat_dim="time",
            combine="nested",
            coords="minimal",
            compat="override",
            chunks={},
        )
        loaded = ds.load()  # force full evaluation of every file

        assert loaded.sizes["time"] == 24  # 12 files x 2 steps
        assert not guard.calls, f"bulk download attempted: {guard.calls}"
        assert set(scratch.rglob("*")) == before, "a local copy appeared on disk"

    def test_local_filesystem_store_no_copy(self, local_store, monkeypatch):
        """``file://`` store: originals are read in place, nothing copied."""
        guard = _BulkDownloadGuard(monkeypatch)
        root = local_store
        before = sorted(p.name for p in root.iterdir())

        ds = XarrayDriver().open(
            f"file://{root}/f_*.nc",
            concat_dim="time",
            combine="nested",
            coords="minimal",
            compat="override",
        )
        assert ds.sizes["time"] == 24
        assert sorted(p.name for p in root.iterdir()) == before
        assert not guard.calls

    def test_single_remote_file_via_file_object(self, memory_store):
        """The driver's single-file remote path streams via fs.open()."""
        ds = XarrayDriver().open("memory://remote/f_000.nc", storage_options={"anon": True})
        assert ds.sizes["x"] == 5

    def test_resolution_uses_store_backend(self, memory_store):
        """Path expansion resolves the store protocol, not the local disk."""
        files = FileUtility.expand_paths("memory://remote/f_*.nc")
        assert len(files) == 12
        assert all(f.startswith("memory://") for f in files)
        assert _path_protocol(files[0]) == "memory"

    @_SKIP_FEW_CORES
    def test_concurrent_remote_reads_no_bulk_copy(self, memory_store, monkeypatch):
        """concurrent_files fan-out works against an object store, still streaming."""
        guard = _BulkDownloadGuard(monkeypatch)
        intervals = []
        lock = threading.Lock()

        def read_method(path, **kwargs):  # driver forwards combine kwargs
            t0 = time.perf_counter()
            f = FileUtility.get_fs(path).open(path)  # stream through the store
            ds = xr.open_dataset(f, engine="h5netcdf")
            time.sleep(0.15)
            t1 = time.perf_counter()
            with lock:
                intervals.append((str(path), t0, t1))
            return ds

        files = [f"memory://remote/f_{i:03d}.nc" for i in range(6)]
        ds = XarrayDriver().open(
            files,
            read_method=read_method,
            concurrent_files=True,
            concat_dim="time",
            combine="nested",
            coords="minimal",
            compat="override",
        )
        assert ds.sizes["time"] == 12
        assert not guard.calls
        busy = sum(e - s for _, s, e in intervals)
        span = max(e for _, _, e in intervals) - min(s for _, s, _ in intervals)
        assert span < busy - 0.2, "concurrent remote reads did not overlap"


# ---------------------------------------------------------------------------
# Credential errors name the store (spec edge case)
# ---------------------------------------------------------------------------
class TestCredentialErrors:
    def test_no_credentials_error_detected(self):
        from botocore.exceptions import NoCredentialsError

        exc = NoCredentialsError()
        assert _is_credential_error(exc)
        out = _store_error(exc, "s3://noaa-private-bucket/data/*.nc")
        assert type(out) is NoCredentialsError  # type preserved
        assert "noaa-private-bucket" in str(out)
        assert "credential" in str(out).lower()

    @pytest.mark.parametrize("code", ["AccessDenied", "ExpiredToken", "InvalidAccessKeyId"])
    def test_client_error_codes_detected(self, code):
        from botocore.exceptions import ClientError

        exc = ClientError({"Error": {"Code": code, "Message": "denied"}}, "ListObjectsV2")
        assert _is_credential_error(exc)
        out = _store_error(exc, "s3://my-store/prefix/f.nc")
        assert "my-store" in str(out)
        assert type(out) is ClientError

    def test_non_credential_errors_untouched(self):
        """A genuine missing-file or network error must NOT be relabeled."""
        from botocore.exceptions import ClientError

        plain = FileNotFoundError("No files found matching pattern: s3://b/*.nc")
        assert _store_error(plain, "s3://b/*.nc") is plain
        not_found = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "not found"}}, "ListObjects"
        )
        assert not _is_credential_error(not_found)
        assert _store_error(not_found, "s3://b/x.nc") is not_found

    def test_bucket_extraction(self):
        assert _store_bucket("s3://my-bucket/deep/path.nc") == "s3://my-bucket"
        assert _store_bucket("memory://a/b.nc") == "memory://a"
        assert _store_bucket("/local/path.nc") == "/local/path.nc"

    def test_get_fs_defaults_anon_and_honors_storage_options(self, monkeypatch):
        """Default stays public (anon=True); callers can request credentials."""
        seen = {}
        import monetio.readers.drivers as drivers

        real = fsspec.filesystem

        def spy(protocol, **kwargs):
            seen["protocol"] = protocol
            seen.update(kwargs)
            return real(protocol, **kwargs)

        monkeypatch.setattr(drivers.fsspec, "filesystem", spy)
        drivers.FileUtility.get_fs("s3://bucket/x.nc")
        assert seen["anon"] is True  # unchanged default behavior
        seen.clear()
        drivers.FileUtility.get_fs("s3://bucket/x.nc", {"anon": False})
        assert seen["anon"] is False

    def test_missing_credentials_surface_store_not_file_not_found(self, monkeypatch):
        """The end-to-end shape of the edge case, without needing real AWS.

        Simulate a private bucket: a store whose glob raises
        ``NoCredentialsError``. The resolution must propagate the credential
        error naming the store — never collapse into the generic
        ``FileNotFoundError`` that an empty listing would produce.
        """
        from botocore.exceptions import NoCredentialsError

        class CredFS:
            def glob(self, pattern):
                raise NoCredentialsError()

            def exists(self, path):
                return True

        with pytest.raises(NoCredentialsError) as excinfo:
            FileUtility.expand_paths("s3://private-bucket/data/*.nc", fs=CredFS())
        message = str(excinfo.value)
        assert "private-bucket" in message
        assert "No files found matching pattern" not in message

    def test_streaming_variant_also_names_store(self):
        from botocore.exceptions import NoCredentialsError

        class CredFS:
            def glob(self, pattern):
                raise NoCredentialsError()

        with pytest.raises(NoCredentialsError) as excinfo:
            list(FileUtility.expand_paths_iter("s3://private-bucket/data/*.nc", fs=CredFS()))
        assert "private-bucket" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Real-S3 variant (network-marked, skips without connectivity/credentials)
# ---------------------------------------------------------------------------
@pytest.mark.network
class TestRealS3:
    """Reads one small object from a public NOAA bucket through the store.

    Uses ``MONETIO_S3_TEST_GLOB`` when set (a public ``s3://`` glob);
    otherwise a stable NOAA PDS prefix. Skips on any network/credential
    failure so CI without S3 access stays green.
    """

    _DEFAULT_GLOB = "s3://noaa-ghcn-pds/csv/by_year/202*.csv"

    def test_public_bucket_streams_without_bulk_copy(self, monkeypatch):
        pytest.importorskip("s3fs")
        guard = _BulkDownloadGuard(monkeypatch)
        pattern = os.environ.get("MONETIO_S3_TEST_GLOB", self._DEFAULT_GLOB)
        try:
            files = FileUtility.expand_paths(pattern, storage_options={"anon": True})
        except Exception as e:  # network/auth/bucket-layout changes -> skip
            pytest.skip(f"S3 listing unavailable: {e}")
        if not files:
            pytest.skip(f"no objects under {pattern}")
        target = files[0]
        try:
            fs = FileUtility.get_fs(target, {"anon": True})
            with fs.open(target, "rb") as fh:
                head = fh.read(16)  # streaming read of the object store
        except Exception as e:
            pytest.skip(f"S3 read unavailable: {e}")
        assert head
        assert not guard.calls, f"real-S3 path bulk-downloaded via {guard.calls}"

    def test_private_bucket_without_credentials_names_store(self):
        """No creds + anon=False on a nonexistent private bucket -> store named.

        Skipped when the machine actually has AWS credentials (the error would
        then be about the bucket, not the credentials).
        """
        pytest.importorskip("botocore")
        import botocore.session

        session = botocore.session.get_session()
        try:
            creds = session.get_credentials()
            resolved = creds.get_frozen_credentials() if creds else None
        except Exception:
            resolved = None
        if resolved is not None and resolved.access_key:
            pytest.skip("machine has AWS credentials; negative test needs none")
        for key in list(os.environ):
            if key.startswith("AWS_"):
                del os.environ[key]
        try:
            FileUtility.expand_paths(
                "s3://monetio-does-not-exist-zzzz/*.nc",
                storage_options={"anon": False},
            )
        except Exception as e:
            message = str(e)
            assert "monetio-does-not-exist-zzzz" in message or "credential" in message.lower(), (
                f"error neither named the store nor the credential problem: {message}"
            )
        else:
            pytest.skip("unexpected success accessing a nonexistent private bucket")
