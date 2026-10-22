# Feature: virtualizarr-reader-refactor, Property 1: Store Selection by Protocol
"""Property-based test for _select_store() protocol-based store selection.

**Validates: Requirements 1.5, 1.6, 1.7**

For any file path with a recognized protocol prefix (s3://, http://, https://, or
local path), _select_store SHALL select the correct object store type and, for local
files, prefix them with file://.
"""

import unittest.mock as mock
from pathlib import Path

from hypothesis import example, given, settings, strategies as st

from monetio.readers.drivers import _select_store

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Random alphanumeric filenames with .nc extension
_filename = st.from_regex(r"[a-zA-Z0-9]{1,20}\.nc", fullmatch=True)

# S3 paths: s3://bucket/path/file.nc
_s3_path = st.builds(
    lambda bucket, subdir, fname: f"s3://{bucket}/{subdir}/{fname}",
    bucket=st.from_regex(r"[a-z][a-z0-9\-]{2,20}", fullmatch=True),
    subdir=st.from_regex(r"[a-z0-9]{1,10}", fullmatch=True),
    fname=_filename,
)

# HTTP paths
_http_path = st.builds(
    lambda host, path, fname: f"http://{host}/{path}/{fname}",
    host=st.from_regex(r"[a-z][a-z0-9]{2,15}\.[a-z]{2,4}", fullmatch=True),
    path=st.from_regex(r"[a-z0-9]{1,10}", fullmatch=True),
    fname=_filename,
)

# HTTPS paths
_https_path = st.builds(
    lambda host, path, fname: f"https://{host}/{path}/{fname}",
    host=st.from_regex(r"[a-z][a-z0-9]{2,15}\.[a-z]{2,4}", fullmatch=True),
    path=st.from_regex(r"[a-z0-9]{1,10}", fullmatch=True),
    fname=_filename,
)

# Local absolute paths
_local_path = st.builds(
    lambda dirs, fname: f"/{dirs}/{fname}",
    dirs=st.from_regex(r"[a-z][a-z0-9]{1,10}(/[a-z0-9]{1,10}){0,3}", fullmatch=True),
    fname=_filename,
)

# Combined protocol strategy — one of the four types
_any_protocol_path = st.one_of(_s3_path, _http_path, _https_path, _local_path)


def _protocol_of(path: str) -> str:
    """Return the protocol category for a given path."""
    if path.startswith("s3://"):
        return "s3"
    if path.startswith("https://"):
        return "https"
    if path.startswith("http://"):
        return "http"
    return "local"


# ---------------------------------------------------------------------------
# Shared mock fixture
# ---------------------------------------------------------------------------


def _mock_obstore_context():
    """Return a context manager that mocks obstore/obspec_utils modules."""
    MockS3Store = mock.MagicMock()
    MockHTTPStore = mock.MagicMock()
    MockLocalStore = mock.MagicMock()
    MockRegistry = mock.MagicMock()

    mock_obstore_store = mock.MagicMock()
    mock_obstore_store.S3Store = MockS3Store
    mock_obstore_store.HTTPStore = MockHTTPStore
    mock_obstore_store.LocalStore = MockLocalStore

    mock_obspec_registry = mock.MagicMock()
    mock_obspec_registry.ObjectStoreRegistry = MockRegistry

    ctx = mock.patch.dict(
        "sys.modules",
        {
            "obstore": mock.MagicMock(),
            "obstore.store": mock_obstore_store,
            "obspec_utils": mock.MagicMock(),
            "obspec_utils.registry": mock_obspec_registry,
        },
    )

    return ctx, MockS3Store, MockHTTPStore, MockLocalStore, MockRegistry


# ---------------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(path=_s3_path)
def test_s3_paths_select_s3store(path):
    """S3 paths must instantiate S3Store with the correct bucket."""
    ctx, MockS3, MockHTTP, MockLocal, MockReg = _mock_obstore_context()
    with ctx:
        registry, result_files = _select_store([path], {"anon": True})

    expected_bucket = path.replace("s3://", "").split("/")[0]
    MockS3.assert_called_once()
    call_args = MockS3.call_args
    assert call_args[0][0] == expected_bucket, (
        f"Expected S3Store({expected_bucket!r}), got S3Store({call_args[0][0]!r})"
    )
    MockHTTP.assert_not_called()
    MockLocal.assert_not_called()
    assert result_files == [path]


@example("http://abc.example/path/file.nc")
@example("https://abc.example/path/file.nc")
@settings(max_examples=100)
@given(path=st.one_of(_http_path, _https_path))
def test_http_and_https_paths_select_httpstore(path):
    """HTTP and HTTPS paths must instantiate HTTPStore."""
    ctx, MockS3, MockHTTP, MockLocal, MockReg = _mock_obstore_context()
    with ctx:
        registry, result_files = _select_store([path], {})

    MockHTTP.assert_called_once()
    MockS3.assert_not_called()
    MockLocal.assert_not_called()
    assert result_files == [path]


@settings(max_examples=100)
@given(path=_local_path)
def test_local_paths_select_localstore_and_prefix(path):
    """Local paths must instantiate LocalStore and prefix files with file://."""
    ctx, MockS3, MockHTTP, MockLocal, MockReg = _mock_obstore_context()
    with ctx:
        registry, result_files = _select_store([path], {})

    MockLocal.assert_called_once()
    MockS3.assert_not_called()
    MockHTTP.assert_not_called()
    # Local files must be prefixed with file://
    assert len(result_files) == 1
    assert result_files[0].startswith("file://"), (
        f"Local path should be prefixed with file://, got {result_files[0]!r}"
    )
    # A resolved URI preserves Windows drive letters and spaces correctly.
    assert result_files[0] == Path(path).resolve().as_uri()


@settings(max_examples=100)
@given(
    paths=st.lists(_any_protocol_path, min_size=1, max_size=5).filter(
        lambda ps: len({_protocol_of(p) for p in ps}) == 1
    )
)
def test_homogeneous_file_lists_select_consistent_store(paths):
    """A list of files sharing the same protocol must select one consistent store type."""
    ctx, MockS3, MockHTTP, MockLocal, MockReg = _mock_obstore_context()
    with ctx:
        registry, result_files = _select_store(paths, {"anon": True})

    protocol = _protocol_of(paths[0])

    if protocol == "s3":
        MockS3.assert_called_once()
        MockHTTP.assert_not_called()
        MockLocal.assert_not_called()
        assert result_files == paths
    elif protocol in ("http", "https"):
        MockHTTP.assert_called_once()
        MockS3.assert_not_called()
        MockLocal.assert_not_called()
        assert result_files == paths
    else:
        # local
        MockLocal.assert_called_once()
        MockS3.assert_not_called()
        MockHTTP.assert_not_called()
        for orig, result in zip(paths, result_files):
            assert result.startswith("file://"), (
                f"Local path should be prefixed with file://, got {result!r}"
            )
