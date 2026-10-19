"""File-set resolution at scale (US2).

The default :meth:`FileUtility.expand_paths` returns a fully materialized,
sorted list — correct and unchanged for ordinary inputs. For very large input
sets, the streaming :meth:`FileUtility.expand_paths_iter` variant resolves
paths without ever building a single full sorted collection: it consumes the
glob in bounded runs, sorts each run, and lazily k-way-merges them, yielding
the **same globally sorted order** as ``expand_paths`` (file order determines
``concat``/``combine_nested`` results, so order must not change).

The scale requirement is verified *structurally* (per the spec: "by inspecting
the resolution path rather than by wall time alone"):

* the resolution is a lazy generator, not a list;
* every ``sorted()`` call the implementation makes receives a bounded working
  set (never the full 100,000-path collection) — contrasted against the
  default path, which does materialize one full sorted list;
* the merge is a lazy k-way merge over the bounded runs;
* the working set per sort equals the configured chunk size, so memory is
  bounded by the chunk, not by the size of the input set.

The existing failure contract is preserved: a glob matching nothing still
raises ``FileNotFoundError`` with the same message form.
"""

from __future__ import annotations

import builtins
import heapq
import re

import fsspec
import pytest

import monetio.readers.drivers as drivers
from monetio.readers.drivers import FileUtility

_N_PATHS = 100_000


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def big_mem_fs():
    """In-memory fsspec filesystem with >=100,000 paths under /big/."""
    fs = fsspec.filesystem("memory")
    for i in range(_N_PATHS):
        with fs.open(f"/big/dir_{i % 50:02d}/f_{i:06d}.nc", "wb") as fh:
            fh.write(b"x")
    return fs


class _CountingFS:
    """Filesystem stub whose glob yields lazily and counts consumption.

    ``glob`` returns a *generator*, so the number of paths actually pulled
    from the directory listing can be observed — the direct evidence that a
    streaming consumer does not walk the full set.
    """

    def __init__(self, paths):
        self._paths = list(paths)
        self.consumed = 0

    def glob(self, pattern):
        def _gen():
            for p in self._paths:
                self.consumed += 1
                yield p

        return _gen()

    def exists(self, path):
        return path in self._paths


class _SortedSpy:
    """Module-global ``sorted`` replacement recording each call's input size.

    ``expand_paths_iter`` sorts bounded ``islice`` runs (generators, no
    ``__len__``); a materializing implementation would sort a full list.
    """

    def __init__(self, real):
        self.real = real
        self.calls = []  # (size_or_None, result_len)

    def __call__(self, iterable, *args, **kwargs):
        out = self.real(iterable, *args, **kwargs)
        try:
            size = len(iterable)
        except TypeError:
            size = None  # lazy iterable (generator/islice): bounded by design
        self.calls.append((size, len(out)))
        return out


@pytest.fixture
def sorted_spy(monkeypatch):
    spy = _SortedSpy(builtins.sorted)
    # expand_paths/expand_paths_iter resolve ``sorted`` from the drivers
    # module globals, so shadowing it there observes their internal calls
    # only. ``raising=False``: the module has no ``sorted`` attribute until
    # this shadow is installed (it is a builtin lookup).
    monkeypatch.setattr(drivers, "sorted", spy, raising=False)
    return spy


@pytest.fixture
def merge_spy(monkeypatch):
    """Record the number of iterables handed to ``heapq.merge``."""
    calls = []
    real = heapq.merge

    def spy(*iterables, **kwargs):
        calls.append(len(iterables))
        return real(*iterables, **kwargs)

    monkeypatch.setattr(heapq, "merge", spy)
    return calls


# ---------------------------------------------------------------------------
# Streaming resolution at >=100,000 paths — structural inspection
# ---------------------------------------------------------------------------
class TestStreamingResolution:
    def test_returns_lazy_generator(self, big_mem_fs):
        gen = FileUtility.expand_paths_iter("/big/dir_*/*.nc", fs=big_mem_fs)
        assert not isinstance(gen, list), "expand_paths_iter must not materialize a list"
        assert iter(gen) is gen  # a generator, consumed lazily
        next(gen)
        gen.close()

    def test_no_full_sorted_collection(self, big_mem_fs, sorted_spy):
        """SC-017: no ``sorted()`` call receives the full 100,000-path set."""
        result = list(FileUtility.expand_paths_iter("/big/dir_*/*.nc", fs=big_mem_fs))
        assert len(result) == _N_PATHS
        assert sorted_spy.calls, "implementation performed no bounded sorts"
        for size, result_len in sorted_spy.calls:
            assert size is None or size < _N_PATHS, (
                f"sorted() materialized a full collection ({size} paths)"
            )
            assert result_len <= 10_000, (
                f"sorted run of {result_len} exceeds the bounded chunk size"
            )

    def test_default_path_does_materialize_one_sorted_list(self, big_mem_fs, sorted_spy):
        """Contrast: the unchanged default path sorts the full set once."""
        files = FileUtility.expand_paths("/big/dir_*/*.nc", fs=big_mem_fs)
        assert len(files) == _N_PATHS
        full_sorts = [c for c in sorted_spy.calls if c[1] == _N_PATHS]
        assert full_sorts, "default expand_paths no longer sorts the full set"

    def test_k_way_merge_over_bounded_runs(self, big_mem_fs, merge_spy):
        """The merge consumes chunked runs, not one giant pre-sorted list."""
        list(FileUtility.expand_paths_iter("/big/dir_*/*.nc", fs=big_mem_fs))
        assert merge_spy == [pytest.approx(10)], (
            f"expected 10 bounded runs (100k / 10k chunk), got {merge_spy}"
        )

    def test_order_identical_to_default(self, big_mem_fs):
        """Globally sorted order preserved — concat results stay deterministic."""
        default = FileUtility.expand_paths("/big/dir_*/*.nc", fs=big_mem_fs)
        streamed = list(FileUtility.expand_paths_iter("/big/dir_*/*.nc", fs=big_mem_fs))
        assert streamed == default

    def test_bounded_working_set_independent_of_set_size(self, sorted_spy):
        """The sort working set is the chunk, not the whole input set.

        With a 200,000-path listing and a 5,000-path chunk, every individual
        ``sorted()`` call sees at most one chunk — memory is bounded by the
        chunk size, not by the size of the input set.
        """
        paths = [f"/x/f_{i:06d}.nc" for i in range(200_000)]
        fs = _CountingFS(paths)
        result = list(FileUtility.expand_paths_iter("/x/*.nc", fs=fs, chunk_size=5_000))
        assert result == sorted(paths)
        # 40 bounded runs + one empty terminator from the drain loop's last check.
        non_empty = [c for c in sorted_spy.calls if c[1] > 0]
        assert len(non_empty) == 40  # 200k / 5k bounded runs
        assert len(sorted_spy.calls) == 41  # + the empty final islice
        assert all(size is None or size == 5_000 for size, _ in sorted_spy.calls)
        assert max((n for _, n in sorted_spy.calls), default=0) == 5_000

    def test_custom_chunk_size_bounds_runs(self):
        paths = [f"/y/f_{i:04d}.nc" for i in range(1_000)]
        fs = _CountingFS(paths)
        result = list(FileUtility.expand_paths_iter("/y/*.nc", fs=fs, chunk_size=64))
        assert result == sorted(paths)
        assert fs.consumed == 1_000  # full consumption when fully drained


class TestStreamingEquivalenceAndTypes:
    def test_list_input_matches_default(self):
        paths = ["/b/x.nc", "/a/y.nc", "/a/z.nc"]
        assert list(FileUtility.expand_paths_iter(paths)) == FileUtility.expand_paths(paths)

    def test_single_existing_file(self, tmp_path):
        f = tmp_path / "one.nc"
        f.write_bytes(b"")
        assert list(FileUtility.expand_paths_iter(str(f))) == FileUtility.expand_paths(str(f))

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError, match="Invalid path type"):
            list(FileUtility.expand_paths_iter(42))
        with pytest.raises(TypeError, match="Invalid path type"):
            FileUtility.expand_paths(42)


# ---------------------------------------------------------------------------
# Failure contract preserved (spec edge case)
# ---------------------------------------------------------------------------
class TestEmptyResolutionStillRaises:
    def test_empty_glob_default_message_form(self, tmp_path):
        with pytest.raises(FileNotFoundError) as excinfo:
            FileUtility.expand_paths(str(tmp_path / "nope_*.nc"))
        assert str(excinfo.value) == f"No files found matching pattern: {tmp_path / 'nope_*.nc'}"

    def test_empty_glob_streaming_same_message_form(self, tmp_path):
        pattern = str(tmp_path / "nope_*.nc")
        with pytest.raises(FileNotFoundError) as excinfo:
            # The error must surface even lazily (on first consumption).
            next(FileUtility.expand_paths_iter(pattern))
        assert str(excinfo.value) == f"No files found matching pattern: {pattern}"

    def test_empty_glob_on_memory_fs(self, big_mem_fs):
        with pytest.raises(FileNotFoundError, match="No files found matching pattern"):
            list(FileUtility.expand_paths_iter("/big/absent_*.nc", fs=big_mem_fs))

    def test_missing_single_file_streaming(self, tmp_path):
        missing = str(tmp_path / "absent.nc")
        # The path is platform-dependent (Windows paths contain backslashes,
        # which are invalid regex escapes), so escape it before matching.
        with pytest.raises(FileNotFoundError, match=re.escape(f"File not found: {missing}")):
            next(FileUtility.expand_paths_iter(missing))

    def test_empty_list_input_yields_empty(self):
        assert list(FileUtility.expand_paths_iter([])) == []
        assert FileUtility.expand_paths([]) == []
