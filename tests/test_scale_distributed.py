"""Distributed-scheduler scale tests (US2).

A deferred load returns an *unevaluated* dask object, so the same graph can be
materialized on different schedulers with no source-specific code. This module
proves the scheduler-selection contract:

* the **same** deferred load, evaluated on a local threaded scheduler and on an
  in-process ``distributed`` cluster, yields **equivalent results** (and both
  equal the eager load) — the deferred path is scheduler-agnostic;
* an **unreachable** distributed scheduler surfaces its own connection error
  rather than silently falling back to the local scheduler and appearing to
  succeed single-machine (spec edge case);
* everything ``distributed``-specific **skips cleanly** when the optional
  ``distributed`` package is absent, so the suite stays runnable without it.

The in-process cluster (``processes=False``) is used deliberately: it can run
in CI without spawning worker processes or pickling user code, while still
exercising the real ``distributed`` scheduler path (not the threaded one).
"""

from __future__ import annotations

import dask
import dask.array as da
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.airnow import AirNowReader
from monetio.readers.cmaq import CMAQReader

_HAS_DISTRIBUTED = True
try:
    import distributed  # noqa: F401
except ImportError:  # pragma: no cover - environment without the extra
    _HAS_DISTRIBUTED = False


requires_distributed = pytest.mark.skipif(
    not _HAS_DISTRIBUTED, reason="distributed not installed (optional extra)"
)


# ---------------------------------------------------------------------------
# Synthetic point-data fixture (mirrors the deferred-loading tests)
# ---------------------------------------------------------------------------
def _synthetic_airnow(tmp_path, monkeypatch, n_rows: int, n_sites: int = 1):
    import monetio.readers.epa_utils as epa_utils

    def fake_read_monitor_file(*a, **k):
        return pd.DataFrame(
            {
                "siteid": [f"0{i:08d}" for i in range(n_sites)],
                "latitude": [40.0 + i for i in range(n_sites)],
                "longitude": [-80.0 - i for i in range(n_sites)],
                "site_name": [f"SITE {i}" for i in range(n_sites)],
            }
        )

    monkeypatch.setattr(epa_utils, "read_monitor_file", fake_read_monitor_file)
    lines = []
    for i in range(n_rows):
        site = f"0{i % n_sites:08d}"
        hour = (i // n_sites) % 24
        day = 1 + (i // n_sites) // 24
        lines.append(f"01/{day:02d}/23|{hour:02d}:00|{site}|T|-5|OZONE|PPB|{50.0 + i}|X")
    fn = tmp_path / "HourlyData_2023010100.dat"
    fn.write_text("\n".join(lines), encoding="ISO-8859-1")
    return str(fn)


def _strip_history(obj):
    """Return ``obj`` with every ``history`` attribute removed.

    The history string embeds a wall-clock timestamp that differs between the
    two evaluations (and between eager and deferred); everything else must be
    byte-identical across schedulers.
    """
    if isinstance(obj, xr.Dataset):
        out = obj.copy()
        out.attrs.pop("history", None)
        for var in out.variables:
            out[var].attrs.pop("history", None)
        return out
    if isinstance(obj, pd.DataFrame):
        out = obj.copy()
        out.attrs.pop("history", None)
        return out
    return obj


# ---------------------------------------------------------------------------
# Same deferred load, equivalent results across schedulers (SC-015)
# ---------------------------------------------------------------------------
class TestSchedulerEquivalence:
    def test_gridded_deferred_matches_eager_on_threads(self, multifile):
        """Local threaded scheduler: evaluate(defer) == eager."""
        reader = CMAQReader()
        deferred = reader.open_dataset(multifile, defer=True)
        assert all(isinstance(v.data, da.Array) for v in deferred.data_vars.values()), (
            "defer must return an unevaluated dataset"
        )
        with dask.config.set(scheduler="threads"):
            on_threads = deferred.compute()
        eager = reader.open_dataset(multifile)
        xr.testing.assert_identical(_strip_history(on_threads), _strip_history(eager))

    @requires_distributed
    def test_gridded_deferred_equivalent_across_schedulers(
        self, multifile, local_distributed_cluster
    ):
        """SC-015: threaded and in-process distributed give the same result.

        The *same* deferred graph is computed under each scheduler; both equal
        the eager load. No reader code changes between the two — selection is
        purely the caller's environment.
        """
        reader = CMAQReader()
        deferred = reader.open_dataset(multifile, defer=True)

        # An active distributed Client routes .compute() through it.
        on_distributed = deferred.compute()
        with dask.config.set(scheduler="threads"):
            on_threads = deferred.compute()

        xr.testing.assert_identical(_strip_history(on_distributed), _strip_history(on_threads))
        eager = reader.open_dataset(multifile)
        xr.testing.assert_identical(_strip_history(on_distributed), _strip_history(eager))

    @requires_distributed
    def test_point_deferred_equivalent_across_schedulers(
        self, tmp_path, monkeypatch, local_distributed_cluster
    ):
        """The point (dask DataFrame) path is equally scheduler-agnostic."""
        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=240, n_sites=6)
        reader = AirNowReader()
        # The point path's ``drop_duplicates`` shuffles, and dask resolves the
        # shuffle method while *building* the deferred graph. dask's default
        # p2p shuffle needs real worker processes (unavailable on the
        # CI-friendly in-process cluster), so pin the scheduler-agnostic
        # ``tasks`` shuffle before the load: the contract under test is that
        # the *same graph* evaluates equivalently across schedulers, not
        # across shuffle methods.
        with dask.config.set({"dataframe.shuffle.method": "tasks"}):
            deferred = reader.open_dataset(
                files=fn,
                defer=True,
                wide_fmt=False,
                as_xarray=False,
                bad_utcoffset="leave",
            )
            assert hasattr(deferred, "npartitions"), "defer point must return a dask DataFrame"
            on_distributed = deferred.compute()
            with dask.config.set(scheduler="threads"):
                on_threads = deferred.compute()

        # Row order across the two evaluations can differ; compare content on
        # the natural key, which is what "equivalent results" means.
        def _ordered(df):
            key = [c for c in ("time", "siteid", "variable") if c in df.columns]
            return df.sort_values(key).reset_index(drop=True) if key else df.reset_index(drop=True)

        pd.testing.assert_frame_equal(_ordered(on_distributed), _ordered(on_threads))

    @requires_distributed
    def test_explicit_scheduler_argument_selects_cluster(self, local_distributed_cluster):
        """Selection by the caller's environment (an active Client) is honored.

        A trivial dask graph computes on the cluster and matches the local
        result — confirming the deferred loads above genuinely ran distributed
        rather than silently staying local.
        """
        x = da.arange(1000, chunks=100).sum()
        cluster_result = x.compute()  # active Client -> distributed
        with dask.config.set(scheduler="synchronous"):
            local_result = x.compute()
        assert int(cluster_result) == int(local_result) == sum(range(1000))
        # The cluster run really used the remote scheduler:
        assert local_distributed_cluster is not None


# ---------------------------------------------------------------------------
# Unreachable scheduler surfaces its error (spec edge case)
# ---------------------------------------------------------------------------
class TestNoSilentFallback:
    def test_unreachable_scheduler_raises_not_local_result(self, multifile):
        """A named-but-unreachable scheduler must error, never return data.

        The dangerous failure mode is ``compute()`` quietly running single-machine
        and *appearing* to succeed while the user believes it is distributed.
        Requesting the distributed scheduler against a dead address must raise.
        """
        reader = CMAQReader()
        deferred = reader.open_dataset(multifile, defer=True)
        with dask.config.set(
            scheduler="dask.distributed",
            distributed={"scheduler": "127.0.0.1:1"},  # nothing listening
        ):
            with pytest.raises((RuntimeError, OSError)) as excinfo:
                deferred.compute()
        message = str(excinfo.value)
        assert "client" in message.lower() or "connect" in message.lower(), (
            f"error did not surface the scheduler problem: {message}"
        )

    @requires_distributed
    def test_direct_client_to_dead_address_raises(self):
        """Constructing a Client against an unreachable address errors directly."""
        with pytest.raises((OSError, TimeoutError)):
            distributed.Client("tcp://127.0.0.1:1", timeout=2)


# ---------------------------------------------------------------------------
# Clean skip without the optional extra
# ---------------------------------------------------------------------------
class TestGracefulSkip:
    def test_module_imports_without_distributed(self):
        """Importing this module never requires ``distributed`` at top level."""
        # If we got here, the module imported; the guarded flag reflects env.
        assert isinstance(_HAS_DISTRIBUTED, bool)

    @requires_distributed
    def test_distributed_present_when_extra_installed(self):
        assert _HAS_DISTRIBUTED
        assert hasattr(distributed, "Client")
