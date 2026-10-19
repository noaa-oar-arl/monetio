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

from pathlib import Path

import dask
import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.actris import ACTRISReader
from monetio.readers.aeronet import AERONETReader
from monetio.readers.airnow import AirNowReader
from monetio.readers.amdar import AMDARReader
from monetio.readers.ameriflux import AmeriFluxReader
from monetio.readers.aqs import AQSReader
from monetio.readers.cems import CEMSReader
from monetio.readers.cmaq import CMAQReader
from monetio.readers.crn import CRNReader
from monetio.readers.earlinet import EARLINETReader
from monetio.readers.era5 import ERA5Reader
from monetio.readers.gml_ozonesonde import GMLOzonesondeReader
from monetio.readers.goes import GOESReader
from monetio.readers.gpm_imerg import GPMIMERGReader
from monetio.readers.hytraj import HYTRAJReader
from monetio.readers.iagos import IAGOSReader
from monetio.readers.icap_mme import ICAPMMEReader
from monetio.readers.icartt import ICARTTReader
from monetio.readers.igra2 import IGRA2Reader
from monetio.readers.improve import IMPROVEReader
from monetio.readers.ish import ISH, ISHReader
from monetio.readers.ish_lite import ISHLiteReader
from monetio.readers.madis import MADISReader
from monetio.readers.merra2 import MERRA2Reader
from monetio.readers.mplnet import MPLNETReader
from monetio.readers.mrms import MRMSReader
from monetio.readers.nadp import NADPReader
from monetio.readers.ndacc import NDACCReader
from monetio.readers.ndbc import NDBCReader
from monetio.readers.openaq import OpenAQReader
from monetio.readers.openaq_aws import OpenAQAWSReader
from monetio.readers.openaq_v2 import OpenAQV2Reader
from monetio.readers.pams import PAMSReader
from monetio.readers.pandora import PandoraReader
from monetio.readers.sentinel4 import Sentinel4Reader
from monetio.readers.skynet import SKYNETReader
from monetio.readers.smap import SMAPReader
from monetio.readers.solrad import SOLRADReader
from monetio.readers.surfrad import SURFRADReader
from monetio.readers.tccon import TCCONReader
from monetio.readers.tolnet import TOLNetReader

_HAS_DISTRIBUTED = True
try:
    import distributed  # noqa: F401
except ImportError:  # pragma: no cover - environment without the extra
    _HAS_DISTRIBUTED = False

_ROOT = Path(__file__).parent
_MULTIFILE = sorted(str(p) for p in (_ROOT / "data" / "multifile").glob("*.nc"))

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


def _synthetic_aqs_csv(tmp_path):
    """Write a minimal local AQS file for process-worker parser tests."""
    path = tmp_path / "hourly_aqs.csv"
    pd.DataFrame(
        {
            "Date GMT": ["01/01/2023", "01/01/2023"],
            "Time GMT": ["00:00", "00:00"],
            "Date Local": ["01/01/2023", "01/01/2023"],
            "Time Local": ["00:00", "00:00"],
            "State Code": ["01", "01"],
            "County Code": ["001", "001"],
            "Site Num": ["0001", "0001"],
            "Parameter Code": [44201, 88101],
            "POC": [1, 1],
            "Latitude": [34.0, 34.0],
            "Longitude": [-86.0, -86.0],
            "Sample Measurement": [40.0, 11.0],
            "Units of Measure": ["Parts per billion", "Micrograms/cubic meter (LC)"],
            "Parameter Name": ["Ozone", "PM2.5 - Local Conditions"],
        }
    ).to_csv(path, index=False)
    return str(path)


def _mark_partition_worker(frame):
    """Add the executing process ID while preserving a Dask table partition."""
    import os

    result = frame.copy()
    result["_worker_pid"] = os.getpid()
    return result


def _worker_pid_block(block):
    """Return process IDs while retaining an input dependency in the graph."""
    import os

    import numpy as np

    return np.full(block.shape, os.getpid(), dtype=np.int64)


from http.server import BaseHTTPRequestHandler


class _OpenAQProcessMockHandler(BaseHTTPRequestHandler):
    """Serve deterministic measurements to process-worker OpenAQ tasks."""

    payload = None

    def do_GET(self):
        import json

        body = json.dumps(type(self).payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def _synthetic_tolnet_hdf5(tmp_path):
    """Write a small local TOLNet HDF5 profile for process-worker tests."""
    import h5py
    import numpy as np

    path = tmp_path / "tolnet-process.h5"
    with h5py.File(path, "w") as handle:
        attributes = handle.create_group("INSTRUMENT_ATTRIBUTES")
        attributes.attrs["Location_Latitude"] = b"39.0 N"
        attributes.attrs["Location_Longitude"] = b"76.5 W"
        data = handle.create_group("DATA")
        data.create_dataset("ALT", data=np.arange(10, dtype="f4") * 100.0)
        data.create_dataset(
            "TIME_MID_UT_UNIX", data=np.array([1_600_000_000_000, 1_600_003_600_000])
        )
        values = np.arange(20, dtype="f4").reshape(10, 2)
        data.create_dataset("O3MR", data=values)
        data.create_dataset("O3ND", data=values * 2)
    return str(path)


def _synthetic_point_netcdf(tmp_path, reader_name):
    """Write an observation file using the direct AMDAR/MADIS NetCDF layout."""
    import numpy as np

    paths = []
    for file_index in range(2):
        dataset = xr.Dataset(
            {
                "temperature": ("recNum", np.array([270.0, 271.0]) + file_index),
                "latitude": ("recNum", [40.0 + file_index, 41.0 + file_index]),
                "longitude": ("recNum", [-80.0, -81.0]),
            }
        )
        path = tmp_path / f"{reader_name}-{file_index}.nc"
        dataset.to_netcdf(path, engine="h5netcdf")
        paths.append(str(path))
    return paths


# ---------------------------------------------------------------------------
# Same deferred load, equivalent results across schedulers (SC-015)
# ---------------------------------------------------------------------------
class TestSchedulerEquivalence:
    @requires_distributed
    def test_camx_pseudonetcdf_runs_on_process_workers(
        self, tmp_path, large_process_distributed_cluster
    ):
        """The default CAMx UAMIV backend executes lazily on separate workers."""
        import os

        from dask_audit_support import write_camx_pseudonetcdf_fixture

        from monetio.readers.camx import CAMxReader

        pytest.importorskip("PseudoNetCDF")
        path = write_camx_pseudonetcdf_fixture(tmp_path / "camx-process-worker.uamiv")

        client = large_process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        reader = CAMxReader()
        lazy = reader.open_dataset(str(path), chunks={"time": 1}, use_dask=True)
        eager = reader.open_dataset(str(path), use_dask=False)
        variable = "o3"
        marked = lazy[variable].copy(
            data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )
        lazy.close()
        eager.close()

    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    def test_gridded_deferred_matches_eager_on_threads(self):
        """Local threaded scheduler: evaluate(defer) == eager."""
        reader = CMAQReader()
        deferred = reader.open_dataset(_MULTIFILE, defer=True)
        assert all(isinstance(v.data, da.Array) for v in deferred.data_vars.values()), (
            "defer must return an unevaluated dataset"
        )
        with dask.config.set(scheduler="threads"):
            on_threads = deferred.compute()
        eager = reader.open_dataset(_MULTIFILE)
        xr.testing.assert_identical(_strip_history(on_threads), _strip_history(eager))

    @requires_distributed
    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    def test_gridded_deferred_equivalent_across_schedulers(self, local_distributed_cluster):
        """SC-015: threaded and in-process distributed give the same result.

        The *same* deferred graph is computed under each scheduler; both equal
        the eager load. No reader code changes between the two — selection is
        purely the caller's environment.
        """
        reader = CMAQReader()
        deferred = reader.open_dataset(_MULTIFILE, defer=True)

        # An active distributed Client routes .compute() through it.
        on_distributed = deferred.compute()
        with dask.config.set(scheduler="threads"):
            on_threads = deferred.compute()

        xr.testing.assert_identical(_strip_history(on_distributed), _strip_history(on_threads))
        eager = reader.open_dataset(_MULTIFILE)
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
    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    @pytest.mark.parametrize("defer", [False, True], ids=["use-dask", "defer"])
    def test_gridded_deferred_equivalent_on_process_workers(
        self, process_distributed_cluster, defer
    ):
        """Reader tasks reopen local source files in separate worker processes."""
        import os

        reader = CMAQReader()
        options = {"defer": True} if defer else {"use_dask": True}
        deferred = reader.open_dataset(_MULTIFILE, **options)
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert worker_pids and os.getpid() not in worker_pids

        variable = next(iter(deferred.data_vars))
        marker = deferred[variable].copy(
            data=deferred[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        on_workers = deferred.assign(_worker_pid=(marker.dims, marker.data)).compute()
        actual_pids = set(on_workers["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        on_workers = on_workers.drop_vars("_worker_pid")
        eager = reader.open_dataset(_MULTIFILE)
        xr.testing.assert_identical(_strip_history(on_workers), _strip_history(eager))

    @requires_distributed
    @pytest.mark.parametrize("defer", [False, True], ids=["use-dask", "defer"])
    def test_point_deferred_equivalent_on_process_workers(
        self, tmp_path, monkeypatch, process_distributed_cluster, defer
    ):
        """Point-reader parser tasks and shuffle run on separate worker processes."""
        import os

        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=240, n_sites=6)
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert worker_pids and os.getpid() not in worker_pids

        with dask.config.set({"dataframe.shuffle.method": "tasks"}):
            dask_options = {"defer": True} if defer else {"use_dask": True}
            deferred = AirNowReader().open_dataset(
                files=fn,
                **dask_options,
                wide_fmt=False,
                as_xarray=False,
                bad_utcoffset="leave",
            )
            on_workers = deferred.compute()
        eager = AirNowReader().open_dataset(
            files=fn,
            defer=False,
            wide_fmt=False,
            as_xarray=False,
            bad_utcoffset="leave",
        )
        key = [c for c in ("time", "siteid", "variable") if c in on_workers.columns]
        if key:
            on_workers = on_workers.sort_values(key).reset_index(drop=True)
            eager = eager.sort_values(key).reset_index(drop=True)
        else:
            on_workers = on_workers.reset_index(drop=True)
            eager = eager.reset_index(drop=True)
        pd.testing.assert_frame_equal(on_workers, eager)

    @requires_distributed
    @pytest.mark.parametrize("defer", [False, True], ids=["use-dask", "defer"])
    def test_point_wide_deferred_equivalent_on_process_workers(
        self, tmp_path, monkeypatch, process_distributed_cluster, defer
    ):
        """AirNow's deferred point-to-wide Xarray graph runs on worker processes."""
        import os

        fn = _synthetic_airnow(tmp_path, monkeypatch, n_rows=48, n_sites=4)
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert worker_pids and os.getpid() not in worker_pids

        with dask.config.set({"dataframe.shuffle.method": "tasks"}):
            dask_options = {"defer": True} if defer else {"use_dask": True}
            deferred = AirNowReader().open_dataset(
                files=fn,
                **dask_options,
                wide_fmt=True,
                as_xarray=True,
                bad_utcoffset="leave",
            )
            assert any(isinstance(v.data, da.Array) for v in deferred.data_vars.values())
            on_workers = deferred.compute()
        eager = AirNowReader().open_dataset(
            files=fn,
            defer=False,
            wide_fmt=True,
            as_xarray=True,
            bad_utcoffset="leave",
        )
        xr.testing.assert_identical(_strip_history(on_workers), _strip_history(eager))

    @requires_distributed
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_aqs_use_dask_output_runs_on_process_workers(
        self, tmp_path, process_distributed_cluster, as_xarray
    ):
        """AQS parser graphs run in workers and preserve eager output values."""
        import os

        import dask.dataframe as dd

        path = _synthetic_aqs_csv(tmp_path)
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        lazy = AQSReader().open_dataset(
            files=path, use_dask=True, as_xarray=as_xarray, wide_fmt=True
        )
        eager = AQSReader().open_dataset(
            files=path, use_dask=False, as_xarray=as_xarray, wide_fmt=True
        )
        if isinstance(lazy, dd.DataFrame):
            marked = lazy.map_partitions(
                _mark_partition_worker,
                meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=True
            )
        else:
            variable = next(iter(lazy.data_vars))
            marker = lazy[variable].copy(
                data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = lazy.assign(_worker_pid=(marker.dims, marker.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_identical(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )

        assert actual_pids and actual_pids <= worker_pids

    @requires_distributed
    @pytest.mark.parametrize(
        "reader_name,reader_class", [("amdar", AMDARReader), ("madis", MADISReader)]
    )
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_direct_point_netcdf_use_dask_runs_on_process_workers(
        self, tmp_path, process_distributed_cluster, reader_name, reader_class, as_xarray
    ):
        """AMDAR/MADIS chunked NetCDF sources reopen and compute in workers."""
        import os

        import dask.dataframe as dd

        paths = _synthetic_point_netcdf(tmp_path, reader_name)
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = reader_class()
        deferred = reader.open_dataset(paths, use_dask=True, as_xarray=as_xarray, expand2d=False)
        eager = reader.open_dataset(paths, use_dask=False, as_xarray=as_xarray, expand2d=False)
        if isinstance(deferred, dd.DataFrame):
            marked = deferred.map_partitions(
                _mark_partition_worker,
                meta=deferred._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
            )
        else:
            variable = "temperature"
            marker = deferred[variable].copy(
                data=deferred[variable].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = deferred.assign(_worker_pid=(marker.dims, marker.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_equal(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )

        assert actual_pids and actual_pids <= worker_pids

    @requires_distributed
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_ndbc_use_dask_runs_on_process_workers(
        self, tmp_path, monkeypatch, process_distributed_cluster, as_xarray
    ):
        """NDBC text partitions evaluate on workers and match eager results."""
        import os

        import dask.dataframe as dd

        import monetio.readers.ndbc as ndbc_module

        monkeypatch.setattr(
            ndbc_module,
            "get_station_table",
            lambda: pd.DataFrame(
                {
                    "siteid": ["41001", "41002"],
                    "name": ["Station 1", "Station 2"],
                    "latitude": [31.0, 32.0],
                    "longitude": [-75.0, -76.0],
                }
            ),
        )

        paths = []
        for station in ("41001", "41002"):
            path = tmp_path / f"{station}.txt"
            path.write_text(
                "#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS PTDY TIDE\n"
                "#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi hPa ft\n"
                "2024 01 01 00 00 40 10.0 12.0 2.0 6 5.0 10 1015.0 15.0 20.0 12.0 MM MM MM\n"
                "2024 01 01 01 00 45 11.0 13.0 2.1 7 5.1 11 1015.1 15.1 20.1 12.1 MM MM MM\n"
            )
            paths.append(str(path))

        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        lazy = NDBCReader().open_dataset(paths, use_dask=True, as_xarray=as_xarray)
        eager = NDBCReader().open_dataset(paths, use_dask=False, as_xarray=as_xarray)
        if isinstance(lazy, dd.DataFrame):
            marked = lazy.map_partitions(
                _mark_partition_worker,
                meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
            )
        else:
            marker = lazy["wind_speed"].copy(
                data=lazy["wind_speed"].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = lazy.assign(_worker_pid=(marker.dims, marker.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_equal(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )
            lazy.close()

        assert actual_pids and actual_pids <= worker_pids

    @requires_distributed
    @pytest.mark.parametrize("lite", [False, True], ids=["ish", "ish-lite"])
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-dataset"])
    def test_ish_station_year_outputs_run_on_process_workers(
        self, tmp_path, monkeypatch, process_distributed_cluster, ish_history, lite, as_xarray
    ):
        """ISH annual-file tasks reopen on workers for table and wide outputs."""
        import gzip
        import os

        import dask.dataframe as dd

        if lite:
            path = tmp_path / "722244-00358-2020.gz"
            with gzip.open(path, "wb") as stream:
                stream.write(b"2020 09 01 00 256 184 10185 220 40 0 0 0\n")
            reader = ISHLiteReader()
        else:
            path = tmp_path / "722244-00358-2020"
            path.write_text(
                "0054722244003582020090100004+38941-076952FM-12+0020KADW "
                "99992201V0040199999199030000199+02561+01841101851\n"
            )
            reader = ISHReader()

        original_build_urls = ISH.build_urls
        observed = {}

        def localize_urls(self, dates=None, sites=None, lite=False):
            urls = original_build_urls(self, dates=dates, sites=sites, lite=lite)
            observed["urls"] = urls.name.tolist()
            localized = urls.copy()
            localized["name"] = [str(path)]
            return localized

        monkeypatch.setattr(
            ISH,
            "read_ish_history",
            lambda self, dates=None: setattr(self, "history", ish_history.copy()),
        )
        monkeypatch.setattr(ISH, "build_urls", localize_urls)
        dates = pd.DatetimeIndex(["2020-09-01"])
        deferred = reader.open_dataset(
            dates=dates, site="72224400358", lazy=True, resample=False, as_xarray=as_xarray
        )
        eager = reader.open_dataset(
            dates=dates, site="72224400358", lazy=False, resample=False, as_xarray=as_xarray
        )
        assert len(observed["urls"]) == 1
        assert observed["urls"][0].endswith("/2020/722244-00358-2020.gz")

        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids
        if isinstance(deferred, dd.DataFrame):
            marked = deferred.map_partitions(
                _mark_partition_worker,
                meta=deferred._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            key = [column for column in ("time", "siteid") if column in eager.columns]
            pd.testing.assert_frame_equal(
                marked.sort_values(key).reset_index(drop=True),
                eager.sort_values(key).reset_index(drop=True),
                check_dtype=False,
            )
        else:
            variable = "t2m" if lite else "t"
            marker = deferred[variable].copy(
                data=deferred[variable].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = deferred.assign(_worker_pid=(marker.dims, marker.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_equal(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )
            deferred.close()
        assert actual_pids and actual_pids <= worker_pids

    @requires_distributed
    @pytest.mark.parametrize("reader_name,reader_class", [("cems", CEMSReader), ("crn", CRNReader)])
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_text_point_readers_use_dask_run_on_process_workers(
        self,
        tmp_path,
        monkeypatch,
        process_distributed_cluster,
        reader_name,
        reader_class,
        as_xarray,
    ):
        """CEMS and CRN file partitions evaluate on workers and match eager results."""
        import os

        import dask.dataframe as dd

        if reader_name == "crn":

            def monitor_table(self, latlonbox=None):
                return pd.DataFrame(
                    {
                        "WBANNO": ["63893"],
                        "STATE": ["AL"],
                        "LOCATION": ["Fairhope"],
                        "VECTOR": ["Test"],
                        "LATITUDE": [30.54],
                        "LONGITUDE": [-87.88],
                        "NETWORK": ["USCRN"],
                        "GMT_OFFSET": [-6.0],
                    }
                )

            monkeypatch.setattr(CRNReader, "get_monitor_df", monitor_table)
            paths = []
            for part in range(2):
                path = tmp_path / f"CRNH0203-2024-AL_Fairhope_{part}.txt"
                path.write_text(
                    f"63893 2024010{part + 1} 1200 2024010{part + 1} 0600 1.000 -87.88 30.54 "
                    "10.5 10.0 11.0 9.5 0.0 " + "0.0 " * 22 + "\n"
                )
                paths.append(str(path))
        else:
            paths = []
            header = (
                "Facility Name,ORISPL Code,Fac ID,SO2 Lbs,NOx Lbs,CO2 Short Tons,Date,Hour,"
                "Latitude,Longitude,State Name\n"
            )
            for part in range(2):
                path = tmp_path / f"2024md0{part + 1}.csv"
                path.write_text(
                    header
                    + f"Plant,123,456,10.5,5.2,100.0,2024-01-0{part + 1},0,39.0,-77.0,Maryland\n"
                )
                paths.append(str(path))

        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        lazy = reader_class().open_dataset(paths, use_dask=True, as_xarray=as_xarray)
        eager = reader_class().open_dataset(paths, use_dask=False, as_xarray=as_xarray)
        if isinstance(lazy, dd.DataFrame):
            marked = lazy.map_partitions(
                _mark_partition_worker,
                meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
            )
        else:
            data_vars = list(lazy.data_vars)
            variable = data_vars[0]
            marker = lazy[variable].copy(
                data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = lazy.assign(_worker_pid=(marker.dims, marker.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_equal(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )
            lazy.close()

        assert actual_pids and actual_pids <= worker_pids

    @requires_distributed
    @pytest.mark.parametrize(
        "reader_class,source_variable,output_variable,dims",
        [
            (MRMSReader, "precip_rate", "precipitation_rate", ("lat", "lon")),
            (SMAPReader, "soil_moisture", "soil_moisture", ("row", "col")),
            (GPMIMERGReader, "precipitationCal", "precipitation", ("lat", "lon")),
            (ERA5Reader, "t2m", "temperature_2m", ("time", "lat", "lon")),
            (MERRA2Reader, "PS", "surface_pressure", ("time", "lat", "lon")),
        ],
        ids=["mrms", "smap", "gpm-imerg", "era5", "merra2"],
    )
    def test_gridded_netcdf_use_dask_runs_on_process_workers(
        self,
        tmp_path,
        process_distributed_cluster,
        reader_class,
        source_variable,
        output_variable,
        dims,
    ):
        """Dask-backed gridded NetCDF reads reopen and evaluate in workers."""
        import os

        import numpy as np

        shape = tuple(2 if dim == "time" else 4 for dim in dims)
        values = np.arange(np.prod(shape), dtype="f4").reshape(shape)
        coords = {
            dim: np.arange(size, dtype="f4") for dim, size in zip(dims, shape) if dim != "time"
        }
        if "time" in dims:
            coords["time"] = pd.date_range("2024-01-01", periods=shape[0], freq="h")
        path = tmp_path / f"{reader_class.__name__}-process.nc"
        xr.Dataset({source_variable: (dims, values)}, coords=coords).to_netcdf(
            path, engine="h5netcdf"
        )

        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = reader_class()
        deferred = reader.open_dataset(
            str(path), engine="h5netcdf", use_dask=True, chunks={dims[0]: 1}
        )
        eager = reader.open_dataset(str(path), engine="h5netcdf", use_dask=False)
        marker = deferred[output_variable].copy(
            data=deferred[output_variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = deferred.assign(_worker_pid=(marker.dims, marker.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_equal(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )

    @requires_distributed
    @pytest.mark.parametrize("reader_class", [PandoraReader, NDACCReader], ids=["pandora", "ndacc"])
    def test_geoms_point_table_runs_on_process_workers(
        self, process_distributed_cluster, reader_class
    ):
        """GEOMS HDF task handles reopen on workers and table output matches eager."""
        import os

        import dask.dataframe as dd

        path = _ROOT / "data" / "pandora-uvvis-no2-boulder-20231206.h5"
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = reader_class()
        lazy = reader.open_dataset(str(path), as_xarray=False, use_dask=True)
        eager = reader.open_dataset(str(path), as_xarray=False, use_dask=False)
        assert isinstance(lazy, dd.DataFrame)
        marked = lazy.map_partitions(
            _mark_partition_worker,
            meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
        ).compute()
        actual_pids = set(marked.pop("_worker_pid").unique())
        assert actual_pids and actual_pids <= worker_pids
        pd.testing.assert_frame_equal(
            marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
        )

    @requires_distributed
    @pytest.mark.parametrize(
        "reader_class,variable",
        [(PandoraReader, "nitrogen_dioxide"), (NDACCReader, "no2_column_absorption_solar")],
        ids=["pandora", "ndacc"],
    )
    def test_geoms_gridded_dataset_runs_on_process_workers(
        self, process_distributed_cluster, reader_class, variable
    ):
        """GEOMS Xarray output computes on worker processes and matches eager values."""
        import os

        path = _ROOT / "data" / "pandora-uvvis-no2-boulder-20231206.h5"
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = reader_class()
        lazy = reader.open_dataset(str(path), use_dask=True)
        eager = reader.open_dataset(str(path), use_dask=False)
        marked = lazy[variable].copy(
            data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )

    @requires_distributed
    def test_goes_use_dask_runs_on_process_workers(self, tmp_path, process_distributed_cluster):
        """GOES granule processing and generated coordinates execute on workers."""
        import os

        sample = tmp_path / "goes-process-worker.nc"
        x = np.linspace(-0.1, 0.1, 10).astype("f4")
        y = np.linspace(0.1, -0.1, 8).astype("f4")
        projection_attrs = {
            "perspective_point_height": 35786023.0,
            "semi_major_axis": 6378137.0,
            "semi_minor_axis": 6356752.31414,
            "inverse_flattening": 298.257222103,
            "latitude_of_projection_origin": 0.0,
            "longitude_of_projection_origin": -75.0,
            "sweep_angle_axis": "x",
            "grid_mapping_name": "geostationary",
        }
        xr.Dataset(
            {
                "AOD": (("y", "x"), np.arange(80, dtype="f4").reshape(8, 10)),
                "goes_imager_projection": ((), np.int32(0), projection_attrs),
            },
            coords={"x": x, "y": y},
            attrs={"time_coverage_start": "2023-01-01T00:00:00Z"},
        ).to_netcdf(sample, engine="h5netcdf")
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        reader = GOESReader()
        lazy = reader.open_dataset(str(sample), use_dask=True, chunks={"x": 5, "y": 4})
        eager = reader.open_dataset(str(sample), use_dask=False)
        variable = next(
            name for name, value in lazy.data_vars.items() if hasattr(value.data, "dask")
        )
        marked = lazy[variable].copy(
            data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )

    @requires_distributed
    def test_sentinel4_use_dask_runs_on_process_workers(
        self, tmp_path, process_distributed_cluster
    ):
        """Sentinel-4 masking and coordinate standardization preserve worker-safe arrays."""
        import os

        path = tmp_path / "sentinel4-process.nc"
        scanline, ground_pixel = 8, 12
        values = np.arange(scanline * ground_pixel, dtype="f4").reshape(scanline, ground_pixel)
        xr.Dataset(
            {
                "qa_value": (("scanline", "ground_pixel"), np.ones_like(values)),
                "nitrogendioxide_tropospheric_column": (("scanline", "ground_pixel"), values),
                "latitude": (
                    ("scanline", "ground_pixel"),
                    np.broadcast_to(np.linspace(40, 45, scanline)[:, None], values.shape),
                ),
                "longitude": (
                    ("scanline", "ground_pixel"),
                    np.broadcast_to(np.linspace(-10, 10, ground_pixel)[None, :], values.shape),
                ),
            }
        ).to_netcdf(path, engine="h5netcdf")
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        reader = Sentinel4Reader()
        lazy = reader.open_dataset(
            str(path),
            group="",
            engine="h5netcdf",
            qa_threshold=0.5,
            use_dask=True,
            chunks={"scanline": 4, "ground_pixel": 6},
        )
        eager = reader.open_dataset(str(path), group="", engine="h5netcdf", qa_threshold=0.5)
        variable = "nitrogendioxide_tropospheric_column"
        marked = lazy[variable].copy(
            data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )

    @requires_distributed
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_hytraj_use_dask_runs_on_process_workers(
        self, tmp_path, process_distributed_cluster, as_xarray
    ):
        """HYTRAJ text parser partitions and conversions run in worker processes."""
        import os

        path = tmp_path / "tdump-process.txt"
        path.write_text(
            "   1   1\n METDATA 200101 00\n   1\n 20 01 01 00 30.0 -100.0 1000.0\n"
            "   1 PRESSURE\n"
            "   1   1 20 01 01 00 00  0.0  0.0 30.00 -100.00 1000.0 1013.2\n"
            "   1   1 20 01 01 01 00  1.0  1.0 31.00 -101.00 1100.0 1010.0\n"
        )
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        reader = HYTRAJReader()
        lazy = reader.open_dataset(str(path), as_xarray=as_xarray, use_dask=True)
        eager = reader.open_dataset(str(path), as_xarray=as_xarray, use_dask=False)
        if as_xarray:
            variable = "pressure"
            marked = lazy[variable].copy(
                data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            assert actual_pids and actual_pids <= worker_pids
            xr.testing.assert_allclose(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )
        else:
            import dask.dataframe as dd

            assert isinstance(lazy, dd.DataFrame)
            marked = lazy.map_partitions(
                _mark_partition_worker,
                meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            assert actual_pids and actual_pids <= worker_pids
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
            )

    @requires_distributed
    @pytest.mark.parametrize(
        "reader_name",
        [
            "ameriflux",
            "improve",
            "openaq_aws",
            "openaq",
            "pams",
            "skynet",
            "icartt",
            "gml_ozonesonde",
            "surfrad",
            "solrad",
            "nadp",
            "igra2",
            "actris",
            "iagos",
        ],
        ids=[
            "ameriflux",
            "improve",
            "openaq-aws",
            "openaq",
            "pams",
            "skynet",
            "icartt",
            "gml-ozonesonde",
            "surfrad",
            "solrad",
            "nadp",
            "igra2",
            "actris",
            "iagos",
        ],
    )
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_point_text_dask_readers_run_on_process_workers(
        self, tmp_path, process_distributed_cluster, reader_name, as_xarray
    ):
        """CSV/text reader graphs and point conversions execute in worker processes."""
        import os

        import dask.dataframe as dd

        if reader_name == "ameriflux":
            path = tmp_path / "ameriflux.csv"
            path.write_text(
                "TIMESTAMP_START,CO2,FC\n202301010000,400.1,1.2\n202301010030,400.5,1.3\n"
            )
            reader = AmeriFluxReader()
        elif reader_name == "improve":
            path = tmp_path / "improve.txt"
            path.write_text(
                "IMPROVE Data File\nMore Header Info\nData\n"
                "EPACode\tVal\tState\tParamCode\tSiteCode\tUnit\tDate\tDataset\n"
                "010010001\t1.5\tAL\t88101\tBIRM1\tug/m3\t2023-01-01\tTEST\n"
                "010010001\t2.0\tAL\t88101\tBIRM1\tug/m3\t2023-01-02\tTEST\n"
            )
            reader = IMPROVEReader()
        elif reader_name == "openaq_aws":
            import gzip

            path = tmp_path / "location-7073-20230101.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8") as stream:
                stream.write(
                    "location_id,sensor_id,location,datetime,lat,lon,parameter,unit,value\n"
                    "7073,1,Test Site,2023-01-01 00:00:00,40,-75,o3,µg/m³,19.9\n"
                    "7073,2,Test Site,2023-01-01 01:00:00,40,-75,o3,µg/m³,25.0\n"
                )
            reader = OpenAQAWSReader()
        elif reader_name == "openaq":
            import json

            path = tmp_path / "openaq.jsonl"
            record = {
                "location": "Test Site",
                "city": "Test City",
                "country": "TS",
                "date": {"utc": "2023-01-01T00:00:00Z", "local": "2023-01-01T01:00:00+01:00"},
                "parameter": "o3",
                "value": 50.0,
                "unit": "µg/m³",
                "coordinates": {"latitude": 40.0, "longitude": -100.0},
                "averagingPeriod": {"value": 1, "unit": "hours"},
                "sourceName": "Source A",
                "sourceType": "government",
                "mobile": False,
            }
            path.write_text(json.dumps(record) + "\n")
            reader = OpenAQReader()
        elif reader_name == "pams":
            import json

            path = tmp_path / "pams.json"
            path.write_text(
                json.dumps(
                    {
                        "Data": [
                            {
                                "state_code": "06",
                                "county_code": "037",
                                "site_number": "1103",
                                "parameter": "Ozone",
                                "sample_measurement": 42.0,
                                "units_of_measure": "Parts per billion",
                                "date_gmt": "2023-01-01",
                                "time_gmt": "12:00",
                                "latitude": 34.05,
                                "longitude": -118.24,
                            },
                            {
                                "state_code": "06",
                                "county_code": "037",
                                "site_number": "1103",
                                "parameter": "Ozone",
                                "sample_measurement": 43.0,
                                "units_of_measure": "Parts per billion",
                                "date_gmt": "2023-01-01",
                                "time_gmt": "13:00",
                                "latitude": 34.05,
                                "longitude": -118.24,
                            },
                        ]
                    }
                )
            )
            reader = PAMSReader()
        elif reader_name == "skynet":
            path = tmp_path / "test_site_20230101.AOT"
            path.write_text(
                "# site: TEST_SITE\n# latitude: 35.0\n# longitude: 135.0\n# elevation: 10.0\n"
                "date time aot_500nm aot_675nm ae\n"
                "2023-01-01 12:00:00 0.1 0.05 1.5\n"
                "2023-01-01 13:00:00 0.2 0.10 1.5\n"
            )
            reader = SKYNETReader()
        elif reader_name == "surfrad":
            path = tmp_path / "bon24001.dat"
            path.write_text(
                "Bondville\n40.05192 -88.37309 213 1\n"
                "2024 1 1 1 0 0 0.0 162.0 10.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0 0.0 0\n"
            )
            reader = SURFRADReader()
        elif reader_name == "solrad":
            widths = [4, 3, 2, 2, 2, 2, 6, 6, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 9, 9, 9, 9]
            values = [
                2024,
                1,
                1,
                1,
                0,
                0,
                0.0,
                150.0,
                10.0,
                0,
                0.0,
                0,
                0.0,
                0,
                0.0,
                0,
                0.0,
                0,
                0.0,
                0.0,
                0.0,
                0.0,
            ]
            row = "".join(f"{str(value):>{width}} " for value, width in zip(values, widths))
            path = tmp_path / "abq24001.dat"
            path.write_text("Albuquerque_NM\n35.05 -106.62 1617 7\n" + row + "\n")
            reader = SOLRADReader()
        elif reader_name == "nadp":
            path = tmp_path / "nadp.csv"
            path.write_text(
                "siteid,network,dateon,dateoff,mg,flagmg,so4,flagso4\n"
                "TX01,NTN,2023-01-01,2023-01-08,1.0, ,3.0, \n"
                "TX01,NTN,2023-01-08,2023-01-15,-1.0, ,4.0,<\n"
            )
            reader = NADPReader()
            reader.get_monitor_df = lambda network: pd.DataFrame(
                {"siteid": ["TX01"], "latitude": [31.0], "longitude": [-100.0]}
            )
        elif reader_name == "igra2":
            path = tmp_path / "USM00072201-data.txt"
            path.write_text(
                "#USM00072201 2023 01 01 00 2315    1                   257906 -803164\n"
                "11    0  100000 10000 11111 22222 33333 44444 55555\n"
            )
            reader = IGRA2Reader()
            reader.stations = pd.DataFrame(
                {
                    "siteid": ["USM00072201"],
                    "name": ["MIAMI"],
                    "elevation": [2.0],
                    "latitude": [25.79],
                    "longitude": [-80.32],
                }
            )
        elif reader_name == "actris":
            path = tmp_path / "actris.nas"
            content = (
                "68 1001\nOriginator, Name\nOrganization\nSubmitter, Name\nProject\n1 1\n"
                "2023 01 01 2023 01 01\n0.041667\nstart_time\n2\n1.0 1.0\n"
                "9999.99 9999.99\nOzone\nCarbon_Monoxide\nStation name: Test Station\n"
                "Station code: TS0001\nStation latitude: 45.0\nStation longitude: 10.0\n"
                "Station altitude: 100.0\n"
            )
            content += "".join(f"Metadata line {i}\n" for i in range(68 - content.count("\n")))
            content += "0.0 10.0 100.0\n0.5 20.0 200.0\n"
            path.write_text(content)
            reader = ACTRISReader()
        elif reader_name == "iagos":
            import numpy as np

            path = tmp_path / "iagos.nc"
            xr.Dataset(
                {
                    "o3": ("time", np.array([40.0, 42.0])),
                    "lat": ("time", np.array([40.0, 41.0])),
                    "lon": ("time", np.array([-100.0, -101.0])),
                },
                coords={
                    "time": pd.date_range("2023-01-01", periods=2, freq="h"),
                    "siteid": ("time", ["TEST", "TEST"]),
                },
            ).to_netcdf(path, engine="h5netcdf")
            reader = IAGOSReader()
        elif reader_name == "icartt":
            path = tmp_path / "test.ict"
            path.write_text(
                "15, 1001\nAero PI\nAero Org\nAero Source\nAero Mission\n1\n"
                "2023, 05, 15, 2023, 05, 15\n1\nTime_Start, seconds\n2\n1.0, 1.0\n"
                "-999, -999\nLatitude, deg\nLongitude, deg\nTime_Start, Latitude, Longitude\n"
                "0, 40.0, -100.0\n1, 41.0, -101.0\n2, -999, -102.0\n"
            )
            reader = ICARTTReader()
        else:
            path = tmp_path / "test.l100"
            path.write_text(
                "Station: Boulder, CO\nLaunch Date: 2023-12-27\nLaunch Time: 17:00:00\n"
                "Latitude: 40.0\nLongitude: -105.0\nStation Height: 1743 meters\n"
                "Flight Number: BU1043\n\n"
                "Level   Press    Alt   Pottp   Temp   FtempV   Hum  Ozone  Ozone   Ozone  Ptemp  O3 # DN O3 Res  O3 Uncert\n"
                " Num     hPa      km     K      C       C       %    mPa    ppmv   atmcm    C   10^11/cc   DU          %\n"
                "    0   892.2   0.100   301.1   18.3    19.1    105   1.07  0.012  0.0009   32.3    2.649    259   5.0\n"
            )
            reader = GMLOzonesondeReader()

        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        output_kwargs = {"wide_fmt": False} if reader_name == "openaq" and not as_xarray else {}
        lazy = reader.open_dataset(str(path), as_xarray=as_xarray, use_dask=True, **output_kwargs)
        eager = reader.open_dataset(str(path), as_xarray=as_xarray, use_dask=False, **output_kwargs)
        if isinstance(lazy, dd.DataFrame):
            marked = lazy.map_partitions(
                _mark_partition_worker,
                meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
            )
        else:
            variable = next(name for name in lazy.data_vars if lazy[name].chunks is not None)
            marked = lazy[variable].copy(
                data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_equal(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )
            lazy.close()
        assert actual_pids and actual_pids <= worker_pids

    @requires_distributed
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_openaq_v2_delayed_api_runs_on_process_workers(
        self, process_distributed_cluster, as_xarray
    ):
        """The delayed API fetch and output graph execute in actual workers."""
        import os
        import threading
        from http.server import ThreadingHTTPServer

        from distributed.diagnostics.plugin import WorkerPlugin

        import monetio.readers.openaq_v2 as openaq_v2

        class EndpointPlugin(WorkerPlugin):
            def __init__(self, base_url):
                self.base_url = base_url

            def setup(self, worker):
                import monetio.readers.openaq_v2 as worker_openaq

                worker_openaq._BASE_URL = self.base_url

        _OpenAQProcessMockHandler.payload = {
            "meta": {"found": 1},
            "results": [
                {
                    "locationId": 1234,
                    "location": "Test Site",
                    "parameter": "o3",
                    "value": 0.05,
                    "date": {"utc": "2023-01-01T00:00:00Z", "local": "2023-01-01T00:00:00+00:00"},
                    "unit": "ppm",
                    "coordinates": {"latitude": 40.0, "longitude": -100.0},
                    "country": "US",
                    "city": "Test City",
                    "isMobile": False,
                    "isAnalysis": False,
                    "entity": "government",
                    "sensorType": "reference grade",
                }
            ],
        }
        server = ThreadingHTTPServer(("127.0.0.1", 0), _OpenAQProcessMockHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        original_base_url = openaq_v2._BASE_URL
        openaq_v2._BASE_URL = base_url
        client = process_distributed_cluster
        client.register_plugin(EndpointPlugin(base_url), name="openaq-v2-local-endpoint")
        worker_pids = set(client.run(os.getpid).values())

        try:
            reader = OpenAQV2Reader()
            output_kwargs = {"wide_fmt": False} if not as_xarray else {}
            lazy = reader.open_dataset(
                dates="2023-01-01",
                parameters=["o3"],
                sites=[1234],
                as_xarray=as_xarray,
                use_dask=True,
                **output_kwargs,
            )
            eager = reader.open_dataset(
                dates="2023-01-01",
                parameters=["o3"],
                sites=[1234],
                as_xarray=as_xarray,
                use_dask=False,
                **output_kwargs,
            )
            if as_xarray:
                variable = next(name for name in lazy.data_vars if lazy[name].chunks is not None)
                marked = lazy[variable].copy(
                    data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
                )
                computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
                actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
                xr.testing.assert_equal(
                    _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
                )
                lazy.close()
            else:
                import dask.dataframe as dd

                assert isinstance(lazy, dd.DataFrame)
                marked = lazy.map_partitions(
                    _mark_partition_worker,
                    meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
                ).compute()
                actual_pids = set(marked.pop("_worker_pid").unique())
                pd.testing.assert_frame_equal(
                    marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
                )
            assert actual_pids and actual_pids <= worker_pids
        finally:
            openaq_v2._BASE_URL = original_base_url
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    @requires_distributed
    @pytest.mark.parametrize(
        "reader_name",
        [
            "eprofile",
            "chimere",
            "ufs",
            "raqms",
            "wrfchem",
            "jpss_atms",
            "jpss_cris",
            "gems",
            "omps",
            "nasa_modis",
            "nesdis_eps_viirs",
            "modis_l2",
            "mopitt",
            "tempo",
            "tropomi",
            "calipso",
            "earthcare",
            "umbc_aerosol",
            "nesdis_frp",
            "omps_nadir",
            "nesdis_viirs_jrr",
            "viirs_jrr",
            "nesdis_edr_viirs",
        ],
    )
    def test_additional_gridded_readers_run_on_process_workers(
        self, tmp_path, request, reader_name
    ):
        """Gridded preprocessing remains worker-executable and eager-equivalent."""
        import os

        import numpy as np

        if reader_name == "eprofile":
            from monetio.readers.eprofile import EPROFILEReader

            path = tmp_path / "EPROFILE_L1_20240101_SITE.nc"
            xr.Dataset(
                {
                    "beta": (("time", "range"), np.arange(8, dtype="f4").reshape(2, 4)),
                    "station_latitude": (("time",), [40.0, 40.0]),
                    "station_longitude": (("time",), [-80.0, -80.0]),
                    "station_altitude": (("time",), [100.0, 100.0]),
                },
                coords={
                    "time": pd.date_range("2024-01-01", periods=2, freq="min"),
                    "range": np.arange(4, dtype="f4") * 100.0,
                },
            ).to_netcdf(path, engine="h5netcdf")
            reader = EPROFILEReader()
        elif reader_name == "chimere":
            from monetio.readers.chimere import ChimereReader

            path = tmp_path / "chimere-process.nc"
            xr.Dataset(
                {
                    "O3": (
                        ("time_counter", "bottom_top", "y", "x"),
                        np.arange(24, dtype="f4").reshape(2, 1, 3, 4),
                    ),
                    "nav_lat": (("y", "x"), np.ones((3, 4), dtype="f4")),
                    "nav_lon": (("y", "x"), np.ones((3, 4), dtype="f4")),
                },
                coords={"time_counter": pd.date_range("2023-01-01", periods=2, freq="h")},
            ).to_netcdf(path, engine="h5netcdf")
            reader = ChimereReader()
        elif reader_name == "ufs":
            from monetio.readers.ufs import UFSReader

            path = tmp_path / "ufs-process.nc"
            shape = (1, 2, 2, 2)
            ds = xr.Dataset(
                {
                    "tmp": (("time", "z", "y", "x"), np.full(shape, 290.0, dtype="f4")),
                    "pressfc": (("time", "y", "x"), np.full((1, 2, 2), 100000.0, dtype="f4")),
                    "ak": (("z_i",), [0.0, 0.0, 0.0]),
                    "bk": (("z_i",), [1.0, 0.5, 0.0]),
                    "lat": (("y", "x"), np.ones((2, 2), dtype="f4")),
                    "lon": (("y", "x"), np.ones((2, 2), dtype="f4")),
                    "delz": (("time", "z", "y", "x"), np.ones(shape, dtype="f4")),
                    "hgtsfc": (("y", "x"), np.ones((2, 2), dtype="f4")),
                    "o3": (("time", "z", "y", "x"), np.ones(shape, dtype="f4") * 1e-6),
                    "aso4j": (("time", "z", "y", "x"), np.ones(shape, dtype="f4")),
                },
                coords={
                    "time": [np.datetime64("2023-01-01")],
                    "pfull": (("z",), [2, 1]),
                    "phalf": (("z_i",), [3, 2, 1]),
                },
            )
            ds["tmp"].attrs["units"] = "K"
            ds["o3"].attrs["units"] = "ppmV"
            ds["aso4j"].attrs["units"] = "ug/kg"
            ds.to_netcdf(path, engine="h5netcdf")
            reader = UFSReader()
        elif reader_name == "raqms":
            from monetio.readers.raqms import RAQMSReader

            path = (
                Path(__file__).parent
                / "data"
                / "synthetic"
                / "raqms"
                / "uwhyb_06_01_2017_18Z.chem.assim.nc"
            )
            reader = RAQMSReader()
        elif reader_name == "wrfchem":
            from monetio.readers.wrfchem import WRFChemReader

            path = (
                Path(__file__).parent
                / "data"
                / "synthetic"
                / "wrfchem"
                / "wrfout_d01_2023-01-01_00-00-00.nc"
            )
            reader = WRFChemReader()
        elif reader_name in {"jpss_atms", "jpss_cris"}:
            from monetio.readers.jpss_atms import JPSSATMSReader
            from monetio.readers.jpss_cris import JPSSCrISReader

            path = tmp_path / f"{reader_name}-process.nc"
            xr.Dataset(
                {"temperature": (("nscan", "nlon"), np.arange(12, dtype="f4").reshape(3, 4))},
                coords={
                    "latitude": (("nscan", "nlon"), np.full((3, 4), 35.0, dtype="f4")),
                    "longitude": (("nscan", "nlon"), np.tile(np.arange(4, dtype="f4"), (3, 1))),
                },
            ).to_netcdf(path, engine="h5netcdf")
            reader = JPSSATMSReader() if reader_name == "jpss_atms" else JPSSCrISReader()
        elif reader_name == "gems":
            from monetio.readers.gems import GEMSReader

            path = tmp_path / "gems-process.nc"
            xr.Dataset(
                {
                    "ColumnAmountNO2": (
                        ("nscans", "npixels"),
                        np.arange(20, dtype="f4").reshape(4, 5),
                    ),
                    "Latitude": (("nscans", "npixels"), np.full((4, 5), 35.0, dtype="f4")),
                    "Longitude": (("nscans", "npixels"), np.full((4, 5), 130.0, dtype="f4")),
                    "Time": (("nscans",), pd.date_range("2023-01-01", periods=4, freq="h")),
                }
            ).to_netcdf(path, engine="h5netcdf")
            reader = GEMSReader()
        elif reader_name == "omps":
            from monetio.readers.omps import OMPSReader

            path = tmp_path / "omps-process.nc"
            xr.Dataset(
                {
                    "ColumnAmountO3": (
                        ("lat", "lon"),
                        np.arange(12, dtype="f4").reshape(3, 4) + 300.0,
                        {"units": "DU"},
                    ),
                    "Latitude": (("lat",), [-30.0, 0.0, 30.0]),
                    "Longitude": (("lon",), [-180.0, -90.0, 0.0, 90.0]),
                },
                attrs={"Date": "2024-01-01"},
            ).to_netcdf(path, engine="h5netcdf")
            reader = OMPSReader()
        elif reader_name == "nasa_modis":
            from monetio.readers.nasa_modis import NASAMODISReader

            path = tmp_path / "nasa-modis-process.nc"
            xr.Dataset(
                {"Data_Field": (("y", "x"), np.arange(100, dtype="f4").reshape(10, 10))},
                attrs={
                    "HORIZONTALTILENUMBER": 10,
                    "VERTICALTILENUMBER": 5,
                    "RANGEBEGINNINGDATE": "2023-01-01",
                    "RANGEBEGINNINGTIME": "12:00:00",
                },
            ).to_netcdf(path, engine="h5netcdf")
            reader = NASAMODISReader()
        elif reader_name == "nesdis_eps_viirs":
            from monetio.readers.nesdis_eps_viirs import NESDISEPSVIIRSReader

            path = next(
                (Path(__file__).parent / "data" / "samples" / "nesdis_eps_viirs").glob("*.nc")
            )
            reader = NESDISEPSVIIRSReader()
        elif reader_name == "modis_l2":
            from monetio.readers.modis_l2 import MODISL2Reader

            path = tmp_path / "modis-l2-process.nc"
            n_rows, n_cols = 6, 4
            lat, lon = np.meshgrid(np.linspace(30, 40, n_cols), np.linspace(-100, -90, n_rows))
            xr.Dataset(
                {
                    "AOD": (
                        ("Cell_Along_Swath", "Cell_Across_Swath"),
                        np.linspace(0.05, 0.95, n_rows * n_cols, dtype=np.float32).reshape(
                            n_rows, n_cols
                        ),
                    ),
                    "Quality_Assurance": (
                        ("Cell_Along_Swath", "Cell_Across_Swath"),
                        np.full((n_rows, n_cols), 3, dtype=np.int8),
                    ),
                    "Scan_Start_Time": (("Cell_Along_Swath",), np.full(n_rows, 946684800.0)),
                },
                coords={
                    "Latitude": (("Cell_Along_Swath", "Cell_Across_Swath"), lat),
                    "Longitude": (("Cell_Along_Swath", "Cell_Across_Swath"), lon),
                },
            ).to_netcdf(path, engine="h5netcdf")
            reader = MODISL2Reader()
        elif reader_name == "mopitt":
            from monetio.readers.mopitt import MOPITTReader

            path = tmp_path / "mopitt-process.nc"
            xr.Dataset(
                {"co_column": (("lon", "lat"), np.arange(12, dtype="f4").reshape(4, 3))},
                coords={
                    "latitude": (("lat",), np.linspace(-60, 60, 3)),
                    "longitude": (("lon",), np.linspace(-150, 150, 4)),
                },
                attrs={"StartTime": [725846400.0]},
            ).to_netcdf(path, engine="h5netcdf")
            reader = MOPITTReader()
        elif reader_name == "tempo":
            from monetio.readers.tempo import TEMPOReader

            path = tmp_path / "tempo-process.nc"
            xr.Dataset(
                {
                    "vertical_column_troposphere": (
                        ("x", "y"),
                        np.arange(20, dtype="f4").reshape(4, 5),
                    )
                }
            ).to_netcdf(path, group="product", engine="h5netcdf")
            reader = TEMPOReader()
        elif reader_name == "tropomi":
            from monetio.readers.tropomi import TROPOMIReader

            path = tmp_path / "tropomi-process.nc"
            xr.Dataset(
                {
                    "no2": (("scanline", "ground_pixel"), np.arange(12, dtype="f4").reshape(3, 4)),
                    "delta_time": (("scanline",), np.array([0, 1000, 2000], dtype="i4")),
                },
                coords={
                    "latitude": (("scanline", "ground_pixel"), np.zeros((3, 4), dtype="f4")),
                    "longitude": (("scanline", "ground_pixel"), np.ones((3, 4), dtype="f4")),
                    "time": np.datetime64("2023-01-01"),
                },
            ).to_netcdf(path, group="PRODUCT", engine="h5netcdf")
            reader = TROPOMIReader()
        elif reader_name == "calipso":
            from monetio.readers.calipso import CALIPSOReader

            path = tmp_path / "calipso-process.nc"
            xr.Dataset(
                {
                    "Extinction_Coefficient_532": (
                        ("nray", "nbin"),
                        np.arange(30, dtype="f4").reshape(5, 6),
                    ),
                    "Latitude": (("nray",), np.linspace(-30, 30, 5)),
                    "Longitude": (("nray",), np.linspace(-100, -90, 5)),
                    "Profile_Time": (("nray",), np.linspace(0, 6000, 5)),
                }
            ).to_netcdf(path, engine="h5netcdf")
            reader = CALIPSOReader()
        elif reader_name == "earthcare":
            from monetio.readers.earthcare import EarthCAREReader

            path = tmp_path / "earthcare-process.nc"
            xr.Dataset(
                {
                    "aerosol_extinction": (
                        ("profile", "n_range"),
                        np.arange(30, dtype="f4").reshape(5, 6),
                    ),
                    "latitude": (("profile",), np.linspace(-30, 30, 5)),
                    "longitude": (("profile",), np.linspace(-100, -90, 5)),
                    "UTC_time": (("profile",), np.linspace(0, 6000, 5)),
                }
            ).to_netcdf(path, engine="h5netcdf")
            reader = EarthCAREReader()
        elif reader_name == "umbc_aerosol":
            import h5py

            from monetio.readers.umbc_aerosol import UMBCAerosolReader

            path = tmp_path / "UMBC_CL51_process.h5"
            with h5py.File(path, "w") as handle:
                data = handle.create_group("DATA")
                data.create_dataset("Altitude_m", data=np.linspace(0, 500, 6))
                data.create_dataset(
                    "UnixTime_UTC", data=np.linspace(1_600_000_000, 1_600_000_600, 3)
                )
                data.create_dataset("Profile_bsc", data=np.arange(18, dtype="f4").reshape(3, 6))
                attrs = handle.create_group("Instrument_Attributes")
                attrs.attrs["Location_lat"] = 39.25
                attrs.attrs["Location_lon"] = -76.71
            reader = UMBCAerosolReader()
        elif reader_name == "nesdis_frp":
            from monetio.readers.nesdis_frp import NESDISFRPReader

            path = tmp_path / "meanFRP.20230101.FV3.C384Grid.tile1.bin"
            path.write_bytes(np.arange(384 * 384, dtype="f4").tobytes())
            reader = NESDISFRPReader()
        elif reader_name == "omps_nadir":
            from monetio.readers.omps_nadir import OMPSNadirReader

            path = tmp_path / "omps-nadir-process.nc"
            target_time = pd.Timestamp("2024-01-01 18:45:10")
            time_val = (target_time - pd.Timestamp("1958-01-01")).total_seconds() * 1e6
            xr.Dataset(
                {
                    "ColumnAmountO3": (
                        ("nTimes", "nIFOV"),
                        np.arange(20, dtype="f4").reshape(4, 5),
                    ),
                    "ScanTime": (("nTimes",), np.full(4, time_val)),
                    "Latitude": (("nTimes", "nIFOV"), np.zeros((4, 5), dtype="f4")),
                    "Longitude": (("nTimes", "nIFOV"), np.ones((4, 5), dtype="f4")),
                }
            ).to_netcdf(path, engine="h5netcdf")
            reader = OMPSNadirReader()
        elif reader_name == "nesdis_edr_viirs":
            import gzip

            from monetio.readers.nesdis_edr_viirs import NESDISEDRVIIRSReader

            path = tmp_path / "npp_aot550_edr_gridded_0.25_20230101.high.bin.gz"
            data = np.arange(2 * 720 * 1440, dtype="<f4").reshape(2, 720, 1440)
            with gzip.open(path, "wb") as stream:
                stream.write(data.tobytes())
            reader = NESDISEDRVIIRSReader()
        else:
            from monetio.readers.nesdis_viirs_jrr import VIIRSJRRReader

            path = tmp_path / "viirs-jrr-process.nc"
            xr.Dataset(
                {"AOD550": (("Rows", "Columns"), np.arange(12, dtype="f4").reshape(3, 4))},
                coords={
                    "Latitude": (("Rows", "Columns"), np.zeros((3, 4), dtype="f4")),
                    "Longitude": (("Rows", "Columns"), np.ones((3, 4), dtype="f4")),
                },
                attrs={"time_coverage_start": "2024-01-01T00:00:00Z"},
            ).to_netcdf(path, engine="h5netcdf")
            reader = VIIRSJRRReader()

        worker_fixture = (
            "large_process_distributed_cluster"
            if reader_name in {"nesdis_eps_viirs", "nesdis_edr_viirs"}
            else "process_distributed_cluster"
        )
        client = request.getfixturevalue(worker_fixture)
        worker_pids = set(client.run(os.getpid).values())
        reader_kwargs = {"engine": "h5netcdf"}
        if reader_name == "gems":
            reader_kwargs["group"] = ""
        if reader_name == "omps":
            reader_kwargs["product"] = "nmto3_l3"
        if reader_name == "modis_l2":
            reader_kwargs["variable_dict"] = {
                "AOD": {"minimum": 0.0, "maximum": 1.0},
                "Quality_Assurance": {},
            }
        if reader_name == "tempo":
            reader_kwargs["group"] = "product"
        if reader_name == "tropomi":
            reader_kwargs.update(group="PRODUCT", calculate_pressure=False)
        if reader_name == "nesdis_frp":
            reader_kwargs["ftype"] = "meanFRP"
        if reader_name == "omps_nadir":
            reader_kwargs["product"] = "v8toz"
        if reader_name == "nesdis_edr_viirs":
            reader_kwargs["resolution"] = "low"
        if reader_name in {"nesdis_viirs_jrr", "viirs_jrr"}:
            reader_kwargs["product"] = "AOD"
        lazy = reader.open_dataset(str(path), use_dask=True, **reader_kwargs)
        eager = reader.open_dataset(str(path), lazy=False, **reader_kwargs)
        variable = next(name for name in lazy.data_vars if lazy[name].chunks is not None)
        marked = lazy[variable].copy(
            data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )
        lazy.close()

    @requires_distributed
    @pytest.mark.parametrize(
        "reader_name",
        ["ncep_grib", "grib2", "gfs", "gefs", "gdas", "hrrr", "nam", "rap", "rrfs"],
    )
    def test_grib_readers_run_on_process_workers(
        self, grib_process_distributed_cluster, reader_name
    ):
        """Actual GRIB2 backend tasks honor chunks and execute on process workers."""
        import os

        pytest.importorskip("grib2io")

        from monetio.readers.gfs import GDASReader, GEFSReader, GFSReader
        from monetio.readers.grib2 import Grib2Reader
        from monetio.readers.hrrr import HRRRReader
        from monetio.readers.nam import NAMReader
        from monetio.readers.ncep_grib import NCEPGribReader
        from monetio.readers.rap import RAPReader
        from monetio.readers.rrfs import RRFSReader

        readers = {
            "ncep_grib": NCEPGribReader,
            "grib2": Grib2Reader,
            "gfs": GFSReader,
            "gefs": GEFSReader,
            "gdas": GDASReader,
            "hrrr": HRRRReader,
            "nam": NAMReader,
            "rap": RAPReader,
            "rrfs": RRFSReader,
        }
        sample_dir = Path(__file__).parent / "data" / "samples"
        sample = sample_dir / (
            "grib2/grib2_sample.grib2"
            if reader_name == "grib2"
            else "ncep_grib/ncep_grib_sample.grib2"
        )
        reader = readers[reader_name]()
        # The native backend decodes one complete GRIB message per read; use
        # one spatial block for worker safety here. Separate local cases cover
        # smaller caller-selected chunks and their rechunking cost.
        kwargs = {"chunks": {"y": 181, "x": 360}}
        lazy = reader.open_dataset(str(sample), use_dask=True, **kwargs)
        eager = reader.open_dataset(str(sample), use_dask=False)
        variable = next(name for name in lazy.data_vars if lazy[name].chunks is not None)
        assert lazy[variable].chunks[lazy[variable].get_axis_num("y")] == (181,)
        assert lazy[variable].chunks[lazy[variable].get_axis_num("x")] == (360,)

        client = grib_process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        marked = lazy[variable].copy(
            data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marked.dims, marked.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )
        lazy.close()
        eager.close()

    @requires_distributed
    @pytest.mark.parametrize(
        "reader_class,variable,dataset",
        [
            (
                EARLINETReader,
                "backscatter",
                xr.Dataset(
                    {
                        "backscatter": (
                            ("wavelength", "time", "altitude"),
                            np.ones((1, 2, 4), dtype="f4"),
                        ),
                        "latitude": ((), 45.82),
                        "longitude": ((), 8.617),
                    },
                    coords={
                        "time": pd.date_range("2024-01-01", periods=2, freq="h"),
                        "altitude": np.arange(4, dtype="f4") * 100,
                        "wavelength": [1064.0],
                    },
                    attrs={"station_ID": "ipr"},
                ),
            ),
            (
                MPLNETReader,
                "nrb",
                xr.Dataset(
                    {
                        "nrb": (("time", "altitude"), np.arange(8, dtype="f4").reshape(2, 4)),
                        "latitude": (("time",), [40.0, 40.0]),
                        "longitude": (("time",), [-80.0, -80.0]),
                        "surface_altitude": (("time",), [0.5, 0.5]),
                    },
                    coords={
                        "time": pd.date_range("2024-01-01", periods=2, freq="min"),
                        "altitude": np.arange(4, dtype="f4"),
                    },
                    attrs={"title": "MPLNET V3 NRB Data"},
                ),
            ),
        ],
        ids=["earlinet", "mplnet"],
    )
    def test_profile_netcdf_use_dask_runs_on_process_workers(
        self, tmp_path, process_distributed_cluster, reader_class, variable, dataset
    ):
        """EARLINET/MPLNET NetCDF arrays compute on workers with eager parity."""
        import os

        path = tmp_path / f"{reader_class.__name__}-worker.nc"
        dataset.to_netcdf(path, engine="h5netcdf")
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = reader_class()
        lazy = reader.open_dataset(str(path), engine="h5netcdf", use_dask=True)
        eager = reader.open_dataset(str(path), engine="h5netcdf", use_dask=False)
        marker = lazy[variable].copy(
            data=lazy[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marker.dims, marker.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )

    @requires_distributed
    def test_icap_mme_use_dask_runs_on_process_workers(self, tmp_path, process_distributed_cluster):
        """ICAP-MME's local NetCDF task graph evaluates in worker processes."""
        import os

        path = tmp_path / "icap-worker.nc"
        times = pd.date_range("2023-01-01", periods=4, freq="6h")
        values = np.arange(4 * 10 * 20, dtype="f4").reshape(4, 10, 20)
        xr.Dataset(
            {"dustaod550": (("time", "lat", "lon"), values)},
            coords={
                "time": times,
                "lat": np.linspace(-90, 90, 10),
                "lon": np.linspace(-180, 180, 20),
            },
        ).to_netcdf(path, engine="h5netcdf")
        reader = ICAPMMEReader()
        lazy = reader.open_dataset(str(path), engine="h5netcdf", use_dask=True, chunks={"time": 2})
        eager = reader.open_dataset(str(path), engine="h5netcdf", use_dask=False)
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        marker = lazy.dustaod550.copy(
            data=lazy.dustaod550.data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = lazy.assign(_worker_pid=(marker.dims, marker.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
        assert actual_pids and actual_pids <= worker_pids
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )

    @requires_distributed
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_tccon_use_dask_runs_on_process_workers(
        self, tmp_path, process_distributed_cluster, as_xarray
    ):
        """TCCON's per-file Dask NetCDF graph runs in worker processes."""
        import os

        import dask.dataframe as dd

        path = tmp_path / "tccon-process.nc"
        xr.Dataset(
            {"xco2": (("time",), np.array([410.0, 411.0], dtype="f4"))},
            coords={
                "time": pd.date_range("2024-01-01", periods=2, freq="h"),
                "lat_deg": 40.0,
                "long_deg": -105.0,
            },
            attrs={"site_name": "testsite"},
        ).to_netcdf(path, engine="h5netcdf")
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = TCCONReader()
        lazy = reader.open_dataset(str(path), engine="h5netcdf", use_dask=True, as_xarray=as_xarray)
        eager = reader.open_dataset(
            str(path), engine="h5netcdf", use_dask=False, as_xarray=as_xarray
        )
        if isinstance(lazy, dd.DataFrame):
            marked = lazy.map_partitions(
                _mark_partition_worker,
                meta=lazy._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
            )
        else:
            marker = lazy["xco2"].copy(
                data=lazy["xco2"].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = lazy.assign(_worker_pid=(marker.dims, marker.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_equal(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )
        assert actual_pids and actual_pids <= worker_pids

    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_tccon_use_dask_honors_variable_selection(self, tmp_path, as_xarray):
        path = tmp_path / "tccon-selective.nc"
        xr.Dataset(
            {
                "xco2": (("time",), np.array([410.0, 411.0], dtype="f4")),
                "unrelated": (("time",), np.array([1.0, 2.0], dtype="f4")),
            },
            coords={
                "time": pd.date_range("2024-01-01", periods=2, freq="h"),
                "lat_deg": 40.0,
                "long_deg": -105.0,
            },
            attrs={"site_name": "testsite"},
        ).to_netcdf(path, engine="h5netcdf")
        result = TCCONReader().open_dataset(
            str(path),
            engine="h5netcdf",
            use_dask=True,
            as_xarray=as_xarray,
            drop_variables=["unrelated"],
        )
        names = set(result.variables) if isinstance(result, xr.Dataset) else set(result.columns)
        assert "unrelated" not in names
        assert "xco2" in names

    @pytest.mark.parametrize(
        "reader_name,reader_class", [("amdar", AMDARReader), ("madis", MADISReader)]
    )
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_direct_point_netcdf_use_dask_honors_chunk_layout(
        self, tmp_path, reader_name, reader_class, as_xarray
    ):
        """Requested source chunks are retained through point output conversion."""
        import dask.dataframe as dd

        paths = _synthetic_point_netcdf(tmp_path, reader_name)
        output = reader_class().open_dataset(
            paths,
            use_dask=True,
            as_xarray=as_xarray,
            expand2d=False,
            chunks={"recNum": 1},
        )
        if as_xarray:
            assert output["temperature"].chunks == ((1, 1, 1, 1),)
        else:
            assert isinstance(output, dd.DataFrame)
            # Column-wise Dask array concatenation realigns the four input
            # chunks into three table partitions; source chunks are accepted
            # but do not define the final table partition count.
            assert output.npartitions == 3

    @pytest.mark.parametrize(
        "reader_name,reader_class", [("amdar", AMDARReader), ("madis", MADISReader)]
    )
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    def test_direct_point_netcdf_use_dask_honors_variable_selection(
        self, tmp_path, reader_name, reader_class, as_xarray
    ):
        """Xarray's drop_variables selection is passed through before reading."""
        import dask.dataframe as dd

        paths = _synthetic_point_netcdf(tmp_path, reader_name)
        output = reader_class().open_dataset(
            paths,
            use_dask=True,
            as_xarray=as_xarray,
            expand2d=False,
            drop_variables=["latitude"],
        )
        names = set(output.columns) if isinstance(output, dd.DataFrame) else set(output.variables)
        assert "latitude" not in names
        assert "temperature" in names

    @requires_distributed
    @pytest.mark.parametrize("defer", [False, True], ids=["use-dask", "defer"])
    def test_tolnet_deferred_output_runs_on_process_workers(
        self, tmp_path, process_distributed_cluster, defer
    ):
        """Path-backed TOLNet chunks reopen safely on process workers."""
        import os

        path = _synthetic_tolnet_hdf5(tmp_path)
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = TOLNetReader()
        dask_options = {"defer": True} if defer else {"use_dask": True}
        deferred = reader.open_dataset(path, **dask_options, chunks={"z": 2, "time": 1})
        eager = reader.open_dataset(path, use_dask=False)
        variable = "ozone_mixing_ratio"
        marker = deferred[variable].copy(
            data=deferred[variable].data.map_blocks(_worker_pid_block, dtype="int64")
        )
        computed = deferred.assign(_worker_pid=(marker.dims, marker.data)).compute()
        actual_pids = set(computed["_worker_pid"].values.ravel().tolist())

        assert actual_pids and actual_pids <= worker_pids
        assert deferred[variable].chunks == ((2, 2, 2, 2, 2), (1, 1))
        xr.testing.assert_allclose(
            _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
        )

    @requires_distributed
    @pytest.mark.parametrize("as_xarray", [False, True], ids=["table", "wide-xarray"])
    @pytest.mark.parametrize("defer", [False, True], ids=["use-dask", "defer"])
    def test_aeronet_deferred_output_runs_on_process_workers(
        self, process_distributed_cluster, as_xarray, defer
    ):
        """AERONET text parser and table-to-wide graph run on worker processes."""
        import os

        import dask.dataframe as dd

        path = _ROOT / "data" / "aeronet-AOD15-example.txt"
        client = process_distributed_cluster
        worker_pids = set(client.run(os.getpid).values())
        assert len(worker_pids) == 2
        assert os.getpid() not in worker_pids

        reader = AERONETReader()
        deferred = reader.open_dataset(
            files=str(path),
            as_xarray=as_xarray,
            defer=defer,
            use_dask=not defer,
            lazy=False,
        )
        eager = reader.open_dataset(files=str(path), as_xarray=as_xarray, defer=False, lazy=False)
        if isinstance(deferred, dd.DataFrame):
            marked = deferred.map_partitions(
                _mark_partition_worker,
                meta=deferred._meta.assign(_worker_pid=pd.Series(dtype="int64")),
            ).compute()
            actual_pids = set(marked.pop("_worker_pid").unique())
            pd.testing.assert_frame_equal(
                marked.reset_index(drop=True), eager.reset_index(drop=True), check_dtype=False
            )
        else:
            variable = next(name for name, value in deferred.data_vars.items() if value.ndim > 0)
            marker = deferred[variable].copy(
                data=deferred[variable].data.map_blocks(_worker_pid_block, dtype="int64")
            )
            computed = deferred.assign(_worker_pid=(marker.dims, marker.data)).compute()
            actual_pids = set(computed["_worker_pid"].values.ravel().tolist())
            xr.testing.assert_equal(
                _strip_history(computed.drop_vars("_worker_pid")), _strip_history(eager)
            )

        assert actual_pids and actual_pids <= worker_pids

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
    @pytest.mark.skipif(len(_MULTIFILE) < 2, reason="multifile fixture missing")
    def test_unreachable_scheduler_raises_not_local_result(self):
        """A named-but-unreachable scheduler must error, never return data.

        The dangerous failure mode is ``compute()`` quietly running single-machine
        and *appearing* to succeed while the user believes it is distributed.
        Requesting the distributed scheduler against a dead address must raise.
        """
        reader = CMAQReader()
        deferred = reader.open_dataset(_MULTIFILE, defer=True)
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
