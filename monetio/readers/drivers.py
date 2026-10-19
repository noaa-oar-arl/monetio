import json
import os
import time
import warnings
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Union
from urllib.parse import quote

import fsspec
import pandas as pd
import xarray as xr

from .errors import (
    ContentInvalidError,
    MalformedInputError,
    OptionUnsupportedError,
    Tolerance,
    annotate,
    format_tolerance_record,
    infer_category,
)

try:
    import dask.dataframe as dd
except ImportError:
    dd = None

try:
    import obstore  # noqa: F401

    HAS_OBSTORE = True
except ImportError:
    HAS_OBSTORE = False


def get_default_storage_options(path: str) -> dict:
    """Get default storage options for a given path/protocol.

    Parameters
    ----------
    path : str
        File path or URL.

    Returns
    -------
    dict
        Default storage options (e.g. ``{"anon": True}``).
    """
    if path.startswith("s3://"):
        return {"anon": True}
    return {}


def _validate_xarray_chunk_dimensions(
    ds: xr.Dataset, chunks, source: str | None = None
) -> xr.Dataset:
    """Reject chunk mapping keys that do not name raw source dimensions."""
    if isinstance(chunks, Mapping):
        unknown = sorted(set(chunks).difference(ds.sizes), key=str)
        if unknown:
            raise ValueError(
                f"Unknown chunk dimension(s) for {source or 'dataset'}: "
                f"{', '.join(map(str, unknown))}; available dimensions are "
                f"{', '.join(ds.sizes)}"
            )
    return ds


def _restore_grib2io_message(path: str, message_number: int):
    """Rebuild a grib2io message without serializing its open file handle."""
    import grib2io

    with grib2io.open(path, _xarray_backend=True) as stream:
        message = stream[message_number]
    # The GRIB backend passes its own worker-local file handle to _data; the
    # message object supplies decoded section metadata only.
    message._monetio_source_path = path
    message._monetio_message_number = int(message_number)
    message_type = type(message)
    if not getattr(message_type, "_monetio_serializable", False):
        message_type.__reduce__ = _reduce_grib2io_message
        message_type._monetio_serializable = True
    return message


def _reduce_grib2io_message(message):
    """Serialize a grib2io message by source path and message index."""
    path = getattr(message, "_monetio_source_path", None)
    number = getattr(message, "_monetio_message_number", None)
    if path is None or number is None:
        on_disk = getattr(message, "_ondiskarray", None)
        handle = getattr(on_disk, "filehandle", None)
        path = getattr(handle, "name", None)
        number = getattr(message, "_msgnum", None)
    if not isinstance(path, str) or number is None:
        raise TypeError("grib2io message has no reopenable MONETIO source identity")
    return _restore_grib2io_message, (path, int(number))


def _make_grib2io_graph_serializable(ds: xr.Dataset) -> xr.Dataset:
    """Make native grib2io Dask source metadata safe to ship to workers.

    grib2io 2.x stores dynamically-created message objects (and their live
    buffered file handles) in its Xarray ``OnDiskArray`` index. Those objects
    cannot be pickled by distributed. Their section metadata is reproducible
    from the file path and message number, so install a compact reducer on the
    backend's message class before the graph is submitted.
    """
    try:
        import grib2io.xarray_backend as grib_backend
    except ImportError:
        return ds

    seen: set[int] = set()

    def visit(value):
        identity = id(value)
        if identity in seen:
            return
        seen.add(identity)

        if (
            type(value).__module__ == grib_backend.__name__
            and type(value).__name__ == "GribBackendArray"
        ):
            index = getattr(value.array, "index", None)
            if index is not None and "msg" in index:
                for message in index["msg"]:
                    source = getattr(getattr(message, "_ondiskarray", None), "filehandle", None)
                    path = getattr(source, "name", None)
                    if not isinstance(path, str):
                        continue
                    message_type = type(message)
                    message._monetio_source_path = path
                    message._monetio_message_number = int(message._msgnum)
                    if not getattr(message_type, "_monetio_serializable", False):
                        message_type.__reduce__ = _reduce_grib2io_message
                        message_type._monetio_serializable = True
            return

        if isinstance(value, Mapping):
            children = (*value.keys(), *value.values())
        elif isinstance(value, tuple | list | set):
            children = value
        else:
            children = tuple(
                getattr(value, name)
                for name in ("array", "args", "kwargs", "func", "task")
                if hasattr(value, name)
            )
        for child in children:
            visit(child)

    for variable in ds.variables.values():
        if variable.chunks is None:
            continue
        graph = variable.data.__dask_graph__()
        layers = getattr(graph, "layers", None)
        if layers is not None:
            for layer in layers.values():
                visit(layer)
        else:
            visit(graph)
    return ds


# Exception types that mean "this engine cannot read this file, try another".
# ``h5netcdf`` signals a non-HDF5 file with a plain
# ``OSError`` ("file signature not found"); ``ImportError`` means the engine
# package is absent; ``ValueError`` covers xarray rejecting the format. Genuine
# read errors — ``PermissionError``/``FileNotFoundError`` (both ``OSError``
# subclasses) — are deliberately EXCLUDED so they propagate with their original
# type instead of being masked by a second engine attempt.
def _read_table_partition(reader_func, path, kwargs, preprocess, source, meta=None):
    """Read and preprocess one delayed table while preserving input identity."""
    try:
        frame = reader_func(path, **kwargs)
        frame = preprocess(frame) if preprocess else frame
        # Header-only inputs commonly infer object dtypes even when the same
        # columns are numeric in data-bearing partitions. An explicit schema
        # is authoritative for an empty partition, where casting cannot alter
        # any values. Keep nonempty schema mismatches visible to Dask's
        # from_delayed metadata check.
        if isinstance(frame, pd.DataFrame) and frame.empty and isinstance(meta, pd.DataFrame):
            frame = frame.reindex(columns=meta.columns).astype(meta.dtypes.to_dict())
        if isinstance(frame, pd.DataFrame) and isinstance(meta, pd.DataFrame):
            from dask.dataframe.utils import check_meta

            # Validate inside the per-input task so a mismatch is annotated
            # with the offending file instead of surfacing later as an
            # anonymous from_delayed metadata error.
            check_meta(frame, meta, funcname="MONETIO reader")
        return frame
    except Exception as exc:  # pragma: no cover - exercised through delayed evaluation
        raise annotate(exc, path) from exc


def _engine_not_applicable(exc: BaseException) -> bool:
    if isinstance(exc, ImportError | ValueError):
        return True
    return isinstance(exc, OSError) and not isinstance(exc, PermissionError | FileNotFoundError)


def _annotate_files(exc: BaseException, file_list) -> BaseException:
    """Same-typed copy of ``exc`` whose message names the file(s) involved.

    An error must identify the specific file and keep its original
    exception type (the old blanket ``raise OSError(...)`` flattened everything).
    Falls back to the untouched exception if the type cannot be reconstructed.
    """
    return annotate(exc, *file_list)


def _reject_unsupported_options(exc: BaseException, source: str | None) -> BaseException:
    """Turn a backend ``TypeError`` about an unexpected keyword into a named rejection.

    When a keyword reaches a storage backend that does not define it, the backend
    raises ``TypeError: ... got an unexpected keyword argument 'X'``. That names
    neither the source nor the option in the caller's terms; this re-raises it as
    an error naming both, so an option in a source's namespace is never silently
    accepted or reported as a backend internal.
    """
    import re

    if not isinstance(exc, TypeError):
        return exc
    match = re.search(r"unexpected keyword argument '([^']+)'", str(exc))
    if match is None:
        return exc
    return OptionUnsupportedError(match.group(1), source or "source")


def _validate_nonempty_result(result, file_list, source: str | None):
    """Refuse an empty result in place of a failure.

    A load that opened every requested input and produced a result carrying no
    information — a dataset with no variables, a dataset whose dimensions are
    all empty, or a frame with neither rows nor columns — is indistinguishable
    from a successful load unless it says so. If the inputs were genuinely empty
    that is still reported here rather than passed through as success.
    """
    if not file_list:
        return result
    if isinstance(result, xr.Dataset):
        if len(result.data_vars) == 0:
            raise ContentInvalidError(
                "load produced a dataset with no data variables",
                *file_list,
                cause=(
                    f"{len(file_list)} input(s) were opened but contributed no "
                    "variables; the result would be indistinguishable from a "
                    "successful load"
                ),
            )
        if result.dims and all(size == 0 for size in result.sizes.values()):
            # Variables exist but every dimension is empty — a schema-only
            # shell (a text reader whose parse silently matched nothing, for
            # instance) is just as indistinguishable from success as having no
            # variables at all.
            raise ContentInvalidError(
                "load produced a dataset with no data points",
                *file_list,
                cause=(
                    f"{len(file_list)} input(s) were opened but contributed no "
                    f"rows on any dimension ({dict(result.sizes)}); the result "
                    "would be indistinguishable from a successful load"
                ),
            )
    elif isinstance(result, pd.DataFrame):
        # A frame with columns but no rows is left to the reader: some sources
        # treat "the query was valid but returned no records" as a documented
        # outcome with its own message, and refusing it here would preempt that
        # report with a generic one. A frame with neither rows nor columns
        # carried no information at all and is refused. A lazy (dask) frame is
        # not validated — nothing has been read yet.
        if len(result) == 0 and len(result.columns) == 0:
            raise ContentInvalidError(
                "load produced an empty result",
                *file_list,
                cause=(
                    f"{len(file_list)} input(s) were opened but contributed no "
                    "rows or columns; the result would be indistinguishable from "
                    "a successful load"
                ),
            )
    return result


def _tolerant_per_file(file_list, open_one, tolerance):
    """Open inputs one at a time, skipping failures under input-scope tolerance.

    Returns ``(results, record)``, where ``record`` is the tolerance record for
    everything skipped. Without tolerance the first failure is raised, annotated
    to name its input. When every input fails, the first failure is raised even
    under tolerance: partial success is tolerated, empty-as-success never is.
    """
    from .errors import ToleranceRecord

    record = ToleranceRecord()
    results = []
    first_error = None
    for f in file_list:
        try:
            results.append(open_one(f))
        except Exception as exc:  # noqa: BLE001 - re-raised or recorded below; categories vary
            annotated = annotate(exc, f)
            if not tolerance:
                raise annotated from exc
            if first_error is None:
                first_error = annotated
            record = record.record_skip(f, str(exc), infer_category(exc))
    if not results and first_error is not None:
        raise first_error
    return results, record


def _file_signature(path: str):
    """Cheap metadata-only structural signature of one file, or ``None``.

    Opens the file lazily (no data is read) and hashes its variable names,
    dimension sizes, per-variable attributes, and coordinate dtypes — the
    three things a combine can disagree on (variables, coordinates,
    attributes). ``decode_times=False`` keeps the signature independent of
    time-decoding differences so only genuine structural mismatches stand out.
    """
    for engine in ("h5netcdf", None):
        try:
            ds = xr.open_dataset(path, decode_times=False, **({"engine": engine} if engine else {}))
        except Exception as exc:  # noqa: BLE001
            if engine is None or not _engine_not_applicable(exc):
                return None  # unopenable, or a real error: this file stands out
            continue
        with ds:
            coord_names = set(ds.coords)

            # ``units``/``calendar`` on coordinate variables legitimately
            # differ per file in a multi-file set (the time axis advances), so
            # they are excluded; every other attribute difference is structural.
            def _attrs(name, var):
                return tuple(
                    sorted(
                        (k, str(v))
                        for k, v in var.attrs.items()
                        if not (k in ("units", "calendar") and name in coord_names)
                    )
                )

            return (
                tuple(sorted(ds.data_vars)),
                tuple(sorted((str(k), int(v)) for k, v in ds.sizes.items())),
                tuple(sorted((name, _attrs(name, var)) for name, var in ds.variables.items())),
                tuple(sorted((str(c), str(ds[c].dtype)) for c in ds.coords)),
            )
    return None


def _mismatch_diagnostic(file_list) -> str | None:
    """Name the file(s) whose structure deviates from the set's majority.

    When a multi-file combine fails (mismatched variables, coordinates, or
    attributes), xarray's own error identifies the conflicting dimension or
    variable but not the *file*. This re-opens each file's metadata only — no
    data payload — and reports the minority-signature paths. Returns ``None``
    when no clear majority exists, so the caller keeps xarray's original error
    instead of a guess.
    """
    from collections import Counter

    files = list(file_list)
    if len(files) < 2:
        return None
    try:
        sigs = {f: _file_signature(f) for f in files}
    except Exception:  # noqa: BLE001 - diagnosis must never mask the real error
        return None
    counts = Counter(s for s in sigs.values() if s is not None)
    if not counts:
        return None
    majority, n = counts.most_common(1)[0]
    if n * 2 <= len(files):  # no majority signature: do not guess a culprit
        return None
    bad = [f for f, s in sigs.items() if s != majority]
    if not bad:
        return None
    preview = ", ".join(str(f) for f in bad[:5])
    if len(bad) > 5:
        preview += ", ..."
    return preview


def _open_mfdataset(file_list, xr_kwargs: dict, engine: str | None = None):
    """``xr.open_mfdataset`` with a file-level diagnostic on combine failures.

    A combine error (``ValueError``, including xarray's ``AlignmentError``)
    gets the mismatched file(s) appended to its message, keeping the original
    exception type. Errors that are not combine-shaped, or sets with no clear
    majority signature, re-raise untouched. A ``_monetio_diagnosed`` marker
    prevents the engine-fallback retry from re-running the diagnosis.
    """
    try:
        if engine is not None:
            return xr.open_mfdataset(file_list, engine=engine, **xr_kwargs)
        return xr.open_mfdataset(file_list, **xr_kwargs)
    except ValueError as exc:
        if getattr(exc, "_monetio_diagnosed", False):
            raise
        diag = _mismatch_diagnostic(file_list)
        if diag is None:
            raise
        annotated = type(exc)(f"{exc} [mismatched file(s): {diag}]")
        annotated._monetio_diagnosed = True
        raise annotated from exc


def _path_protocol(path: str) -> str | None:
    """Return the ``scheme`` of a ``scheme://`` path, or ``None`` for local."""
    if "://" in path:
        return path.split("://", 1)[0]
    return None


def _store_bucket(path: str) -> str:
    """Best-effort store identifier from a path (bucket for s3, root for others)."""
    protocol = _path_protocol(path)
    if not protocol:
        return path
    remainder = path.split("://", 1)[1]
    if protocol == "s3":
        bucket = remainder.split("/")[0]
        return f"s3://{bucket}"
    return f"{protocol}://{remainder.split('/')[0]}"


# botocore error codes / classes that mean "the store rejected our credentials".
_CREDENTIAL_ERROR_CODES = {
    "AccessDenied",
    "InvalidAccessKeyId",
    "ExpiredToken",
    "SignatureDoesNotMatch",
}


def _is_credential_error(exc: BaseException) -> bool:
    """True when ``exc`` is a missing/expired/rejected-credential failure.

    Detected by botocore class name (``NoCredentialsError``) or a ``ClientError``
    whose response error code is in the credential set. A bare network error or
    a genuine "no such bucket" is *not* a credential error and is left untouched
    so it is never mislabeled.
    """
    name = type(exc).__name__
    if name in {"NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"}:
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code", "")
        if code in _CREDENTIAL_ERROR_CODES:
            return True
    return False


def _store_error(exc: BaseException, path: str) -> BaseException:
    """Return ``exc`` with the object store named, when it is a credential error.

    A missing or expired credential must identify the store and the credential
    problem rather than surfacing as a generic file-not-found. The original
    exception type is preserved (botocore errors are annotated in place via
    ``.args``, which their ``str`` reflects); non-credential errors pass
    through unchanged.
    """
    if not _is_credential_error(exc):
        return exc
    store = _store_bucket(str(path))
    message = str(exc)
    if store not in message:
        message = f"{message} [store: {store}; credential problem]"
    try:
        exc.args = (message,)
    except (AttributeError, TypeError):  # pragma: no cover - exotic exceptions
        return type(exc)(message) if _reconstructable(exc) else exc
    return exc


def _reconstructable(exc: BaseException) -> bool:
    try:
        type(exc)(str(exc))
    except Exception:  # noqa: BLE001
        return False
    return True


def _build_s3_config(storage_options: dict) -> dict:
    """Build an S3Store config dict from fsspec-style storage_options."""
    s3_config = {}
    # S3Store expects string values for config keys in some versions,
    # or boolean if using latest obstore. Let's use strings to be safe.
    if storage_options.get("anon", True) or storage_options.get("skip_signature", False):
        s3_config["skip_signature"] = "true"
    if "client_kwargs" in storage_options and "region_name" in storage_options["client_kwargs"]:
        s3_config["region"] = storage_options["client_kwargs"]["region_name"]
    elif "region_name" in storage_options:
        s3_config["region"] = storage_options["region_name"]

    # If no region is provided, obstore might fail if it can't detect it.
    # For common public buckets, us-east-1 is a safe default if detection fails.
    if "region" not in s3_config:
        # Check environment or use a sensible default for public data if anon
        if s3_config.get("skip_signature") == "true":
            s3_config["region"] = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    return s3_config


def _should_fallback_to_s3fs_fileobj(error: Exception) -> bool:
    """Detect obstore/fsspec S3 issues that can be bypassed with s3fs file objects.

    .. deprecated::
        Retained only for backward compatibility with external callers.
        MONETIO no longer registers obstore.fsspec globally, so the failure modes
        this detects should not occur in normal usage.
    """
    msg = str(error).lower()

    return (
        ("default_fill_cache" in msg and "not valid for store 's3'" in msg)
        or "169.254.169.254" in msg
        or "unknownconfigurationkey" in msg
    )


def _select_store(file_list: list[str], storage_options: dict) -> tuple:
    """Select the appropriate object store based on file protocol.

    Parameters
    ----------
    file_list : list[str]
        List of file paths (all assumed to share the same protocol).
    storage_options : dict
        fsspec-style storage options (e.g. ``{"anon": True}``).

    Returns
    -------
    tuple[ObjectStoreRegistry, list[str]]
        The configured registry and (possibly updated) file list.
    """
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import HTTPStore, LocalStore, S3Store

    registry = ObjectStoreRegistry()

    if file_list[0].startswith("s3://"):
        bucket = file_list[0].replace("s3://", "").split("/")[0]
        config = _build_s3_config(storage_options)
        store = S3Store(bucket, config=config)
        registry.register(f"s3://{bucket}", store)
    elif file_list[0].startswith("http://") or file_list[0].startswith("https://"):
        store = HTTPStore()
        registry.register("http://", store)
        registry.register("https://", store)
    else:
        local_files = [Path(f.removeprefix("file://")).resolve() for f in file_list]
        registered_roots = set()
        for path in local_files:
            root = Path(path.anchor)
            root_url = root.as_uri()
            if root_url not in registered_roots:
                # Register the filesystem root as the URI prefix. On Windows,
                # registering only file:/// leaves the encoded drive letter
                # in the object key (D%3A/...), which LocalStore then resolves
                # relative to the current drive and cannot open.
                store = LocalStore(prefix=str(root))
                registry.register(root_url, store)
                # VirtualiZarr/obspec versions may percent-encode the Windows
                # drive colon when resolving a local file URL. Register that
                # equivalent prefix too, so LocalStore receives a path relative
                # to the drive root instead of ``D%3A/...``.
                if root.drive:
                    encoded_drive_url = f"file:///{quote(root.drive, safe='')}/"
                    if encoded_drive_url != root_url:
                        registry.register(encoded_drive_url, store)
                registered_roots.add(root_url)
        file_list = [path.as_uri() for path in local_files]

    return registry, file_list


def _open_via_icechunk(
    vds, icechunk_url: str, virtualizarr_file: str | None, chunks=None
) -> xr.Dataset:
    """Store virtual references in Icechunk and return the dataset.

    Parameters
    ----------
    vds : virtualizarr.VirtualDataset
        The virtual dataset to persist.
    icechunk_url : str
        Path (local or remote) to the Icechunk repository.
    virtualizarr_file : str | None
        Unused for Icechunk but accepted for interface consistency.

    Returns
    -------
    xr.Dataset
        The dataset opened from the Icechunk store.
    """
    # If the local directory already exists, clear it for a clean, idempotent run
    if not icechunk_url.startswith("s3://") and os.path.exists(icechunk_url):
        import shutil

        try:
            shutil.rmtree(icechunk_url)
        except Exception:  # noqa: BLE001 - best-effort cleanup for an idempotent re-run; a store that cannot be cleared is left for the write below to report (categories: permission_denied, not_found)
            pass

    try:
        import icechunk
    except ImportError:
        raise ImportError(
            "Icechunk backend requires 'icechunk'. Install with: pip install monetio[icechunk]"
        )

    from virtualizarr.manifests.array import ManifestArray

    unique_prefixes = set()
    for var in vds.variables.values():
        if isinstance(var.data, ManifestArray):
            for path in var.data.manifest.iter_nonempty_paths():
                if path.startswith("s3://"):
                    parts = path[5:].split("/", 1)
                    prefix = "s3://" + parts[0] + "/"
                    unique_prefixes.add(prefix)
                elif path.startswith("http://") or path.startswith("https://"):
                    parts = path.split("/", 4)
                    prefix = "/".join(parts[:3]) + "/"
                    unique_prefixes.add(prefix)
                elif path.startswith("file://"):
                    prefix = path.rsplit("/", 1)[0] + "/"
                    unique_prefixes.add(prefix)

    config = icechunk.RepositoryConfig.default()
    authorize = {}
    for prefix in unique_prefixes:
        if prefix.startswith("s3://"):
            store_conf = icechunk.s3_store(anonymous=True, region="us-east-1")
            authorize[prefix] = None
        elif prefix.startswith("http://") or prefix.startswith("https://"):
            store_conf = icechunk.http_store()
            authorize[prefix] = icechunk.credentials.HttpAccess
        else:
            store_conf = icechunk.local_filesystem_store(prefix)
            authorize[prefix] = icechunk.credentials.LocalFileSystemAccess

        container = icechunk.VirtualChunkContainer(
            url_prefix=prefix,
            store=store_conf,
        )
        config.set_virtual_chunk_container(container)

    if icechunk_url.startswith("s3://"):
        parts = icechunk_url[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else None
        storage = icechunk.s3_storage(bucket=bucket, prefix=prefix)
    else:
        storage = icechunk.local_filesystem_storage(icechunk_url)
    repo = icechunk.Repository.open_or_create(
        storage,
        config=config,
        authorize_virtual_chunk_access=authorize,
    )
    session = repo.writable_session("main")
    store = session.store

    vds.virtualize.to_icechunk(store)
    session.commit("VirtualiZarr references")

    # Re-open for reading
    session = repo.readonly_session(branch="main")
    open_kwargs = {"consolidated": False}
    if chunks is not None:
        open_kwargs["chunks"] = chunks
    return xr.open_zarr(session.store, **open_kwargs)


def _is_transient_network_error(exc: Exception) -> bool:
    """Return True when the exception looks transient and retryable."""
    msg = str(exc).lower()
    retry_tokens = [
        "timeout",
        "timed out",
        "connect",
        "connection reset",
        "dns error",
        "nodename nor servname",
        "name or service not known",
    ]
    if any(token in msg for token in retry_tokens):
        return True
    return "icechunk" in type(exc).__name__.lower()


def _call_with_retries(
    func: Callable,
    *args,
    attempts: int = 1,
    base_sleep: float = 1.0,
    **kwargs,
):
    """Execute a callable with transient network retries."""
    retry_attempts = max(1, int(attempts))
    retry_sleep = max(0.0, float(base_sleep))

    for attempt in range(1, retry_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - retry loop, not a swallow: re-raised below unless the error is transient and attempts remain (categories: retrieval_failed)
            if attempt >= retry_attempts or not _is_transient_network_error(exc):
                raise
            sleep_seconds = retry_sleep * (2 ** (attempt - 1))
            warnings.warn(
                f"Transient network error in {func.__name__} "
                f"(attempt {attempt}/{retry_attempts}): {exc}. "
                f"Retrying in {sleep_seconds:.1f}s.",
                RuntimeWarning,
                stacklevel=2,
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)


class FileUtility:
    """
    Helper class to manage file path expansion (Local + S3 + HTTP).
    """

    @staticmethod
    def get_fs(path: str, storage_options: dict | None = None, **kwargs):
        """
        Returns the correct filesystem (local, s3, http, or other fsspec
        protocol) based on the path's protocol.

        Options may be supplied either as a ``storage_options`` mapping or as
        keyword arguments; both are forwarded to the fsspec backend, with
        keyword arguments winning on a duplicate key. For ``s3://`` paths,
        ``anon`` defaults to ``True`` (public bucket) exactly as before;
        passing ``anon=False`` uses AWS credentials, so a missing/expired
        credential surfaces as its own error rather than being masked as an
        empty listing. Any other ``protocol://`` prefix (``memory://``,
        ``gcs://``, ``abfs://``, …) resolves to that fsspec backend; plain
        paths use the local filesystem.

        Parameters
        ----------
        path : str
            File path or URL.
        storage_options : dict or None, optional
            Options forwarded to fsspec.filesystem.
        **kwargs : dict
            Additional arguments passed to fsspec.filesystem.
        """
        options = dict(storage_options or {})
        options.update(kwargs)
        if path.startswith("s3://"):
            # anon=True means public bucket. Use anon=False to use your AWS credentials.
            for key, value in get_default_storage_options(path).items():
                options.setdefault(key, value)
            return fsspec.filesystem("s3", **options)
        elif path.startswith("http://") or path.startswith("https://"):
            return fsspec.filesystem("http", **options)
        elif path.startswith("ftp://"):
            return fsspec.filesystem("ftp", **options)
        elif "://" in path:
            protocol = path.split("://", 1)[0]
            return fsspec.filesystem(protocol, **options)
        return fsspec.filesystem("file", **options)

    @staticmethod
    def expand_paths(
        path_input: str | list[str], fs=None, storage_options: dict | None = None, **kwargs
    ) -> list[str]:
        """
        Converts a string (with wildcards), a single path, or a list of paths
        into a guaranteed list of file paths/objects.
        """
        # Convert Path objects to string
        if hasattr(path_input, "__fspath__"):
            path_input = str(path_input)

        # Case 1: It's a list already
        if isinstance(path_input, list):
            return sorted([str(p) if hasattr(p, "__fspath__") else p for p in path_input])

        # Case 2: It's a single string (S3 or Local)
        if isinstance(path_input, str):
            # If no specific filesystem provided, guess it from the path
            if fs is None:
                fs = FileUtility.get_fs(path_input, storage_options, **kwargs)

            # Use fsspec/s3fs to glob wildcards (works for s3://bucket/data/*.nc too!)
            if any(char in path_input for char in ["*", "?"]):
                # HTTP globbing is generally not supported by fsspec without specific implementation
                # For S3/Local it works.
                if path_input.startswith("http"):
                    # Fallback: treat as single file if glob chars present but http (unlikely to work)
                    pass

                try:
                    files = sorted(fs.glob(path_input))
                except Exception as exc:  # noqa: BLE001
                    raise _store_error(exc, path_input) from exc
                # fs.glob usually returns paths without the protocol
                # (e.g. 'bucket/file.nc'); restore it so downstream open()
                # calls resolve the same backend the glob used.
                protocol = _path_protocol(path_input)
                if protocol and files and not str(files[0]).startswith(f"{protocol}://"):
                    files = [f"{protocol}://{f}" for f in files]

                if not files:
                    raise FileNotFoundError(f"No files found matching pattern: {path_input}")
                return files
            else:
                # It is a specific single file
                if not path_input.startswith("http") and not fs.exists(path_input):
                    raise FileNotFoundError(f"File not found: {path_input}")
                return [path_input]

        raise TypeError(f"Invalid path type: {type(path_input)}. Must be str or list.")

    @staticmethod
    def expand_paths_iter(
        path_input, fs=None, chunk_size: int = 10_000, storage_options: dict | None = None
    ):
        """Streaming, order-preserving variant of :meth:`expand_paths`.

        Yields file paths in the **same globally sorted order** as
        ``expand_paths`` — so ``concat``/``combine_nested`` results are
        identical — but never materializes a single full sorted collection:
        paths are consumed in bounded ``chunk_size`` runs, each sorted, then
        merged lazily with :func:`heapq.merge`. This lets a lazy consumer such
        as the ``dask.delayed`` fan-out (``concurrent_files=True``) build its
        task graph over ≥100,000 paths without an O(n) ``sorted`` list.

        The default :meth:`expand_paths` (a plain list) is unchanged.

        Raises the same ``FileNotFoundError`` as ``expand_paths`` when a glob
        matches nothing or a named single file does not exist (spec edge case).
        """
        import heapq
        from itertools import islice

        if hasattr(path_input, "__fspath__"):
            path_input = str(path_input)

        glob_mode = False
        if isinstance(path_input, list):
            raw = iter(str(p) if hasattr(p, "__fspath__") else p for p in path_input)
        elif isinstance(path_input, str):
            if fs is None:
                fs = FileUtility.get_fs(path_input, storage_options)
            if any(char in path_input for char in ["*", "?"]):
                glob_mode = True
                if path_input.startswith("http"):
                    raise FileNotFoundError(f"No files found matching pattern: {path_input}")
                protocol = _path_protocol(path_input)

                def _raw_glob(fs=fs, pattern=path_input, protocol=protocol):
                    # fsspec's glob materializes its own list; we stream it into
                    # bounded sorted runs below rather than sorting the whole set.
                    try:
                        globbed = fs.glob(pattern)
                    except Exception as exc:  # noqa: BLE001
                        raise _store_error(exc, pattern) from exc
                    for f in globbed:
                        f = str(f)
                        if protocol and not f.startswith(f"{protocol}://"):
                            f = f"{protocol}://{f}"
                        yield f

                raw = _raw_glob()
            else:
                if not path_input.startswith("http") and not fs.exists(path_input):
                    raise FileNotFoundError(f"File not found: {path_input}")
                yield path_input
                return
        else:
            raise TypeError(f"Invalid path type: {type(path_input)}. Must be str or list.")

        # Bounded-memory sorted merge: sort each chunk_size run (working set
        # O(chunk_size)) and lazily k-way-merge the runs. The merged stream is
        # globally sorted — identical to expand_paths' output, so concat order
        # is unchanged — while no single full sorted list is ever built and a
        # lazy consumer can overlap with later runs.
        runs = []
        while True:
            chunk = sorted(islice(raw, chunk_size))
            if not chunk:
                break
            runs.append(chunk)

        if not runs:
            if glob_mode:
                raise FileNotFoundError(f"No files found matching pattern: {path_input}")
            return  # empty list input -> empty stream (matches expand_paths)

        yield from heapq.merge(*runs)


class XarrayDriver:
    """
    The unified driver for opening gridded data (NetCDF, GRIB, HDF).
    Supports S3 via obstore or fsspec.
    """

    def __init__(self, source: str | None = None):
        #: Manifest source name, when the driver belongs to a registered
        #: reader. Errors name it (an unsupported option must identify the
        #: source, not the backend engine) and scale-capability declarations
        #: are looked up through it. A bare ``XarrayDriver()`` used directly
        #: (e.g. in tests) has no name and skips the capability check.
        self.source = source

    def _validate_chunk_dimensions(self, ds: xr.Dataset, chunks) -> xr.Dataset:
        """Reject chunk keys that cannot apply to the opened source dimensions.

        xarray currently ignores unknown dimension names in a chunk mapping,
        which makes a typo silently fall back to backend defaults. Validate on
        the raw dataset, before reader preprocessing renames or drops dimensions.
        """
        return _validate_xarray_chunk_dimensions(ds, chunks, self.source)

    def _require_defer_support(self) -> None:
        """Refuse ``defer=True`` for a source that has not earned it.

        Enforced here rather than only in the reader base class because
        readers forward their keyword arguments down to the driver: checking
        at the driver means ``defer`` can never reach a storage backend as an
        unknown keyword (which surfaced there as a ``TypeError`` naming
        neither the source nor the option).
        """
        from .._manifest import resolve
        from .errors import CapabilityUnavailableError

        if self.source is None:
            return
        try:
            spec = resolve(self.source)
        except KeyError:
            return
        if not spec.capabilities.deferred:
            reason = spec.capabilities.reason or "no reason recorded"
            raise CapabilityUnavailableError(spec.name, "defer", reason)

    def _open_grib2io_native(
        self,
        file_list: list[str],
        xr_kwargs: dict,
        preprocess: Callable[[xr.Dataset], xr.Dataset] | None,
        retry_attempts: int,
        retry_base_sleep: float,
        icechunk_url: str | None = None,
    ) -> xr.Dataset:
        """Open GRIB2 data via native xarray+grib2io backend."""
        xr_kwargs["engine"] = "grib2io"

        # Keep public S3 reads safe by default unless the caller set explicit options.
        if (
            file_list
            and str(file_list[0]).startswith("s3://")
            and "storage_options" not in xr_kwargs
        ):
            xr_kwargs["storage_options"] = get_default_storage_options(str(file_list[0]))

        # Remove ReferenceGenerator / VirtualiZarr / icechunk-only parameters from xr_kwargs
        # so they do not cause TypeErrors in standard/fallback paths.
        for key in [
            "use_icechunk",
            "max_workers",
            "network_timeout",
            "max_concurrent_requests",
            "max_scan_attempts",
            "store_path",
            "icechunk_url",
            "icechunk_repo",
        ]:
            xr_kwargs.pop(key, None)

        # Icechunk path: delegate to grib2io's own open_mfdataset, which builds a
        # SINGLE combined manifest across all files and writes ONE icechunk store
        # with ONE commit. Routing through xarray.open_mfdataset instead would open
        # each file via the backend separately, creating one icechunk store/commit
        # per file -- slow and unsafe for concurrent local commits.
        if xr_kwargs.get("use_icechunk"):
            from grib2io.xarray_backend import open_mfdataset as grib2io_open_mfdataset

            mf_kwargs = dict(xr_kwargs)
            # engine/use_icechunk are passed explicitly; xarray-combine-only kwargs
            # are not used by grib2io's icechunk path.
            mf_kwargs.pop("engine", None)
            mf_kwargs.pop("use_icechunk", None)
            for key in (
                "combine",
                "concat_dim",
                "coords",
                "compat",
                "data_vars",
                "ids",
                "infer_order",
                "join",
                "backend_kwargs",
            ):
                mf_kwargs.pop(key, None)
            # grib2io writes the store at store_path; map MONETIO's icechunk_url.
            if icechunk_url and "store_path" not in mf_kwargs:
                mf_kwargs["store_path"] = icechunk_url
            if preprocess:
                mf_kwargs["preprocess"] = preprocess

            return _call_with_retries(
                grib2io_open_mfdataset,
                file_list,
                attempts=retry_attempts,
                base_sleep=retry_base_sleep,
                use_icechunk=True,
                **mf_kwargs,
            )

        # Default to nested concatenation along time for multi-file GRIB2 scans.
        # grib2io messages frequently lack the dimension coordinates that
        # open_mfdataset's default combine="by_coords" needs, so prefer the
        # documented nested/time pattern unless the caller specifies otherwise.
        # coords="minimal"/compat="override" tolerate per-file scalar coords
        # (e.g. fixed-surface coords like "entire_atmosphere") that vary across
        # forecast lead times.
        if len(file_list) > 1:
            xr_kwargs.setdefault("concat_dim", "time")
            xr_kwargs.setdefault("combine", "nested")
            xr_kwargs.setdefault("coords", "minimal")
            xr_kwargs.setdefault("compat", "override")

        if preprocess:
            xr_kwargs["preprocess"] = preprocess

        with warnings.catch_warnings():
            # Keep logs clean while preserving actionable user warnings.
            warnings.filterwarnings(
                "ignore",
                message=".*pkg_resources is deprecated as an API.*",
                category=UserWarning,
            )

            if len(file_list) == 1:
                # Remove open_mfdataset-only kwargs to avoid open_dataset type errors.
                mfdataset_keys = [
                    "combine",
                    "concat_dim",
                    "parallel",
                    "compat",
                    "data_vars",
                    "coords",
                    "ids",
                    "infer_order",
                    "join",
                ]
                for key in mfdataset_keys:
                    xr_kwargs.pop(key, None)

                # ``xr.open_dataset`` has no ``preprocess`` argument and forwards
                # unknown kwargs straight to the backend, which the grib2io
                # entrypoint rejects with a ``TypeError``. ``open_mfdataset`` (the
                # multi-file path below) runs ``preprocess`` itself, so only the
                # single-file path needs it applied to the opened dataset here.
                xr_kwargs.pop("preprocess", None)
                ds = _call_with_retries(
                    xr.open_dataset,
                    file_list[0],
                    attempts=retry_attempts,
                    base_sleep=retry_base_sleep,
                    **xr_kwargs,
                )
                return preprocess(ds) if preprocess else ds

            return _call_with_retries(
                xr.open_mfdataset,
                file_list,
                attempts=retry_attempts,
                base_sleep=retry_base_sleep,
                **xr_kwargs,
            )

    def open(
        self,
        files: str | list[str],
        use_dask: bool = False,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        parallel: bool = False,
        concurrent_files: bool = False,
        defer: bool = False,
        tolerate=None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Open gridded data backend-agnostically.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path(s), URL(s), or glob pattern.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr to create a virtual Zarr dataset, by default False.
            Useful for large datasets to avoid xarray.open_mfdataset overhead.
        virtualizarr_file : str, optional
            Path to save/load the VirtualiZarr reference JSON file. If provided and the file
            exists, the references will be loaded from it. If the file does not exist,
            the references will be computed and saved to this path.
        virtualizarr_parser : str, optional
            The VirtualiZarr parser to use (e.g., 'hdf5', 'netcdf3', 'zarr', 'grib2').
            If None, MONETIO will attempt to infer it from the engine or file extension.
        virtualizarr_backend : str, optional
            Backend for VirtualiZarr references ("kerchunk" or "icechunk"), by default "kerchunk".
            Note: This parameter is deprecated in favor of `use_icechunk`.
        icechunk_repo : str, optional
            Path to the Icechunk repository. Required when
            ``virtualizarr_backend="icechunk"``.
            Note: This parameter is deprecated in favor of `icechunk_url`.
        use_icechunk : bool, optional
            Whether to use Icechunk as the storage backend for VirtualiZarr references,
            by default False. If True, references are stored in an Icechunk repository.
            If False, the default Kerchunk JSON format is used.
        icechunk_url : str, optional
            Path or URL to the Icechunk repository. Required when ``use_icechunk=True``.
        parallel : bool, optional
            Passed through to :func:`xarray.open_mfdataset` to parallelize per-file
            metadata parsing. Default ``False`` reproduces today's
            single-threaded behavior; the result is identical either way.
        concurrent_files : bool, optional
            For the multi-file custom ``read_method`` path, fan the per-file reads
            out with :func:`dask.delayed` instead of a serial comprehension,
            preserving file order. Default ``False`` is the serial path.
        defer : bool, optional
            Return an unevaluated, dask-backed dataset. Implies
            ``use_dask=True``; raises for a source declaring
            ``capabilities.deferred = False``.
        tolerate : str or bool, optional
            Explicit tolerance for skipped inputs (``"input"``): a failing file
            of several is skipped and recorded in provenance instead of failing
            the load. Never the default; with no tolerance the first failure
            raises. Requires opening files one at a time, so the multi-file
            standard path combines per-file datasets rather than delegating to
            :func:`xarray.open_mfdataset`.
        **kwargs : dict
            Additional arguments passed to xarray open functions.

        Returns
        -------
        xr.Dataset
            The loaded dataset.
        """
        from .sat_utils import update_history

        # Handle deprecated parameters
        if virtualizarr_backend == "icechunk":
            warnings.warn(
                "The 'virtualizarr_backend' parameter is deprecated. Use 'use_icechunk=True' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            use_icechunk = True
        if icechunk_repo is not None:
            warnings.warn(
                "The 'icechunk_repo' parameter is deprecated. Use 'icechunk_url' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            icechunk_url = icechunk_repo
            use_icechunk = True

        tolerance = Tolerance.parse(tolerate)
        if defer:
            self._require_defer_support()
            # defer wins over use_dask=False.
            use_dask = True

        # Validate backend selection
        if not use_icechunk and virtualizarr_backend not in ("kerchunk", "icechunk"):
            raise ValueError(
                f"Invalid virtualizarr_backend '{virtualizarr_backend}'. "
                "Must be 'kerchunk' or 'icechunk'."
            )

        # Prepare kwargs for xarray
        xr_kwargs = kwargs.copy()

        # Parallel metadata parsing. Only inject when requested so
        # the default path is byte-identical to before (xarray's own default is
        # False). Reaches xr.open_mfdataset; stripped for single-file/virtual paths.
        if parallel:
            xr_kwargs["parallel"] = True

        # Expand wildcards (supports S3 globbing now). storage_options (e.g.
        # {"anon": False}) reach the fsspec backend so credential problems
        # surface as themselves instead of an empty listing.
        file_list = FileUtility.expand_paths(
            files, storage_options=dict(xr_kwargs.get("storage_options", {})) or None
        )

        # Handle 'lazy' keyword: Eager by default. Explicit deferral is
        # authoritative even when a legacy alias is also supplied.
        if "lazy" in xr_kwargs:
            requested_lazy = xr_kwargs.pop("lazy")
            use_dask = bool(requested_lazy) or use_dask
        if defer:
            use_dask = True
        if use_dask and xr_kwargs.get("chunks", object()) is None:
            raise OptionUnsupportedError(
                "chunks=None",
                self.source or "source",
                cause="Dask-backed loading requires chunked array reads",
            )

        # Optional retry knobs for transient remote read failures.
        retry_attempts = int(xr_kwargs.pop("retry_attempts", 3))
        retry_base_sleep = float(xr_kwargs.pop("retry_base_sleep", 1.0))

        # If laziness or specific chunking is requested, ensure auto-chunking is used.
        if (use_dask or "chunks" in xr_kwargs) and "chunks" not in xr_kwargs:
            xr_kwargs["chunks"] = {}

        # Extract MONETIO-specific keywords
        preprocess = xr_kwargs.pop("preprocess", None)
        read_method = xr_kwargs.pop("read_method", None)

        # xarray silently ignores chunk mapping keys that do not match a
        # source dimension. Check each raw per-file dataset before any reader
        # preprocessing can rename its dimensions. The callback also covers
        # open_mfdataset and the native GRIB multi-file path.
        chunks = xr_kwargs.get("chunks")
        if isinstance(chunks, Mapping):
            user_preprocess = preprocess

            def preprocess_with_chunk_validation(ds):
                self._validate_chunk_dimensions(ds, chunks)
                return user_preprocess(ds) if user_preprocess else ds

            preprocess = preprocess_with_chunk_validation

        if xr_kwargs.get("engine") == "grib2io" and not use_virtualizarr:
            # Inject use_icechunk into xr_kwargs — grib2io's xarray backend
            # uses this to activate its icechunk-aware S3 path.
            xr_kwargs.setdefault("use_icechunk", use_icechunk)

            # VirtualiZarr-only concepts must not reach the native
            # grib2io/xarray backend kwargs. Note: use_icechunk IS a valid
            # grib2io backend kwarg and must be preserved.
            xr_kwargs.pop("use_virtualizarr", None)
            xr_kwargs.pop("virtualizarr_backend", None)
            xr_kwargs.pop("virtualizarr_file", None)
            xr_kwargs.pop("virtualizarr_parser", None)
            xr_kwargs.pop("icechunk_url", None)
            xr_kwargs.pop("icechunk_repo", None)

            ds = self._open_grib2io_native(
                file_list=file_list,
                xr_kwargs=xr_kwargs,
                preprocess=preprocess,
                retry_attempts=retry_attempts,
                retry_base_sleep=retry_base_sleep,
                icechunk_url=icechunk_url,
            )
            if use_dask or xr_kwargs.get("chunks") is not None:
                ds = _make_grib2io_graph_serializable(ds)
            return ds

        if use_virtualizarr:
            # Determine Parser
            parser_map = {
                "hdf5": "HDFParser",
                "netcdf3": "NetCDF3Parser",
                "zarr": "ZarrParser",
                "fits": "FITSParser",
                "dmrpp": "DMRPPParser",
                "grib2": "GRIB2Parser",
            }

            parser_name = virtualizarr_parser
            if parser_name is None:
                engine = xr_kwargs.get("engine", "")
                first_file = str(file_list[0]).lower() if file_list else ""
                looks_like_grib2 = any(
                    token in first_file for token in [".grib2", ".grb2", "pgrb2"]
                )
                if engine == "grib2io" or looks_like_grib2:
                    parser_name = "grib2"
                elif engine == "zarr":
                    parser_name = "zarr"
                else:
                    parser_name = "hdf5"

            if parser_name not in parser_map:
                raise ValueError(
                    f"Unsupported virtualizarr_parser '{parser_name}'. "
                    f"Choose one of {sorted(parser_map)}."
                )

            # In MONETIO, GRIB virtual references must be routed through grib2io.
            if parser_name == "grib2":
                try:
                    # Importing the submodule also imports its parent package, so
                    # a missing grib2io surfaces here as the same ImportError.
                    from grib2io.kerchunk import ReferenceGenerator
                except ImportError:
                    raise ImportError("grib2io is required for GRIB2 VirtualiZarr reading.")

                try:
                    import ujson
                    import zarr
                    from virtualizarr import open_virtual_dataset
                    from virtualizarr.parsers import KerchunkJSONParser
                except ImportError:
                    raise ImportError(
                        "VirtualiZarr support for GRIB2 requires virtualizarr, obstore, obspec_utils, ujson, and zarr."
                    )

                if xr_kwargs.get("engine") == "grib2io":
                    warnings.warn(
                        "For engine='grib2io', use_virtualizarr is redirected to the VirtualiZarr GRIB2 pipeline.",
                        DeprecationWarning,
                        stacklevel=2,
                    )

                # --- Kerchunk cache: load existing refs if available ---
                refs = None
                if (
                    not use_icechunk
                    and virtualizarr_file is not None
                    and os.path.exists(virtualizarr_file)
                ):
                    try:
                        with open(virtualizarr_file) as f_ref:
                            refs = ujson.load(f_ref)
                    except Exception as e:  # noqa: BLE001 - cached references are best-effort; an unreadable cache is discarded and recomputed below (categories: malformed, not_found)
                        warnings.warn(f"Failed to load virtualizarr_file {virtualizarr_file}: {e}")
                        refs = None
                    # Earlier VirtualiZarr releases dropped the GRIB2 decoder
                    # filter while converting this manifest to Kerchunk. Such
                    # a cache looks valid but exposes packed GRIB bytes as
                    # float chunks, so regenerate it from grib2io metadata.
                    if refs is not None:
                        has_grib_filter = any(
                            key.endswith("/.zarray") and '"id": "grib2io"' in json.dumps(value)
                            for key, value in refs.get("refs", {}).items()
                        )
                        if not has_grib_filter:
                            refs = None

                if refs is None:
                    # Pop GRIB2-specific parameters to avoid reaching xarray
                    filters = xr_kwargs.pop("filters", None)
                    max_workers = xr_kwargs.pop("max_workers", None)
                    storage_options = dict(xr_kwargs.get("storage_options", {}))

                    # 1. Generate the reference manifest using grib2io
                    gen = ReferenceGenerator(
                        file_paths=file_list,
                        filters=filters,
                        storage_options=storage_options,
                        max_workers=max_workers,
                    )
                    manifest = gen.generate()

                    # 2. Write manifest to virtualizarr_file or temporary JSON
                    manifest_path_str = None
                    if virtualizarr_file is not None:
                        manifest_path_str = virtualizarr_file
                    else:
                        import tempfile

                        fd, temp_path_str = tempfile.mkstemp(
                            suffix=".json", prefix="grib2_manifest_"
                        )
                        os.close(fd)
                        manifest_path_str = temp_path_str

                    try:
                        with open(manifest_path_str, "w") as f:
                            ujson.dump(manifest, f)

                        # 3. Resolve store registry and register LocalStore
                        registry, _ = _select_store(file_list, storage_options)
                        from obstore.store import LocalStore

                        registry.register("file:///", LocalStore(prefix="/"))

                        # 4. Open virtual dataset
                        import pathlib

                        manifest_file = pathlib.Path(manifest_path_str).resolve()
                        manifest_url = manifest_file.as_uri()

                        vds = _call_with_retries(
                            open_virtual_dataset,
                            url=manifest_url,
                            registry=registry,
                            parser=KerchunkJSONParser(),
                            loadable_variables=[],
                            attempts=retry_attempts,
                            base_sleep=retry_base_sleep,
                        )

                        # 5. Route to icechunk or kerchunk
                        if use_icechunk:
                            if chunks is None:
                                ds = _open_via_icechunk(vds, icechunk_url, virtualizarr_file)
                            else:
                                ds = _open_via_icechunk(
                                    vds, icechunk_url, virtualizarr_file, chunks=chunks
                                )
                            self._validate_chunk_dimensions(ds, chunks)
                            if preprocess:
                                ds = preprocess(ds)
                            return ds

                        # The original grib2io manifest carries the custom
                        # numcodecs GRIB2 filter. VirtualiZarr's Kerchunk
                        # export currently drops that filter and leaves only
                        # compressed GRIB message byte ranges.
                        refs = manifest

                        # Write refs to cache if requested
                        if virtualizarr_file is not None:
                            try:
                                with open(virtualizarr_file, "w") as f_ref:
                                    ujson.dump(refs, f_ref)
                            except Exception as e:
                                # The caller asked for the references at this
                                # path; a failed write must not pass as success
                                # (that is the silent-discard failure mode).
                                raise MalformedInputError(
                                    virtualizarr_file,
                                    f"could not write virtualizarr references: {e}",
                                ) from e

                    finally:
                        if virtualizarr_file is None and manifest_path_str is not None:
                            try:
                                os.remove(manifest_path_str)
                            except Exception:  # noqa: BLE001 - temporary manifest cleanup; the load already succeeded, so an undeletable temp file must not fail it (categories: permission_denied, not_found)
                                pass

                # --- Materialize Dataset via reference mapper ---
                remote_protocol = "file"
                remote_options = {}
                if file_list[0].startswith("s3://"):
                    remote_protocol = "s3"
                    remote_options = dict(xr_kwargs.get("storage_options", {}))
                    if "anon" not in remote_options:
                        remote_options["anon"] = True
                elif file_list[0].startswith("http"):
                    remote_protocol = "http"

                remote_options["asynchronous"] = True

                mapper = fsspec.get_mapper(
                    "reference://",
                    fo=refs,
                    remote_protocol=remote_protocol,
                    remote_options=remote_options,
                    asynchronous=True,
                )

                # Clean up open_mfdataset-only / virtualizarr kwargs
                mfdataset_keys = [
                    "combine",
                    "concat_dim",
                    "parallel",
                    "compat",
                    "data_vars",
                    "coords",
                    "ids",
                    "infer_order",
                    "join",
                ]
                virtualizarr_keys = [
                    "use_virtualizarr",
                    "virtualizarr_backend",
                    "virtualizarr_file",
                    "virtualizarr_parser",
                ]
                import itertools

                for key in itertools.chain(
                    mfdataset_keys, virtualizarr_keys, ["engine", "icechunk_url", "icechunk_repo"]
                ):
                    try:
                        del xr_kwargs[key]
                    except KeyError:
                        pass
                xr_kwargs.pop("use_icechunk", None)
                xr_kwargs.pop("store_path", None)
                xr_kwargs.pop("max_scan_attempts", None)
                xr_kwargs.pop("network_timeout", None)
                xr_kwargs.pop("max_concurrent_requests", None)
                xr_kwargs.pop("storage_options", None)
                xr_kwargs.pop("filters", None)
                xr_kwargs.pop("max_workers", None)

                ds = _call_with_retries(
                    xr.open_dataset,
                    mapper,
                    attempts=retry_attempts,
                    base_sleep=retry_base_sleep,
                    engine="zarr",
                    consolidated=False,
                    # grib2io's Kerchunk manifests are Zarr v2 metadata. Pin
                    # the reader here because Zarr 3 otherwise interprets the
                    # references through its v3 codec pipeline and skips the
                    # registered GRIB2 numcodecs filter.
                    zarr_format=2,
                    **xr_kwargs,
                )
                self._validate_chunk_dimensions(ds, chunks)
                return preprocess(ds) if preprocess else ds

            try:
                import ujson  # noqa: F401
                import zarr  # noqa: F401
                from virtualizarr import open_virtual_dataset, open_virtual_mfdataset
            except ImportError:
                raise ImportError(
                    "VirtualiZarr support requires additional packages. "
                    "Install with: pip install monetio[virtualizarr]"
                )

            try:
                import virtualizarr.parsers as parsers

                parser_cls_name = parser_map[parser_name]
                parser_cls = getattr(parsers, parser_cls_name)
                parser = parser_cls()
            except (ImportError, AttributeError) as e:
                if parser_name == "grib2":
                    raise ImportError(
                        "virtualizarr GRIB2 parser is unavailable. "
                        "Install/upgrade virtualizarr with GRIB2 parser support. "
                        "MONETIO will not fall back to non-GRIB parsers for GRIB data."
                    ) from e
                from virtualizarr.parsers import HDFParser

                parser = HDFParser()

            # --- Kerchunk cache: load existing refs if available ---
            refs = None
            if (
                not use_icechunk
                and virtualizarr_file is not None
                and os.path.exists(virtualizarr_file)
            ):
                try:
                    with open(virtualizarr_file) as f_ref:
                        refs = ujson.load(f_ref)
                except Exception as e:  # noqa: BLE001 - cache read is best-effort (malformed, content_invalid); recompute below
                    warnings.warn(f"Failed to load virtualizarr_file {virtualizarr_file}: {e}")
                    refs = None

            if refs is None:
                storage_options = dict(xr_kwargs.get("storage_options", {}))
                registry, file_list = _select_store(file_list, storage_options)

                concat_dim = xr_kwargs.get("concat_dim", "time")
                parallel_sweep = xr_kwargs.get("parallel", True)
                # virtualizarr>=2 expects 'dask'/'lithops'/Executor/False, not True.
                if parallel_sweep is True:
                    parallel_sweep = "dask"
                try:
                    if len(file_list) == 1:
                        vds = _call_with_retries(
                            open_virtual_dataset,
                            url=file_list[0],
                            attempts=retry_attempts,
                            base_sleep=retry_base_sleep,
                            registry=registry,
                            parser=parser,
                            loadable_variables=[],
                        )
                    else:
                        vds = _call_with_retries(
                            open_virtual_mfdataset,
                            file_list,
                            attempts=retry_attempts,
                            base_sleep=retry_base_sleep,
                            registry=registry,
                            parser=parser,
                            combine="nested",
                            concat_dim=concat_dim,
                            coords=xr_kwargs.get("coords", "minimal"),
                            compat=xr_kwargs.get("compat", "override"),
                            data_vars=xr_kwargs.get("data_vars", "all"),
                            parallel=parallel_sweep,
                            loadable_variables=[],
                        )
                except ValueError:
                    vds = _call_with_retries(
                        open_virtual_mfdataset,
                        file_list,
                        attempts=retry_attempts,
                        base_sleep=retry_base_sleep,
                        registry=registry,
                        parser=parser,
                        combine="by_coords",
                        parallel=parallel_sweep,
                        loadable_variables=[],
                    )

                # --- Branch on backend ---
                if use_icechunk:
                    if chunks is None:
                        ds = _open_via_icechunk(vds, icechunk_url, virtualizarr_file)
                    else:
                        ds = _open_via_icechunk(vds, icechunk_url, virtualizarr_file, chunks=chunks)
                    self._validate_chunk_dimensions(ds, chunks)
                    if preprocess:
                        ds = preprocess(ds)
                    return ds

                # Kerchunk path: export refs and optionally cache them
                refs = vds.vz.to_kerchunk()

                if virtualizarr_file is not None:
                    try:
                        with open(virtualizarr_file, "w") as f_ref:
                            ujson.dump(refs, f_ref)
                    except Exception as e:
                        # The caller asked for the references at this path; a
                        # failed write must not pass as success (that is the
                        # silent-discard failure mode). categories: malformed,
                        # permission_denied, not_found.
                        raise MalformedInputError(
                            virtualizarr_file,
                            f"could not write virtualizarr references: {e}",
                        ) from e

            remote_protocol = "file"
            remote_options = {}
            if file_list[0].startswith("s3://"):
                remote_protocol = "s3"
                remote_options = dict(xr_kwargs.get("storage_options", {}))
                if "anon" not in remote_options:
                    remote_options["anon"] = True
            elif file_list[0].startswith("http"):
                remote_protocol = "http"
                # file_list for fsspec mapper should not start with file:// if they are local
            elif file_list[0].startswith("file://"):
                pass

            mapper = fsspec.get_mapper(
                "reference://",
                fo=refs,
                remote_protocol=remote_protocol,
                remote_options=remote_options,
            )

            # Clean up xr_kwargs for open_dataset
            mfdataset_keys = [
                "combine",
                "concat_dim",
                "parallel",
                "compat",
                "data_vars",
                "coords",
                "ids",
                "infer_order",
                "join",
                "engine",
                "backend_kwargs",
                "storage_options",
            ]
            for k in mfdataset_keys:
                xr_kwargs.pop(k, None)

            ds = _call_with_retries(
                xr.open_dataset,
                mapper,
                attempts=retry_attempts,
                base_sleep=retry_base_sleep,
                engine="zarr",
                backend_kwargs={"consolidated": False},
                consolidated=False,
                **xr_kwargs,
            )

            self._validate_chunk_dimensions(ds, chunks)
            if preprocess:
                ds = preprocess(ds)

            return ds

        try:
            # Case A: Single File (Optimized)
            if len(file_list) == 1:
                ds = self._open_single_file(file_list[0], xr_kwargs, read_method, preprocess)
                return _validate_nonempty_result(ds, file_list, self.source)

            # Case B: Multiple Files (dataset)
            else:
                if read_method:
                    # Custom read_method path (e.g. TOLNet)
                    if concurrent_files:
                        # Fan out per-file reads with dask.delayed, mirroring
                        # PandasDriver's lazy path. dask.compute
                        # returns results in submission order, so file order —
                        # which combine_nested/concat depend on — is preserved.
                        import dask

                        delayed_ds = [dask.delayed(read_method)(f, **xr_kwargs) for f in file_list]
                        dsets = list(dask.compute(*delayed_ds))
                    else:
                        dsets = [read_method(f, **xr_kwargs) for f in file_list]

                    if preprocess:
                        dsets = [preprocess(ds) for ds in dsets]

                    # Combine logic (backend-agnostic)
                    concat_dim = xr_kwargs.get("concat_dim")
                    if concat_dim is not None:
                        # If a explicit dimension is given, we use nested combination
                        # or direct concatenation if nested combine fails.
                        try:
                            ds = xr.combine_nested(
                                dsets,
                                concat_dim=concat_dim,
                                data_vars="minimal",
                                coords="minimal",
                                compat="override",
                            )
                        except (ValueError, TypeError):
                            ds = xr.concat(
                                dsets, dim=concat_dim, coords="different", data_vars="minimal"
                            )
                    else:
                        try:
                            ds = xr.combine_by_coords(
                                dsets,
                                data_vars="minimal",
                                coords="minimal",
                                compat="override",
                            )
                        except (ValueError, TypeError):
                            # Fallback to concat if combine_by_coords fails.
                            ds = xr.concat(
                                dsets, dim="time", coords="different", data_vars="minimal"
                            )
                    return _validate_nonempty_result(ds, file_list, self.source)

                # Tolerated multi-file loads open one input at a time so a single
                # bad file can be skipped and recorded; xarray's own multi-file
                # open cannot report which file failed inside a combine error.
                if tolerance and tolerance.scope == "input":

                    def _open_one(f):
                        return self._open_single_file(f, xr_kwargs, None, preprocess)

                    dsets, record = _tolerant_per_file(file_list, _open_one, tolerance)
                    ds = self._combine_many(dsets, xr_kwargs)
                    line = format_tolerance_record(record, requested=True)
                    if line:
                        ds = update_history(ds, line)
                    return _validate_nonempty_result(ds, file_list, self.source)

                # Standard path: use xr.open_mfdataset
                if preprocess:
                    xr_kwargs["preprocess"] = preprocess

                # If concat_dim is provided, ensure we use nested combine to avoid xarray errors
                if "concat_dim" in xr_kwargs and "combine" not in xr_kwargs:
                    xr_kwargs["combine"] = "nested"

                # Use native grib2io file-like support for remote files.
                engine = xr_kwargs.get("engine", None)
                if (
                    engine == "grib2io"
                    and isinstance(file_list, list)
                    and file_list
                    and (file_list[0].startswith("s3://") or file_list[0].startswith("http"))
                ):
                    storage_options = xr_kwargs.pop("storage_options", {})
                    file_objs = []
                    for f in file_list:
                        fs = FileUtility.get_fs(f, **storage_options)
                        file_objs.append(fs.open(f, mode="rb"))
                    file_list = file_objs

                if "engine" not in xr_kwargs:
                    try:
                        ds = _open_mfdataset(file_list, xr_kwargs, engine="h5netcdf")
                    except Exception as first:  # noqa: BLE001
                        if not _engine_not_applicable(first):
                            raise
                        ds = _open_mfdataset(file_list, xr_kwargs)
                else:
                    ds = _open_mfdataset(file_list, xr_kwargs)

                # xarray.open_mfdataset always returns Dask-backed variables,
                # even when the caller requested the default eager path and
                # supplied no chunks. Materialize that implicit Dask graph and
                # release its backend handles before returning. An explicit
                # chunks value or use_dask=True keeps the lazy backend result.
                if not use_dask and xr_kwargs.get("chunks") is None:
                    ds.load()
                    ds.close()
                return _validate_nonempty_result(ds, file_list, self.source)

        except Exception as e:
            # Preserve the original exception type and name the files,
            # instead of flattening everything into a generic OSError.
            raise _reject_unsupported_options(_annotate_files(e, file_list), self.source) from e

    def _open_single_file(self, filename, xr_kwargs, read_method, preprocess) -> xr.Dataset:
        """Open (and preprocess) exactly one input, without combining.

        The per-file half of the multi-file path: used for a single-file load,
        for tolerated loads (where a failure must be attributable to one file),
        and by readers that combine datasets themselves.
        """
        # Remove open_mfdataset specific arguments to prevent TypeError in xr.open_dataset
        xr_kwargs = {
            k: v
            for k, v in xr_kwargs.items()
            if k
            not in (
                "combine",
                "concat_dim",
                "parallel",
                "compat",
                "data_vars",
                "coords",
                "ids",
                "infer_order",
                "join",
            )
        }

        if read_method:
            ds = read_method(filename, **xr_kwargs)
        else:
            # Logic for standard engine/remote access
            if _path_protocol(filename) and not filename.startswith("file://"):
                if xr_kwargs.get("engine") == "grib2io":
                    # Use native grib2io file-like support for remote files:
                    # the engine reads the binary stream itself, so the store
                    # options are consumed here rather than forwarded to
                    # open_dataset alongside an already-opened file object.
                    storage_options = xr_kwargs.pop("storage_options", None)
                    fs = FileUtility.get_fs(filename, storage_options)
                    file_obj = fs.open(filename, mode="rb")
                else:
                    fs = FileUtility.get_fs(filename, xr_kwargs.get("storage_options"))
                    file_obj = fs.open(filename)
            else:
                file_obj = filename

            if "engine" not in xr_kwargs:
                try:
                    ds = xr.open_dataset(file_obj, engine="h5netcdf", **xr_kwargs)
                except Exception as first:  # noqa: BLE001
                    # Only fall back when the preferred engine genuinely
                    # cannot read this format; a real read error must not
                    # be masked by a second, misleading attempt.
                    if not _engine_not_applicable(first):
                        raise
                    ds = xr.open_dataset(file_obj, **xr_kwargs)
            else:
                ds = xr.open_dataset(file_obj, **xr_kwargs)

        # Apply preprocess manually
        ds = self._validate_chunk_dimensions(ds, xr_kwargs.get("chunks"))
        if preprocess:
            ds = preprocess(ds)

        return ds

    @staticmethod
    def _combine_many(dsets, xr_kwargs) -> xr.Dataset:
        """Combine per-file datasets with the same logic the multi-file paths use."""
        concat_dim = xr_kwargs.get("concat_dim")
        if concat_dim is not None:
            try:
                return xr.combine_nested(
                    dsets,
                    concat_dim=concat_dim,
                    data_vars="minimal",
                    coords="minimal",
                    compat="override",
                )
            except (ValueError, TypeError):
                return xr.concat(dsets, dim=concat_dim, coords="different", data_vars="minimal")
        try:
            return xr.combine_by_coords(
                dsets, data_vars="minimal", coords="minimal", compat="override"
            )
        except (ValueError, TypeError):
            return xr.concat(dsets, dim="time", coords="different", data_vars="minimal")

    def to_kerchunk(self, files: str | list[str], virtualizarr_file: str | None = None, **kwargs):
        """Generate Kerchunk references for the given files."""
        kwargs["use_virtualizarr"] = True
        kwargs["use_icechunk"] = False
        kwargs["virtualizarr_file"] = virtualizarr_file
        return self.open(files, **kwargs)

    def to_icechunk(self, files: str | list[str], icechunk_url: str, **kwargs):
        """Generate Icechunk references for the given files."""
        kwargs["use_virtualizarr"] = True
        kwargs["use_icechunk"] = True
        kwargs["icechunk_url"] = icechunk_url
        return self.open(files, **kwargs)


class PandasDriver:
    """
    The unified driver for opening tabular/point data.
    """

    def __init__(self, source: str | None = None):
        #: Manifest source name; see :attr:`XarrayDriver.source`.
        self.source = source

    def _require_defer_support(self) -> None:
        """Keep direct driver calls aligned with the source capability manifest."""
        from .._manifest import resolve
        from .errors import CapabilityUnavailableError

        if self.source is None:
            return
        try:
            spec = resolve(self.source)
        except KeyError:
            return
        if not spec.capabilities.deferred:
            reason = spec.capabilities.reason or "no reason recorded"
            raise CapabilityUnavailableError(spec.name, "defer", reason)

    def open(
        self,
        files: str | list[str],
        read_method: str | Callable = "read_csv",
        lazy: bool = False,
        defer: bool = False,
        meta: pd.DataFrame | pd.Series | dict | tuple | None = None,
        tolerate=None,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        as_xarray: bool = False,
        **kwargs,
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """Open tabular inputs.

        ``tolerate`` (``"input"``) applies to the eager path only: the lazy path
        defers reading to Dask, where a per-file failure surfaces at compute
        time and cannot be attributed to one file here. Tolerating a lazy load
        would record skips that never happened, so the request is rejected
        rather than quietly ignored. Non-None array ``chunks`` mappings are
        rejected because each input file becomes one complete dataframe
        partition; use multiple source files to add partitions.
        """
        from .sat_utils import update_history

        if kwargs.get("chunks") is not None:
            raise OptionUnsupportedError(
                "chunks",
                self.source or "point reader",
                cause=(
                    "tabular readers parse each complete input file as one dataframe "
                    "partition; array chunk mappings cannot control those reads"
                ),
            )
        # `chunks=None` has no meaning for the whole-file pandas partition path.
        kwargs.pop("chunks", None)

        # ``defer`` is an explicit promise about when payload reads begin.
        # Resolve aliases first, then ensure they cannot undo deferral.
        if use_dask:
            lazy = True
        if defer:
            self._require_defer_support()
            lazy = True
            if meta is None:
                raise OptionUnsupportedError(
                    "defer without explicit schema meta",
                    self.source or "source",
                    cause=(
                        "Dask would otherwise evaluate the first input to infer "
                        "the dataframe schema; supply post-preprocessing meta"
                    ),
                )

        tolerance = Tolerance.parse(tolerate)
        file_list = FileUtility.expand_paths(
            files, storage_options=dict(kwargs.get("storage_options", {})) or None
        )

        # Get the actual reading function
        if callable(read_method):
            reader_func = read_method
        elif hasattr(pd, read_method):
            reader_func = getattr(pd, read_method)
        else:
            raise ValueError(f"Pandas method '{read_method}' not found and not callable.")

        if lazy:
            import dask
            import dask.dataframe as dd

            if tolerance:
                raise OptionUnsupportedError(
                    "tolerate",
                    self.source or "source",
                    cause=(
                        "a lazy load defers reading to Dask, so a failing input "
                        "cannot be identified and recorded here; use the eager "
                        "path (lazy=False) to tolerate bad inputs"
                    ),
                )

            # Extract preprocess if present
            preprocess = kwargs.pop("preprocess", None)

            delayed_dfs = []
            for f in file_list:
                if f.startswith("s3://"):
                    if "storage_options" not in kwargs:
                        kwargs["storage_options"] = {"anon": True}

                d = dask.delayed(_read_table_partition, pure=False)(
                    reader_func, f, dict(kwargs), preprocess, self.source, meta
                )
                delayed_dfs.append(d)

            if not delayed_dfs:
                return dd.from_pandas(pd.DataFrame(), npartitions=1)

            # Keep the pandas parser's string dtypes intact. Dask's global
            # convert-string default otherwise silently changes object columns
            # to PyArrow strings, breaking eager/lazy dtype parity for readers.
            # The context is local to graph construction and restores the
            # caller's Dask configuration immediately afterward.
            with dask.config.set({"dataframe.convert-string": False}):
                return dd.from_delayed(delayed_dfs, meta=meta)

        data_frames = []
        # Reuse our filesystem logic
        try:
            # Extract preprocess if present
            preprocess = kwargs.pop("preprocess", None)

            def _read_one(f):
                if f.startswith("s3://"):
                    # Pandas can read S3 URLs directly if s3fs is installed!
                    per_file = kwargs
                    if "storage_options" not in per_file:
                        per_file = {**per_file, "storage_options": {"anon": True}}
                else:
                    per_file = kwargs
                df = reader_func(f, **per_file)
                if preprocess:
                    df = preprocess(df)
                return df

            if tolerance and tolerance.scope == "input":
                # One input at a time so a failure is attributable to one file
                # and can be skipped and recorded rather than failing the load.
                data_frames, record = _tolerant_per_file(file_list, _read_one, tolerance)
                if not data_frames:
                    return pd.DataFrame()
                df = pd.concat(data_frames, ignore_index=True)
                line = format_tolerance_record(record, requested=True)
                if line:
                    df = update_history(df, line)
                return _validate_nonempty_result(df, file_list, self.source)

            # Use concurrent downloads for S3 files when there are many
            if len(file_list) > 5 and file_list[0].startswith("s3://"):
                import concurrent.futures

                max_workers = min(32, len(file_list))
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(_read_one, f): f for f in file_list}
                    # Collect by submission order, not completion order, so the
                    # concatenated rows do not depend on which request finished
                    # first.
                    #
                    # A failure is NOT absorbed here. Skipping a missing input
                    # would grant tolerance that the caller never asked for, and
                    # the skip would be invisible in the result; tolerated loads
                    # take the branch above, which records every skip in
                    # provenance.
                    for future in futures:
                        data_frames.append(future.result())
            else:
                for f in file_list:
                    data_frames.append(_read_one(f))

            if not data_frames:
                return pd.DataFrame()

            df = pd.concat(data_frames, ignore_index=True)
            return _validate_nonempty_result(df, file_list, self.source)

        except (RuntimeError, ValueError, MalformedInputError):
            raise
        except Exception as e:
            # Preserve the original exception type and name the files,
            # instead of flattening everything into a generic OSError.
            raise _reject_unsupported_options(annotate(e, *file_list), self.source) from e
