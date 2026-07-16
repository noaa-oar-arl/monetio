import os
import time
import warnings
from collections.abc import Callable
from typing import Union

import fsspec
import pandas as pd
import xarray as xr

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
        store = LocalStore(prefix="/")
        registry.register("file:///", store)
        file_list = [f"file://{f}" if not f.startswith("file://") else f for f in file_list]

    return registry, file_list


def _open_via_icechunk(vds, icechunk_url: str, virtualizarr_file: str | None) -> xr.Dataset:
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
        except Exception:
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

    config = icechunk.RepositoryConfig.default()
    for prefix in unique_prefixes:
        if prefix.startswith("s3://"):
            store_conf = icechunk.s3_store(anonymous=True, region="us-east-1")
        elif prefix.startswith("http://") or prefix.startswith("https://"):
            store_conf = icechunk.http_store()
        else:
            store_conf = icechunk.local_filesystem_store()
        
        container = icechunk.VirtualChunkContainer(
            url_prefix=prefix,
            store=store_conf,
        )
        config.set_virtual_chunk_container(container)

    authorize = {prefix: None for prefix in unique_prefixes}

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
    return xr.open_zarr(session.store, consolidated=False)


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
        except Exception as exc:
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
    def get_fs(path: str, **kwargs):
        """
        Returns the correct filesystem (local, s3, or http) based on the protocol.

        Parameters
        ----------
        path : str
            File path or URL.
        **kwargs : dict
            Additional arguments passed to fsspec.filesystem.
        """
        if path.startswith("s3://"):
            # Default to anonymous access for S3 if not specified
            if "anon" not in kwargs and "storage_options" not in kwargs:
                kwargs.update(get_default_storage_options(path))

            return fsspec.filesystem("s3", **kwargs)
        elif path.startswith("http://") or path.startswith("https://"):
            return fsspec.filesystem("http", **kwargs)
        elif path.startswith("ftp://"):
            return fsspec.filesystem("ftp", **kwargs)
        elif path.startswith("file://"):
            return fsspec.filesystem("file", **kwargs)
        return fsspec.filesystem("file", **kwargs)

    @staticmethod
    def expand_paths(path_input: str | list[str], fs=None, **kwargs) -> list[str]:
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
                fs = FileUtility.get_fs(path_input, **kwargs)

            # Use fsspec/s3fs to glob wildcards (works for s3://bucket/data/*.nc too!)
            if any(char in path_input for char in ["*", "?"]):
                # HTTP globbing is generally not supported by fsspec without specific implementation
                # For S3/Local it works.
                if path_input.startswith("http"):
                    # Fallback: treat as single file if glob chars present but http (unlikely to work)
                    pass

                files = sorted(fs.glob(path_input))

                # Ensure paths have the protocol if the input had it
                if not files:
                    raise FileNotFoundError(f"No files found matching pattern: {path_input}")

                # fsspec.unstrip_protocol is the standard way to restore the protocol
                # but it might not be available or consistent across all versions/fs.
                # Manual fix for common cases in monetio:
                protocol = ""
                possible_protocol, _host_path = path_input.split("://", 1)
                if possible_protocol in ("s3", "http", "https", "ftp"):
                    protocol = f"{possible_protocol}://"

                if protocol and not str(files[0]).startswith(protocol):
                    files = [
                        f"{protocol}{f}" if not str(f).startswith(protocol) else f for f in files
                    ]

                return files
            else:
                # It is a specific single file
                if not path_input.startswith("http") and not fs.exists(path_input):
                    raise FileNotFoundError(f"File not found: {path_input}")
                return [path_input]

        raise TypeError(f"Invalid path type: {type(path_input)}. Must be str or list.")


class XarrayDriver:
    """
    The unified driver for opening gridded data (NetCDF, GRIB, HDF).
    Supports S3 via obstore or fsspec.
    """

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
        for key in ["use_icechunk", "max_workers", "network_timeout", "max_concurrent_requests", "max_scan_attempts", "store_path", "icechunk_url", "icechunk_repo"]:
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

                return _call_with_retries(
                    xr.open_dataset,
                    file_list[0],
                    attempts=retry_attempts,
                    base_sleep=retry_base_sleep,
                    **xr_kwargs,
                )

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
        use_cubed: bool = False,
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
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
        use_cubed : bool, optional
            Whether to use Cubed for lazy loading, by default False.
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
            Path to the Icechunk repository. Required when ``virtualizarr_backend="icechunk"``.
            Note: This parameter is deprecated in favor of `icechunk_url`.
        use_icechunk : bool, optional
            Whether to use Icechunk as the storage backend for VirtualiZarr references,
            by default False. If True, references are stored in an Icechunk repository.
            If False, the default Kerchunk JSON format is used.
        icechunk_url : str, optional
            Path or URL to the Icechunk repository. Required when ``use_icechunk=True``.
        **kwargs : dict
            Additional arguments passed to xarray open functions.

        Returns
        -------
        xr.Dataset
            The loaded dataset.
        """
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

        # Validate backend selection
        if not use_icechunk and virtualizarr_backend not in ("kerchunk", "icechunk"):
            raise ValueError(
                f"Invalid virtualizarr_backend '{virtualizarr_backend}'. "
                "Must be 'kerchunk' or 'icechunk'."
            )

        # Prepare kwargs for xarray
        xr_kwargs = kwargs.copy()

        if use_cubed:
            try:
                import cubed  # noqa: F401
                import cubed_xarray  # noqa: F401
            except ImportError:
                raise ImportError(
                    "The 'cubed' backend requires 'cubed' and 'cubed-xarray'. "
                    "Install with `pip install cubed cubed-xarray`."
                )
            xr_kwargs["chunked_array_type"] = "cubed"

        # Expand wildcards (supports S3 globbing now)
        file_list = FileUtility.expand_paths(files)

        # Handle 'lazy' keyword: Eager by default.
        if "lazy" in xr_kwargs:
            use_dask = xr_kwargs.pop("lazy")

        # Optional retry knobs for transient remote read failures.
        retry_attempts = int(xr_kwargs.pop("retry_attempts", 3))
        retry_base_sleep = float(xr_kwargs.pop("retry_base_sleep", 1.0))

        # If laziness or specific chunking is requested, ensure auto-chunking is used.
        if (use_dask or use_cubed or "chunks" in xr_kwargs) and "chunks" not in xr_kwargs:
            xr_kwargs["chunks"] = {}

        # Extract MONETIO-specific keywords
        preprocess = xr_kwargs.pop("preprocess", None)
        read_method = xr_kwargs.pop("read_method", None)

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

            return self._open_grib2io_native(
                file_list=file_list,
                xr_kwargs=xr_kwargs,
                preprocess=preprocess,
                retry_attempts=retry_attempts,
                retry_base_sleep=retry_base_sleep,
                icechunk_url=icechunk_url,
            )

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
                    import grib2io
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
                    except Exception as e:
                        warnings.warn(f"Failed to load virtualizarr_file {virtualizarr_file}: {e}")
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
                        fd, temp_path_str = tempfile.mkstemp(suffix=".json", prefix="grib2_manifest_")
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
                            ds = _open_via_icechunk(vds, icechunk_url, virtualizarr_file)
                            if preprocess:
                                ds = preprocess(ds)
                            return ds

                        refs = vds.vz.to_kerchunk()

                        # Write refs to cache if requested
                        if virtualizarr_file is not None:
                            try:
                                with open(virtualizarr_file, "w") as f_ref:
                                    ujson.dump(refs, f_ref)
                            except Exception as e:
                                warnings.warn(f"Failed to save virtualizarr_file {virtualizarr_file}: {e}")

                    finally:
                        if virtualizarr_file is None and manifest_path_str is not None:
                            try:
                                os.remove(manifest_path_str)
                            except Exception:
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

                return _call_with_retries(
                    xr.open_dataset,
                    mapper,
                    attempts=retry_attempts,
                    base_sleep=retry_base_sleep,
                    engine="zarr",
                    consolidated=False,
                    **xr_kwargs,
                )

            try:
                import ujson  # noqa: F401
                import zarr  # noqa: F401
                from virtualizarr import open_virtual_mfdataset
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
                except Exception as e:
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
                    vds = _call_with_retries(
                        open_virtual_mfdataset,
                        file_list,
                        attempts=retry_attempts,
                        base_sleep=retry_base_sleep,
                        registry=registry,
                        parser=parser,
                        combine="nested",
                        concat_dim=concat_dim,
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
                    ds = _open_via_icechunk(vds, icechunk_url, virtualizarr_file)
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
                        warnings.warn(f"Failed to save virtualizarr_file {virtualizarr_file}: {e}")

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

            if preprocess:
                ds = preprocess(ds)

            return ds

        try:
            # Case A: Single File (Optimized)
            if len(file_list) == 1:
                filename = file_list[0]

                # Remove open_mfdataset specific arguments to prevent TypeError in xr.open_dataset
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
                for k in mfdataset_keys:
                    xr_kwargs.pop(k, None)

                if read_method:
                    ds = read_method(filename, **xr_kwargs)
                else:
                    # Logic for standard engine/remote access
                    if filename.startswith("s3://") or filename.startswith("http"):
                        engine = xr_kwargs.get("engine", None)
                        if engine == "grib2io":
                            # Use native grib2io file-like support for remote files.
                            storage_options = xr_kwargs.pop("storage_options", {})
                            fs = FileUtility.get_fs(filename, **storage_options)
                            file_obj = fs.open(filename, mode="rb")
                        else:
                            storage_options = xr_kwargs.get("storage_options", {})
                            fs = FileUtility.get_fs(filename, **storage_options)
                            file_obj = fs.open(filename)
                    else:
                        file_obj = filename

                    if "engine" not in xr_kwargs:
                        try:
                            ds = xr.open_dataset(file_obj, engine="h5netcdf", **xr_kwargs)
                        except Exception:
                            ds = xr.open_dataset(file_obj, **xr_kwargs)
                    else:
                        ds = xr.open_dataset(file_obj, **xr_kwargs)

                # Apply preprocess manually
                if preprocess:
                    ds = preprocess(ds)

                return ds

            # Case B: Multiple Files (dataset)
            else:
                if read_method:
                    # Custom read_method path (e.g. TOLNet)
                    dsets = [read_method(f, **xr_kwargs) for f in file_list]

                    if preprocess:
                        dsets = [preprocess(ds) for ds in dsets]

                    # Combine logic (backend-agnostic)
                    concat_dim = xr_kwargs.get("concat_dim")
                    if concat_dim is not None:
                        # If a explicit dimension is given, we use nested combination
                        # or direct concatenation if nested combine fails.
                        try:
                            return xr.combine_nested(
                                dsets,
                                concat_dim=concat_dim,
                                data_vars="minimal",
                                coords="minimal",
                                compat="override",
                            )
                        except (ValueError, TypeError):
                            return xr.concat(
                                dsets, dim=concat_dim, coords="different", data_vars="minimal"
                            )
                    else:
                        try:
                            return xr.combine_by_coords(
                                dsets,
                                data_vars="minimal",
                                coords="minimal",
                                compat="override",
                            )
                        except (ValueError, TypeError):
                            # Fallback to concat if combine_by_coords fails.
                            return xr.concat(
                                dsets, dim="time", coords="different", data_vars="minimal"
                            )

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
                        return xr.open_mfdataset(file_list, engine="h5netcdf", **xr_kwargs)
                    except Exception:
                        return xr.open_mfdataset(file_list, **xr_kwargs)
                else:
                    return xr.open_mfdataset(file_list, **xr_kwargs)

        except Exception as e:
            raise OSError(f"XarrayDriver failed to open files. Error: {e}") from e

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

    def open(
        self,
        files: str | list[str],
        read_method: str | Callable = "read_csv",
        lazy: bool = False,
        meta: pd.DataFrame | pd.Series | dict | tuple | None = None,
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
        # Handle 'use_dask' as alias for 'lazy'
        if use_dask:
            lazy = True

        file_list = FileUtility.expand_paths(files)

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

            # Extract preprocess if present
            preprocess = kwargs.pop("preprocess", None)

            delayed_dfs = []
            for f in file_list:
                if f.startswith("s3://"):
                    if "storage_options" not in kwargs:
                        kwargs["storage_options"] = {"anon": True}

                d = dask.delayed(reader_func)(f, **kwargs)
                if preprocess:
                    d = dask.delayed(preprocess)(d)
                delayed_dfs.append(d)

            if not delayed_dfs:
                return dd.from_pandas(pd.DataFrame(), npartitions=1)

            return dd.from_delayed(delayed_dfs, meta=meta)

        data_frames = []
        # Reuse our filesystem logic
        try:
            # Extract preprocess if present
            preprocess = kwargs.pop("preprocess", None)
            _skipped = 0

            # Use concurrent downloads for S3 files when there are many
            if len(file_list) > 5 and file_list[0].startswith("s3://"):
                import concurrent.futures

                if "storage_options" not in kwargs:
                    kwargs["storage_options"] = {"anon": True}

                def _read_one(f):
                    df = reader_func(f, **kwargs)
                    if preprocess:
                        df = preprocess(df)
                    return df

                max_workers = min(32, len(file_list))
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(_read_one, f): f for f in file_list}
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            data_frames.append(future.result())
                        except FileNotFoundError:
                            _skipped += 1
                        except Exception:
                            _skipped += 1

                if _skipped > 0:
                    warnings.warn(
                        f"PandasDriver: Skipped {_skipped} missing/failed files "
                        f"out of {len(file_list)} total.",
                        stacklevel=2,
                    )
            else:
                for f in file_list:
                    try:
                        if f.startswith("s3://"):
                            if "storage_options" not in kwargs:
                                kwargs["storage_options"] = {"anon": True}
                            df = reader_func(f, **kwargs)
                        else:
                            df = reader_func(f, **kwargs)

                        if preprocess:
                            df = preprocess(df)
                        data_frames.append(df)
                    except FileNotFoundError:
                        _skipped += 1
                        continue

                if _skipped > 0:
                    warnings.warn(
                        f"PandasDriver: Skipped {_skipped} missing files "
                        f"out of {len(file_list)} total.",
                        stacklevel=2,
                    )

            if not data_frames:
                return pd.DataFrame()

            return pd.concat(data_frames, ignore_index=True)

        except (RuntimeError, ValueError):
            raise
        except Exception as e:
            raise OSError(f"PandasDriver failed to open files. Error: {e}") from e
