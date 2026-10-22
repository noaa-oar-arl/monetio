import abc
import inspect
import threading
from functools import wraps
from typing import TYPE_CHECKING, NamedTuple, Union

import numpy as np
import pandas as pd
import xarray as xr

from .._manifest import resolve
from ..util import ds_to_2d, force_object_strings
from .errors import (
    CapabilityUnavailableError,
    OptionUnsupportedError,
    ToleranceRecord,
    format_tolerance_record,
)
from .sat_utils import update_history

if TYPE_CHECKING:
    import dask.dataframe as dd

from .drivers import PandasDriver, XarrayDriver, _validate_nonempty_result


def _partition_column(part, col):
    """Extract one column from a delayed partition frame as a numpy array.

    Used by the deferred wide path so each output array is built from a
    pre-materialized partition list instead of re-optimizing the DataFrame
    graph once per column. Module-level (not a lambda) so the delayed task
    stays picklable for distributed schedulers.
    """
    return np.asarray(part[col])


def _partition_layout(part, columns):
    """Return partition length and the small row keys needed for Xarray layout.

    Deferred table-to-Xarray conversion needs concrete index coordinates for
    the UGRID node layout. Gather all of those keys together with each
    partition length so the reader parses a partition once during preparation
    instead of once for the length and again for each index coordinate.
    """
    return len(part), {name: part[name].to_numpy(copy=True) for name in columns}


def _is_dask_frame(df) -> bool:
    """True for a dask DataFrame (import-guarded; dask is an optional extra)."""
    try:
        import dask.dataframe as dd
    except ImportError:  # pragma: no cover - dask installed in dev env
        return False
    return isinstance(df, dd.DataFrame)


# 1. Diagnostic Specification
class DiagnosticSpec(NamedTuple):
    """Specification for a derived diagnostic variable."""

    variables: list[str]
    weights: list[float] | None = None
    units: str = "unknown"
    long_name: str = "unknown"
    name: str = "unknown"


# 2. The Registry
READER_REGISTRY = {}
# Guards the duplicate-check + insert so concurrent first-use of two sources
# cannot both pass the check before either writes.
_REGISTRATION_LOCK = threading.Lock()


def register_reader(name):
    """Decorator to register a reader class, validated against the manifest.

    The manifest (``monetio/_manifest.py``)
    is authoritative, so at decoration time this MUST

    1. reject a ``name`` not declared in the manifest (undeclared source);
    2. reject a *different* class already holding ``name`` (duplicate claim),
       naming both claimants — replacing today's silent last-import-wins;
    3. reject a base class that disagrees with the declared ``kind``.

    The dependency arrow is ``readers/base.py -> _manifest.py`` only; the
    manifest never imports readers.
    """

    def _register(cls):
        try:
            spec = resolve(name)
        except KeyError:
            raise ValueError(
                f"register_reader({name!r}): {name!r} is not declared in "
                "monetio._manifest.SOURCE_MANIFEST. Add a SourceSpec there "
                "(single source of truth)."
            ) from None

        expected = GriddedReader if spec.kind == "gridded" else PointReader
        if not issubclass(cls, expected):
            raise TypeError(
                f"register_reader({name!r}): class {cls.__name__} "
                f"({cls.__module__}) does not subclass {expected.__name__}, "
                f"but the manifest declares kind={spec.kind!r}."
            )

        with _REGISTRATION_LOCK:
            existing = READER_REGISTRY.get(name)
            if existing is not None and existing is not cls:
                raise ValueError(
                    f"Duplicate registration of source {name!r}: "
                    f"{existing.__module__}.{existing.__name__} is already "
                    f"registered, and {cls.__module__}.{cls.__name__} claims "
                    "the same name (last-import-wins is disallowed)."
                )
            # Guard the public dispatch boundary as well as the shared
            # drivers. A few registered readers call xarray.open_dataset
            # directly, so their ``defer`` keyword would otherwise leak into
            # the backend and fail with an unrelated TypeError (or be ignored).
            original_open = cls.open_dataset
            signature = inspect.signature(original_open)

            @wraps(original_open)
            def _capability_guard(self, *args, **kwargs):
                try:
                    bound = signature.bind_partial(self, *args, **kwargs)
                except TypeError:
                    # Preserve the reader's original argument error.
                    return original_open(self, *args, **kwargs)
                defer = bound.arguments.get("defer", False)
                requested_chunks = bound.arguments.get("chunks")
                for param in signature.parameters.values():
                    if param.kind is inspect.Parameter.VAR_KEYWORD:
                        defer = defer or bound.arguments.get(param.name, {}).get("defer", False)
                        if requested_chunks is None:
                            requested_chunks = bound.arguments.get(param.name, {}).get("chunks")
                if defer:
                    self._require_defer_support()
                # ACTRIS NetCDF inputs bypass the tabular driver entirely and
                # hand chunk options to xarray. Its NASA-Ames path still uses
                # PandasDriver, where array chunks have no meaning.
                files = bound.arguments.get("files")
                if files is None and args:
                    files = args[0]
                file_items = files if isinstance(files, list | tuple) else [files]
                is_actris_netcdf = self._source_name == "actris" and all(
                    str(path).lower().endswith(".nc") for path in file_items if path is not None
                )
                if (
                    requested_chunks is not None
                    and isinstance(getattr(self, "driver", None), PandasDriver)
                    and not is_actris_netcdf
                ):
                    raise OptionUnsupportedError(
                        "chunks",
                        getattr(self, "_source_name", None) or "point reader",
                        cause=(
                            "tabular readers parse each complete input file as one dataframe "
                            "partition; array chunk mappings cannot control those reads"
                        ),
                    )
                return original_open(self, *args, **kwargs)

            cls.open_dataset = _capability_guard
            READER_REGISTRY[name] = cls
        return cls

    return _register


# 2. The Abstract Base Class
class BaseReader(abc.ABC):
    """
    The interface that ALL readers must implement.
    """

    @abc.abstractmethod
    def open_dataset(self, files: str | list[str], **kwargs) -> xr.Dataset | pd.DataFrame:
        """
        Main entry point to read data.

        Args:
            files: File path, list of paths, or glob pattern.
            **kwargs: Reader-specific arguments.

        Returns:
            xarray.Dataset (for models/sat) or pandas.DataFrame (for point obs).
        """
        pass

    def harmonize(self, ds):
        """
        Optional: Apply standard naming conventions (middleware).
        Can be overridden by specific readers.
        """
        return ds

    def _manifest_spec(self):
        """Return this reader's manifest spec, or ``None`` when unregistered.

        Resolution order: the declared ``name``, then identity against the
        reader registry. A reader used directly in tests without registration
        has no spec, and capability declarations cannot apply to it.
        """
        from .._manifest import MANIFEST_BY_NAME

        spec = None
        name = getattr(self, "name", None) or getattr(self, "_registry_name", None)
        if name is not None:
            try:
                spec = resolve(name)
            except KeyError:
                spec = None
        if spec is None:
            # Recover the registered name by identity against the registry.
            for reg_name, cls in READER_REGISTRY.items():
                if cls is type(self) and reg_name in MANIFEST_BY_NAME:
                    spec = MANIFEST_BY_NAME[reg_name]
                    break
        return spec

    @property
    def _source_name(self) -> str | None:
        """The manifest name this reader loads under, when registered."""
        spec = self._manifest_spec()
        return spec.name if spec is not None else None

    def _require_defer_support(self) -> None:
        """Raise a clear error when this source declares no deferred support.

        ``defer=True`` on a source with
        ``capabilities.deferred = False`` MUST raise naming the source and the
        recorded reason — it MUST NOT silently return a materialized result.
        The lookup is best-effort: a reader that is not registered under a
        manifest name (e.g. a subclass used directly in tests) is allowed to
        defer.
        """
        spec = self._manifest_spec()
        if spec is not None and not spec.capabilities.deferred:
            reason = spec.capabilities.reason or "no reason recorded"
            raise CapabilityUnavailableError(spec.name, "defer", reason)


def _ensure_time_dimension(ds: xr.Dataset) -> xr.Dataset:
    """Ensure ``time`` is represented as a dimension when possible."""
    if not isinstance(ds, xr.Dataset):
        return ds

    if "time" in ds.dims:
        return ds

    if "time" in ds.coords:
        time_dims = ds["time"].dims

        # Promote scalar time to a singleton dimension.
        if len(time_dims) == 0:
            ds = ds.expand_dims("time")
            return ds

        # If time is a 1D coordinate attached to another dimension, swap dimensions.
        if len(time_dims) == 1 and time_dims[0] in ds.dims:
            try:
                ds = ds.swap_dims({time_dims[0]: "time"})
            except Exception:  # noqa: BLE001 - best-effort dimension normalization; a frame whose time coordinate cannot become a dimension keeps its original layout and still loads (categories: malformed)
                pass
            return ds

    if "time" in ds.variables and "time" not in ds.coords:
        ds = ds.set_coords("time")
        return _ensure_time_dimension(ds)

    return ds


class GriddedReader(BaseReader):
    """
    Base class for gridded data (Models, Satellites) that utilizes XarrayDriver.
    """

    def __init__(self):
        # The driver is told which source it belongs to so failures name the
        # source (unsupported options) and capability declarations can be
        # enforced at the driver level, where every reader's keywords land.
        self.driver = XarrayDriver(source=self._source_name)

    def open_dataset(
        self,
        files: str | list[str],
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        use_dask: bool = False,
        defer: bool = False,
        tolerate=None,
        **kwargs,
    ) -> xr.Dataset:
        """
        Uses XarrayDriver to open files. VirtualiZarr options are forwarded to the driver.

        Parameters
        ----------
        files : str or list[str]
            File path(s), URL(s), or glob pattern.
        use_virtualizarr : bool, optional
            Whether to use VirtualiZarr to create a virtual Zarr dataset, by default False.
        virtualizarr_file : str or None, optional
            Path to save/load the VirtualiZarr reference JSON file, by default None.
        virtualizarr_parser : str or None, optional
            The VirtualiZarr parser to use (e.g. 'hdf5', 'netcdf3', 'zarr', 'grib2').
        virtualizarr_backend : str, optional
            Backend for VirtualiZarr references ("kerchunk" or "icechunk"), by default "kerchunk".
        icechunk_repo : str or None, optional
            Path to the Icechunk repository, by default None.
        use_icechunk : bool, optional
            Whether to use Icechunk for VirtualiZarr references, by default False.
        icechunk_url : str or None, optional
            Path to the Icechunk repository, by default None.
        use_dask : bool, optional
            Whether to use Dask for lazy loading, by default False.
        defer : bool, optional
            Return an unevaluated, dask-backed dataset with no hidden
            full-dataset compute. Implies ``use_dask=True`` and
            wins over ``use_dask=False``; raises for
            a source declaring ``capabilities.deferred = False``.
        tolerate : str or bool, optional
            Explicit tolerance for skipped inputs (``"input"``); a failing file
            of several is skipped and recorded in provenance instead of failing
            the load. Never implied and never the default.
        **kwargs : dict
            Additional arguments passed to the driver.

        Returns
        -------
        xr.Dataset
            The loaded dataset.
        """
        if defer:
            self._require_defer_support()
            # defer wins over use_dask=False.
            use_dask = True

        ds = self.driver.open(
            files,
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
            use_dask=use_dask,
            defer=defer,
            tolerate=tolerate,
            **kwargs,
        )
        ds = self.harmonize(ds)
        ds = _ensure_time_dimension(ds)
        return ds

    def to_kerchunk(self, files: str | list[str], virtualizarr_file: str | None = None, **kwargs):
        """Generate Kerchunk references for the given files."""
        return self.driver.to_kerchunk(files, virtualizarr_file=virtualizarr_file, **kwargs)

    def to_icechunk(self, files: str | list[str], icechunk_url: str, **kwargs):
        """Generate Icechunk references for the given files."""
        return self.driver.to_icechunk(files, icechunk_url=icechunk_url, **kwargs)


class PointReader(BaseReader):
    """
    Base class for point/tabular data (Observations) that utilizes PandasDriver.
    """

    fixed_location = True

    def __init__(self):
        self.driver = PandasDriver(source=self._source_name)

    def open_dataset(
        self,
        files: str | list[str],
        # VirtualiZarr applies to gridded data only; a tabular source rejects
        # these instead of accepting and ignoring them (see _reject_virtualizarr).
        use_virtualizarr: bool = False,
        virtualizarr_file: str | None = None,
        virtualizarr_parser: str | None = None,
        virtualizarr_backend: str = "kerchunk",
        icechunk_repo: str | None = None,
        use_icechunk: bool = False,
        icechunk_url: str | None = None,
        # Standard PointReader kwargs
        read_method: str = "read_csv",
        as_xarray: bool = True,
        lazy: bool = False,
        defer: bool = False,
        tolerate=None,
        use_dask: bool = False,
        meta: pd.DataFrame | pd.Series | dict | tuple | None = None,
        expand2d: bool = True,
        **kwargs,
    ) -> Union[pd.DataFrame, xr.Dataset, "dd.DataFrame"]:
        """
        Retrieve and load point data.

        Parameters
        ----------
        files : Union[str, List[str]]
            File path, list of paths, or glob pattern.
        use_virtualizarr : bool, optional
            Rejected for point sources — virtualization only applies to gridded
            data. Passing it raises rather than being silently ignored.
        virtualizarr_file : str or None, optional
            Rejected for point sources (same reason).
        virtualizarr_parser : str or None, optional
            Rejected for point sources (same reason).
        virtualizarr_backend : str, optional
            Rejected for point sources when set to a non-default value.
        icechunk_repo : str or None, optional
            Rejected for point sources (same reason).
        use_icechunk : bool, optional
            Rejected for point sources when set to a non-default value.
        icechunk_url : str or None, optional
            Rejected for point sources (same reason).
        read_method : str, optional
            The pandas/dask reading method to use, by default "read_csv".
        as_xarray : bool, optional
            If True, return an xarray.Dataset, by default True.
        lazy : bool, optional
            If True, return a dask-backed object, by default False.
        defer : bool, optional
            If True, return an **unevaluated** result and perform no hidden
            full-data compute during the load. Implies
            ``lazy=True`` and overrides ``lazy=False``. With ``as_xarray=False`` this returns an
            unevaluated Dask DataFrame; with ``as_xarray=True`` it returns a
            Dask-backed :class:`xarray.Dataset` built without materializing the
            data (bounded metadata only). A source whose manifest
            declares ``capabilities.deferred = False`` raises a clear error
            naming the source and reason rather than silently
            returning a materialized result.
        use_dask : bool, optional
            Alias for `lazy`, by default False.
        meta : pd.DataFrame, pd.Series, dict, or tuple, optional
            Dask metadata to use for lazy loading, by default None.
        tolerate : str or bool, optional
            Explicit tolerance for skipped inputs (``"input"``); a failing file
            of several is skipped and recorded in provenance instead of failing
            the load. Never implied and never the default. Eager path only — a
            lazy load defers reading to Dask, where a failing input cannot be
            identified here, so the request is rejected rather than ignored.
        **kwargs : dict
            Additional arguments passed to the reader and driver.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, dd.DataFrame]
            The loaded dataset.
        """
        self._reject_virtualizarr(
            use_virtualizarr=use_virtualizarr,
            virtualizarr_file=virtualizarr_file,
            virtualizarr_parser=virtualizarr_parser,
            virtualizarr_backend=virtualizarr_backend,
            icechunk_repo=icechunk_repo,
            use_icechunk=use_icechunk,
            icechunk_url=icechunk_url,
        )
        # Handle 'use_dask' as an alias for 'lazy'
        if use_dask:
            lazy = True
        if defer:
            self._require_defer_support()
            # defer wins over lazy=False.
            lazy = True

        df = self.driver.open(
            files,
            read_method=read_method,
            lazy=lazy,
            defer=defer,
            meta=meta,
            tolerate=tolerate,
            **kwargs,
        )

        df = self.harmonize(df)

        # Consistently force object strings to avoid nullable string issues in Pandas/Dask
        df = force_object_strings(df)

        if as_xarray:
            ds = self.to_xarray(df, expand2d=expand2d, defer=defer, **kwargs)
            if not lazy:
                # The driver checked the frame; assembling the Dataset can
                # still leave the caller with a schema-only shell (variables
                # and coordinates defined, every dimension empty) when an
                # input parsed into nothing. That is indistinguishable from a
                # successful load, so it is refused here too. A lazy/deferred
                # result is not validated: nothing has been read yet.
                inputs = (
                    list(files)
                    if isinstance(files, list | tuple)
                    else ([] if files is None else [files])
                )
                ds = _validate_nonempty_result(ds, inputs, self._source_name)
            return ds

        return df

    def _reject_virtualizarr(self, **options) -> None:
        """Refuse virtualization options on a tabular source, naming each one.

        VirtualiZarr produces virtual references for chunked gridded formats;
        there is nothing for it to virtualize in a flat table. Accepting the
        options and doing nothing with them left callers believing a reference
        file had been written, so every non-default value raises instead.
        """
        defaults = {
            "use_virtualizarr": False,
            "virtualizarr_file": None,
            "virtualizarr_parser": None,
            "virtualizarr_backend": "kerchunk",
            "icechunk_repo": None,
            "use_icechunk": False,
            "icechunk_url": None,
        }
        source = self._source_name or type(self).__name__
        for option, default in defaults.items():
            value = options.get(option)
            if value != default:
                raise OptionUnsupportedError(option, source)

    def harmonize(
        self, df: Union[pd.DataFrame, "dd.DataFrame"]
    ) -> Union[pd.DataFrame, "dd.DataFrame"]:
        """
        Harmonize the dataset (standard naming, dropping NaNs).

        Rows dropped for a missing location are counted and the count is
        recorded in provenance — the discard is correct behavior, but an
        unrecorded one breaks mass-balance checks between input and output
        row counts. The count is only taken on the eager path: counting a
        lazy frame would require computing it, which a deferred load
        promises not to do.

        Parameters
        ----------
        df : Union[pd.DataFrame, "dd.DataFrame"]
            Input dataframe.

        Returns
        -------
        Union[pd.DataFrame, "dd.DataFrame"]
            Harmonized dataframe.
        """
        discarded = None
        if "latitude" in df.columns and "longitude" in df.columns:
            before = None
            if not _is_dask_frame(df):
                before = len(df)
            df = df.dropna(subset=["latitude", "longitude"])
            if before is not None:
                discarded = before - len(df)

        # Update history if attributes exist (backend-agnostic)
        message = "Harmonized and dropped NaN locations."
        if discarded is not None:
            record = ToleranceRecord().with_discarded_rows(discarded)
            line = format_tolerance_record(record, requested=True)
            if line:
                message = f"{message} {line}"
        df = update_history(df, message)

        return super().harmonize(df)

    def to_xarray(
        self,
        obj: Union[pd.DataFrame, "dd.DataFrame", xr.Dataset],
        expand2d: bool = True,
        defer: bool = False,
        **kwargs,
    ) -> xr.Dataset:
        """
        Convert a DataFrame or Dataset to an xarray Dataset in UGRID convention.
        By default, returns a 2D dataset (time, node) if expand2d=True.

        Parameters
        ----------
        obj : Union[pd.DataFrame, dd.DataFrame, xr.Dataset]
            Input dataframe or unstructured dataset.
        expand2d : bool, optional
            Whether to expand to 2D (time, node) structure, by default True.
        defer : bool, optional
            Preserve the explicit deferred-loading contract. Lazy and deferred
            table-to-Xarray conversion runs one preparation task per partition
            to collect its length and required ``time``/``siteid`` coordinate
            keys; ``variable`` keys are also collected for the wide UGRID
            layout. This is a full partition parse for text readers, with
            O(rows) coordinate keys retained in memory; it does not persist
            payload columns. Later evaluation can reread the input.
        **kwargs : dict
            Additional arguments passed to ds_to_2d (e.g. pivot).

        Returns
        -------
        xr.Dataset
            The dataset in UGRID convention.
        """
        # 1. Handle Dataset Input (Optimization to avoid round-trip)
        if isinstance(obj, xr.Dataset):
            ds = obj
            # Ensure dimension is 'node'
            if "node" not in ds.dims:
                # Find the primary dimension (should be 1D)
                potential_dims = [d for d in ds.dims if d not in ["time", "siteid"]]
                if potential_dims:
                    ds = ds.rename({potential_dims[0]: "node"})
        else:
            df = obj
            # 2. Identify backend
            try:
                import dask.dataframe as dd

                is_dask = isinstance(df, dd.DataFrame)
            except ImportError:
                is_dask = False

            # 3. Prepare DataFrame (ensure time and siteid are columns)
            if is_dask:
                temp_df = df
            else:
                temp_df = df.copy()

            # Drop 'index' if it exists to avoid conflicts during conversion
            if "index" in temp_df.columns:
                temp_df = temp_df.drop(columns="index")

            for name in ["time", "siteid"]:
                try:
                    names = temp_df.index.names
                except AttributeError:
                    names = [temp_df.index.name]

                if name in names:
                    temp_df = temp_df.reset_index()

            # 4. Handle Backends
            # Consistently force object strings for both backends to avoid nullable string issues.
            temp_df = force_object_strings(temp_df)

            if is_dask:
                # 4a. Lazy Path
                # Xarray needs concrete node lengths for the one-dimensional
                # partition layout. Computing these lengths executes each
                # upstream parser once; for text inputs this is a full payload
                # preparation scan, even though only O(partitions) integers are
                # returned. Share that scan across columns and retain the graph
                # for subsequent evaluation. Do not claim it is metadata-only
                # or persist the full table to avoid the later storage read.
                import dask
                import dask.array as da
                from dask.delayed import delayed

                # Materialize each partition once for lengths and the index
                # columns that Xarray must know before it can build node
                # coordinates. These values are bounded to partition lengths
                # and required coordinate keys; data variables remain in the
                # deferred graph and are reread when that graph is evaluated.
                parts = temp_df.to_delayed()
                layout_columns = [name for name in ("time", "siteid") if name in temp_df.columns]
                if expand2d and "variable" in temp_df.columns:
                    layout_columns.append("variable")
                if layout_columns:
                    layout = dask.compute(
                        *[
                            delayed(_partition_layout, pure=True)(part, layout_columns)
                            for part in parts
                        ]
                    )
                    lengths = [int(item[0]) for item in layout]
                    prepared_indexes = {
                        name: np.concatenate([item[1][name] for item in layout])
                        for name in layout_columns
                    }
                else:
                    # Dask's map_partitions(len) can trigger parser/meta work
                    # in addition to its length computation. The same delayed
                    # partition helper gives one full parse per partition and
                    # retains only the O(partitions) lengths for 1D output.
                    layout = dask.compute(
                        *[delayed(_partition_layout, pure=True)(part, []) for part in parts]
                    )
                    lengths = [int(item[0]) for item in layout]
                    prepared_indexes = {}
                # its own delayed parts re-optimizes the whole expression tree
                # per column, which is quadratic in the number of columns —
                # fatal for wide observation products (hundreds of wavelength
                # columns). Selecting the column inside the delayed callable
                # keeps it lazy without touching the graph again.

                def _col_array(col):
                    series = temp_df[col]
                    dtype = series.dtype
                    if isinstance(dtype, pd.api.extensions.ExtensionDtype):
                        # Mirror to_dask_array's conversion of extension dtypes
                        # (e.g. nullable strings) to object arrays.
                        dtype = np.dtype(object)
                    # Each partition delay yields a pandas DataFrame; wrap it so
                    # the dask-array blocks are real numpy arrays (a downstream
                    # unstack shuffle indexes blocks positionally, which a Series
                    # rejects). ``np.asarray`` is applied lazily, not computed.
                    arrs = [
                        da.from_delayed(
                            delayed(_partition_column, pure=True)(part, col),
                            shape=(n,),
                            dtype=dtype,
                        )
                        for part, n in zip(parts, lengths, strict=True)
                    ]
                    return arrs[0] if len(arrs) == 1 else da.concatenate(arrs)

                ds = xr.Dataset()
                for col in temp_df.columns:
                    if col == "node" and "node" in ds.dims:
                        continue
                    data = prepared_indexes[col] if col in prepared_indexes else _col_array(col)
                    ds[col] = (("node",), data)

                # Dask DataFrame has no DataFrame-level attrs. Preserve stable
                # reader metadata attached to its declared empty schema so
                # deferred Xarray output retains provenance such as AERONET's
                # source header, matching the eager conversion.
                ds.attrs.update(getattr(temp_df._meta, "attrs", {}))
            else:
                # 4b. Eager Path
                # Consistently use 1D for both Eager and Lazy by default.
                ds = temp_df.reset_index(drop=True).to_xarray()
                if "index" in ds.dims:
                    if "node" in ds.variables or "node" in ds.dims:
                        ds = ds.drop_vars("node", errors="ignore")
                    ds = ds.rename({"index": "node"})

        # Set standard coordinates
        coords = [
            c for c in ["time", "siteid", "latitude", "longitude", "elevation"] if c in ds.data_vars
        ]
        ds = ds.set_coords(coords)

        # Ensure node coordinate is a simple integer range for both
        if "node" in ds.dims:
            ds.coords["node"] = (("node",), np.arange(ds.sizes["node"]))

        # 4. Standard Path (Consistently try 2D expansion by default)
        # The user requested 2D UGRID as default.
        if expand2d:
            # We pass kwargs to allow control over pivoting (wide_fmt or pivot)
            pivot = kwargs.get("wide_fmt", kwargs.get("pivot", True))
            ds = ds_to_2d(ds, pivot=pivot, fixed_location=self.fixed_location)

        # Add UGRID metadata
        if "node" in ds.dims:
            node_coords = []
            for c in ["longitude", "latitude", "elevation"]:
                if c in ds.coords:
                    node_coords.append(c)

            if node_coords:
                ds["mesh"] = xr.DataArray(
                    data=np.int32(0),
                    attrs={
                        "cf_role": "mesh_topology",
                        "topology_dimension": 0,
                        "node_coordinates": " ".join(node_coords),
                    },
                )

            if "latitude" in ds.coords:
                ds.coords["latitude"].attrs.update(
                    {"units": "degrees_north", "standard_name": "latitude"}
                )
            if "longitude" in ds.coords:
                ds.coords["longitude"].attrs.update(
                    {"units": "degrees_east", "standard_name": "longitude"}
                )
            if "elevation" in ds.coords:
                ds.coords["elevation"].attrs.update(
                    {"units": "m", "standard_name": "height_above_mean_sea_level"}
                )

            for var in ds.data_vars:
                if "node" in ds[var].dims:
                    ds[var].attrs.update({"mesh": "mesh", "location": "node"})

        # Copy attributes from input object if they exist (e.g. history).
        # Dask DataFrames don't support .attrs the same way as pandas, so
        # we guard with getattr to avoid AttributeError.
        df_attrs = getattr(obj, "attrs", {}) or {}
        for k, v in df_attrs.items():
            if k not in ds.attrs:
                ds.attrs[k] = v
            elif k == "history":
                ds.attrs[k] = f"{v}\n{ds.attrs[k]}"

        # Add Global Attributes
        if "Conventions" not in ds.attrs:
            ds.attrs["Conventions"] = "CF-1.8 UGRID-1.0"
        elif "UGRID-1.0" not in ds.attrs["Conventions"]:
            ds.attrs["Conventions"] += " UGRID-1.0"

        # Update history
        ds = update_history(ds, "Converted to xarray Dataset with UGRID convention.")

        ds = _ensure_time_dimension(ds)

        return ds


def add_lazy_diagnostic(
    ds: xr.Dataset,
    name: str,
    spec: DiagnosticSpec,
    aliases: dict[str, list[str]] | None = None,
) -> xr.Dataset:
    """
    Adds a lazy diagnostic variable to the dataset if constituent variables exist.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.
    name : str
        Name of the diagnostic variable.
    spec : DiagnosticSpec
        Specification for the diagnostic.
    aliases : Dict[str, List[str]], optional
        Mapping of diagnostic names to potential existing variables in the file
        to use instead of calculating from constituents.

    Returns
    -------
    xarray.Dataset
        Dataset with diagnostic added if possible.
    """
    # 1. Check if name already exists as a data variable
    if name in ds.data_vars:
        return ds

    # 2. Check for pre-calculated summary variables to prevent regressions
    # Comprehensive default aliases for common model outputs
    default_aliases = {
        "PM25": ["PM25_TOT", "PM2_5", "PM2_5_DRY"],
        "PM10": ["PMC_TOT", "PM10", "PM_TOT", "PM10_DRY", "PM10_TOT"],
        "NOx": ["NOX"],
        "NOy": ["NOY"],
        "O3": ["OZONE"],
    }
    if aliases is not None:
        # Merge user aliases with defaults
        for k, v in aliases.items():
            if k in default_aliases:
                default_aliases[k] = list(set(default_aliases[k] + v))
            else:
                default_aliases[k] = v

    for alias in default_aliases.get(name, []):
        if alias in ds.data_vars:
            ds[name] = ds[alias].copy()
            ds[name].attrs.update(
                {"units": spec.units, "name": spec.name, "long_name": spec.long_name}
            )
            # Update history
            ds = update_history(ds, f"Added lazy diagnostic: {name} (using alias {alias}).")
            return ds

    # 3. Identify constituent variables available in the dataset
    available_vars = [v for v in spec.variables if v in ds.data_vars]
    if not available_vars:
        return ds

    # If weights are provided, they must match the full variable list in spec
    if spec.weights is not None:
        weights_map = dict(zip(spec.variables, spec.weights))
        weights = [weights_map[v] for v in available_vars]
    else:
        weights = [1.0] * len(available_vars)

    # 4. Compute lazy sum with unit synchronization
    with xr.set_options(keep_attrs=True):
        # Use first variable as base
        v0 = available_vars[0]
        new_var = ds[v0] * weights[0]
        base_units = ds[v0].attrs.get("units", "").lower()

        for i in range(1, len(available_vars)):
            v = available_vars[i]
            v_var = ds[v]
            v_units = v_var.attrs.get("units", "").lower()

            # Unit synchronization (e.g. ppmV vs ppbV)
            if v_units != base_units:
                if "ppm" in v_units and "ppb" in base_units:
                    v_var = v_var * 1000.0
                elif "ppb" in v_units and "ppm" in base_units:
                    v_var = v_var / 1000.0

            new_var = new_var + v_var * weights[i]

    # Inherit units from constituent variables if available, otherwise use spec
    units = ds[v0].attrs.get("units", spec.units)

    ds[name] = new_var.assign_attrs(
        {"units": units, "name": spec.name, "long_name": spec.long_name}
    )

    # Update history
    ds = update_history(ds, f"Added lazy diagnostic: {name} (sum of {', '.join(available_vars)}).")

    return ds


def _convert_to_ppb(ds: xr.Dataset) -> xr.Dataset:
    """
    Converts gas species units from ppmV to ppbV lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with converted units.
    """
    to_convert = [
        v for v in ds.data_vars if "units" in ds[v].attrs and "ppm" in ds[v].attrs["units"].lower()
    ]

    if not to_convert:
        return ds

    for v in to_convert:
        ds[v] = ds[v] * 1000.0
        ds[v].attrs["units"] = "ppbV"

    # Update history
    ds = update_history(ds, f"Converted {', '.join(to_convert)} from ppmV to ppbV.")

    return ds


def _format_units(ds: xr.Dataset) -> xr.Dataset:
    """
    Formats unit strings for particulate matter lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with formatted unit strings.
    """
    to_format = [
        v
        for v in ds.data_vars
        if "units" in ds[v].attrs
        and ("micrograms" in ds[v].attrs["units"].lower() or "ug" in ds[v].attrs["units"].lower())
    ]

    if not to_format:
        return ds

    for v in to_format:
        ds[v].attrs["units"] = r"$\mu g m^{-3}$"

    # Update history
    ds = update_history(ds, rf"Formatted units for {', '.join(to_format)} to $\mu g m^{{-3}}$.")

    return ds


def _convert_ugkg_to_ugm3(
    ds: xr.Dataset,
    *,
    alt_name: str = "ALT",
    pres_name: str = "pres_pa_mid",
    temp_name: str = "temperature_k",
    R: float = 287.05,
) -> xr.Dataset:
    """
    Converts mass mixing ratio (ug/kg) to mass concentration (ug/m3) lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.
    alt_name : str, optional
        Name of the specific volume variable, by default "ALT".
    pres_name : str, optional
        Name of the pressure variable (in Pa), by default "pres_pa_mid".
    temp_name : str, optional
        Name of the temperature variable (in K), by default "temperature_k".
    R : float, optional
        Gas constant for dry air in J/(kg·K), by default 287.05.

    Returns
    -------
    xarray.Dataset
        Dataset with converted units.
    """
    to_convert = [
        v
        for v in ds.data_vars
        if "units" in ds[v].attrs and "ug/kg" in ds[v].attrs["units"].lower()
    ]

    if not to_convert:
        return ds

    method = None
    if alt_name in ds.variables:
        # rho = 1 / ALT
        rho = 1.0 / ds[alt_name]
        method = f"using {alt_name} (specific volume)"
    elif pres_name in ds.variables and temp_name in ds.variables:
        # rho = P / (R * T)
        rho = ds[pres_name] / (R * ds[temp_name])
        method = f"using air density calculated from {pres_name} and {temp_name}"
    elif "P" in ds.variables and "PB" in ds.variables and "T" in ds.variables:
        # WRF-specific fallback if not already handled by pres_name/temp_name
        P_tot = ds["P"] + ds["PB"]
        T_actual = (ds["T"] + 300.0) * (P_tot / 100000.0) ** (287.05 / 1004.5)
        rho = P_tot / (R * T_actual)
        method = "using air density calculated from P, PB, T"
    else:
        return ds

    for v in to_convert:
        ds[v] = ds[v] * rho
        ds[v].attrs["units"] = r"$\mu g m^{-3}$"

    # Update history
    ds = update_history(ds, f"Converted {', '.join(to_convert)} from ug/kg to ug/m3 {method}.")

    return ds


def _add_ioapi_latlon(ds: xr.Dataset, proj4_srs: str) -> xr.Dataset:
    """
    Assigns latitude and longitude coordinates lazily for IOAPI-compliant grids.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset with IOAPI grid metadata (XORIG, YORIG, XCELL, YCELL, NCOLS, NROWS).
    proj4_srs : str
        The PROJ4 projection string.

    Returns
    -------
    xarray.Dataset
        Dataset with 'latitude' and 'longitude' coordinates.

    Examples
    --------
    >>> ds = _add_ioapi_latlon(ds, "+proj=lcc +lat_1=33 +lat_2=45 ...")
    """
    # 1. Generate 1D x and y values
    # NCOLS/NROWS might be attributes or dimensions
    ncols = ds.attrs.get("NCOLS", ds.sizes.get("x", ds.sizes.get("COL")))
    nrows = ds.attrs.get("NROWS", ds.sizes.get("y", ds.sizes.get("ROW")))

    xorig = ds.attrs.get("XORIG")
    yorig = ds.attrs.get("YORIG")
    xcell = ds.attrs.get("XCELL")
    ycell = ds.attrs.get("YCELL")

    if any(v is None for v in [ncols, nrows, xorig, yorig, xcell, ycell]):
        return ds

    # 2. Coordinate and Dimension Names
    x_dim = "COL" if "COL" in ds.dims else "x"
    y_dim = "ROW" if "ROW" in ds.dims else "y"

    # Generate 1D arrays
    x = np.linspace(xorig + xcell * 0.5, xorig + (ncols - 0.5) * xcell, ncols)
    y = np.linspace(yorig + ycell * 0.5, yorig + (nrows - 0.5) * ycell, nrows)

    xda = xr.DataArray(x, dims=x_dim)
    yda = xr.DataArray(y, dims=y_dim)

    # 3. Backend-Agnostic Chunking
    if ds.chunks:
        # Match coordinate chunking to data variables to maintain laziness.
        # We avoid hardcoded 'auto' and instead respect existing dataset chunks.
        x_chunks = {d: ds.chunks[d] for d in xda.dims if d in ds.chunks}
        y_chunks = {d: ds.chunks[d] for d in yda.dims if d in ds.chunks}
        if x_chunks:
            xda = xda.chunk(x_chunks)
        if y_chunks:
            yda = yda.chunk(y_chunks)

    # Broadcast to 2D
    yv, xv = xr.broadcast(yda, xda)

    # 4. Apply projection lazily
    def _proj_inv(
        x_val: np.ndarray, y_val: np.ndarray, p_srs: str | np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Kernel for lazy projection inversion using pyproj.Transformer.

        Parameters
        ----------
        x_val : np.ndarray
            X-coordinates in the projection.
        y_val : np.ndarray
            Y-coordinates in the projection.
        p_srs : Union[str, np.ndarray]
            PROJ projection string.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            Longitude and latitude arrays.
        """
        from pyproj import Transformer

        # Handle scalar/array inputs from apply_ufunc
        if isinstance(p_srs, np.ndarray | np.generic):
            p_srs = p_srs.item()
        if isinstance(p_srs, bytes):
            p_srs = p_srs.decode()

        # Transformer is generally faster than Proj for repeated use
        # and better handles modern PROJ versions.
        # We use a transformer from the input SRS to WGS84 (EPSG:4326).
        transformer = Transformer.from_crs(p_srs, "EPSG:4326", always_xy=True)
        return transformer.transform(x_val, y_val)

    lon, lat = xr.apply_ufunc(
        _proj_inv,
        xv,
        yv,
        proj4_srs,
        dask="parallelized",
        output_dtypes=[float, float],
        output_core_dims=[(), ()],
        keep_attrs=True,
    )

    # Assign coordinates and standard metadata
    ds = ds.assign_coords(
        longitude=lon.assign_attrs(
            {"long_name": "Longitude", "units": "degree_east", "standard_name": "longitude"}
        ),
        latitude=lat.assign_attrs(
            {"long_name": "Latitude", "units": "degree_north", "standard_name": "latitude"}
        ),
    )

    # Update history
    ds = update_history(ds, "Generated Latitude/Longitude coordinates via PROJ inversion.")

    return ds


def _get_ioapi_times(ds: xr.Dataset) -> xr.Dataset:
    """
    Extracts a concrete time coordinate from the small IOAPI TFLAG variable.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset with TFLAG variable.

    Returns
    -------
    xarray.Dataset
        Dataset with 'time' coordinate.

    Examples
    --------
    >>> ds = _get_ioapi_times(ds)
    """
    from .time_utils import parse_ioapi_times

    tflag = ds.TFLAG
    # TFLAG can be (TSTEP, DATE_TIME) or (TSTEP, VAR, DATE_TIME)
    if tflag.ndim == 3:
        tflag = tflag.isel(VAR=0, drop=True)

    # Handle dimension names (COL is often used for DATE_TIME in pseudonetcdf)
    dt_dims = [d for d in tflag.dims if "DATE" in str(d).upper() and "TIME" in str(d).upper()]
    if not dt_dims:
        dt_dim = tflag.dims[-1]
    else:
        dt_dim = dt_dims[0]

    # Parse only TFLAG through a local synchronous scheduler. Xarray needs a
    # concrete index coordinate, and inheriting a caller's distributed Client
    # here can send the PseudoNetCDF file manager to a worker during graph
    # construction. The gridded data variables remain Dask-backed.
    dates = xr.apply_ufunc(
        parse_ioapi_times,
        tflag.isel(**{dt_dim: 0}),
        tflag.isel(**{dt_dim: 1}),
        vectorize=False,
        dask="parallelized",
        output_dtypes=[np.dtype("datetime64[ns]")],
    )
    dates = dates.compute(scheduler="synchronous")

    # Some HDF readers expose the first TFLAG date as a fill value even when
    # the IOAPI header contains the valid start date. SDATE/STIME are the
    # authoritative timestamp for the first record, so use them to repair
    # only that missing value; later TFLAG values remain untouched.
    date_values = np.asarray(dates.values, dtype="datetime64[ns]")
    if date_values.size and np.isnat(date_values.flat[0]) and "SDATE" in ds.attrs:
        start_date = parse_ioapi_times(ds.attrs["SDATE"], ds.attrs.get("STIME", 0))
        start_date = np.asarray(start_date, dtype="datetime64[ns]").reshape(-1)
        if start_date.size and not np.isnat(start_date[0]):
            date_values = date_values.copy()
            date_values.flat[0] = start_date[0]
            dates = dates.copy(data=date_values)

    # If 'TSTEP' is the time dimension, we replace its values
    if "TSTEP" in ds.dims:
        ds = ds.assign_coords(TSTEP=dates)
        ds = ds.rename({"TSTEP": "time"})
    else:
        # Fallback: assume first dimension is time
        time_dim = tflag.dims[0]
        ds = ds.assign_coords({time_dim: dates})
        ds = ds.rename({time_dim: "time"})

    # Update history
    ds = update_history(ds, "Optimized IOAPI time parsing.")

    return ds


def _harmonize_ioapi_dims(ds: xr.Dataset) -> xr.Dataset:
    """
    Standardize IOAPI dimension names (COL, ROW, LAY) to (x, y, z) lazily.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with standardized dimensions.
    """
    rename_dict = {}
    if "COL" in ds.dims:
        rename_dict["COL"] = "x"
    if "ROW" in ds.dims:
        rename_dict["ROW"] = "y"
    if "LAY" in ds.dims:
        rename_dict["LAY"] = "z"

    if rename_dict:
        ds = ds.rename(rename_dict)
        ds = update_history(ds, f"Renamed dimensions: {rename_dict}")

    return ds


def _harmonize_ioapi_vars(ds: xr.Dataset) -> xr.Dataset:
    """
    Standardize IOAPI variable names and remove redundant data variables.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with standardized variables.
    """
    # 1. Standardize variable names to lowercase
    mapping = {v: v.lower() for v in ds.data_vars}
    # Avoid collisions with existing coordinates
    mapping = {k: v for k, v in mapping.items() if v not in ds.coords}
    ds = ds.rename(mapping)

    # 2. Drop redundant variables that are now coordinates
    # This prevents cluttering the data_vars when they have been promoted
    to_drop = [v for v in ds.data_vars if v in ds.coords]

    # Specific common ones in IOAPI that might be present but promoted/renamed
    # e.g. 'lat', 'lon', 'tflag'
    for redundant in ["lat", "lon", "tflag"]:
        if redundant in ds.data_vars:
            to_drop.append(redundant)

    if to_drop:
        ds = ds.drop_vars(list(set(to_drop)))
        ds = update_history(ds, f"Dropped redundant data variables: {list(set(to_drop))}")

    return ds


def _scientific_hygiene(ds: xr.Dataset) -> xr.Dataset:
    """
    Apply standard scientific hygiene to the dataset.

    1. Identifies standard coordinates (latitude, longitude, time) and ensures they are set.
    2. Strips whitespace from all string attributes.
    3. Updates the history attribute.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset
        Processed dataset.
    """
    # 1. Coordinate Handling - Ensure standard coordinates are set
    # We do NOT use reset_coords() to avoid demoting non-standard coords.
    coords = [
        c for c in ["latitude", "longitude", "time"] if c in ds.variables and c not in ds.coords
    ]
    if coords:
        ds = ds.set_coords(coords)

    # 2. Attribute Cleaning - Strip whitespace from string attributes
    for var in ds.variables:
        for attr, val in ds[var].attrs.items():
            if isinstance(val, str):
                ds[var].attrs[attr] = val.strip()

    # Also for global attributes
    for attr, val in ds.attrs.items():
        if isinstance(val, str):
            ds.attrs[attr] = val.strip()

    # 3. Provenance tracking
    ds = update_history(
        ds, "Applied scientific hygiene (standard coordinates and attribute cleaning)."
    )

    return ds
