"""MONETIO — Model and Observation Evaluation Toolkit: I/O.

Package initialization is **lazy**: importing ``monetio`` loads no reader, no
legacy facade submodule, and no source-specific optional dependency. Reader
and facade modules are drawn in on first access via PEP 562
:func:`__getattr__`.
"""

from __future__ import annotations

import importlib

from . import _lazy
from ._manifest import SOURCE_MANIFEST, all_source_names, build_reader_modules, resolve

__version__ = "0.2.7"

# Derived from the single-source-of-truth manifest. Retained as a
# private compatibility view because existing tests read ``monetio._READER_MODULES``;
# it is no longer hand-maintained, so it cannot drift from the registry.
_READER_MODULES = build_reader_modules()

# Package submodules that are real attributes but no longer eagerly imported.
# This is stable infrastructure (not a per-source list), so it cannot drift.
_PACKAGE_SUBMODULES = frozenset({"grids", "util", "cli", "obs", "models", "sat", "profile"})

# Legacy top-level convenience attributes, derived from the manifest's
# ``legacy_paths`` so that ``monetio.airnow`` keeps binding the deprecated
# facade module (which carries ``add_data``), exactly as today.
# Maps e.g. ``"airnow" -> "obs.airnow"``.
_LEGACY_ATTRS: dict[str, str] = {
    path.rsplit(".", 1)[-1]: path for spec in SOURCE_MANIFEST for path in spec.legacy_paths
}


# --- public enumeration API ------------------------------------------------


def available_sources() -> list[str]:
    """Every loadable source name (canonical + aliases), sorted.

    Imports no reader module.
    """
    return all_source_names()


def source_info(name: str):
    """Return the :class:`~monetio._manifest.SourceSpec` for ``name``/alias.

    Raises :class:`ValueError` naming the unknown source and listing available
    sources. Does not import the reader module.
    """
    try:
        return resolve(name)
    except KeyError:
        raise _lazy.unknown_source_error(name, verb="Unknown source") from None


def source_capabilities(name: str):
    """Return the scale-capability declaration for ``name``/alias."""
    return source_info(name).capabilities


def load(source: str, files=None, **kwargs):
    """
    Universal load function.

    Usage:
        ds = monetio.load("cmaq", files="/path/to/data*.nc")
        df = monetio.load("airnow", files=["2023-01-01", "2023-01-02"])

    The available sources are enumerated by :func:`available_sources`; this
    docstring no longer hand-lists them (that list drifted).

    Parameters
    ----------
    source : str
        Loadable source name (see :func:`available_sources`).
    files : str, list of str, or None
        File path(s), glob pattern, URL(s), or date arguments for remote
        sources.
    **kwargs
        Source-specific options. Two are shared across sources:

        ``defer``
            Return an unevaluated, dask-backed result. Sources that have not
            earned deferred support raise instead of silently materializing.
        ``tolerate``
            Explicit tolerance for skipped inputs — ``"input"`` skips a failing
            file of several and records the skip (location, reason, category)
            in the result's provenance. Tolerance is never implied and never
            the default: without it, the first bad input fails the whole load.
    """
    try:
        reader_cls = _lazy.get_reader_class(source)
    except KeyError:
        # An unknown name must report the same way as the other entry points
        # (ValueError naming the source + the available list), not a bare
        # KeyError from the manifest lookup.
        raise _lazy.unknown_source_error(source) from None
    reader = reader_cls()
    try:
        result = reader.open_dataset(files=files, **kwargs)
    except Exception as exc:
        # A failure must identify the inputs it came from. Readers annotate
        # their own errors at the exact site, but a failure raised in shared
        # plumbing (a parse helper, a coordinate transform) can surface here
        # without naming anything; add the caller's own inputs so no error
        # message can leave the caller guessing which file broke.
        from .readers.errors import annotate

        if files is not None:
            locations = files if isinstance(files, list | tuple) else [files]
            message = str(exc)
            missing = [str(f) for f in locations if str(f) not in message]
            if missing:
                annotated = annotate(exc, *missing)
                if annotated is exc:
                    # Already a reader error: the locations were appended in
                    # place, so re-raise untouched to keep its own chain.
                    raise
                annotated.__cause__ = exc.__cause__ if exc.__cause__ is not None else exc
                raise annotated from annotated.__cause__
        raise

    # A load that opened inputs and produced nothing is indistinguishable from
    # success, so it must refuse rather than return. The drivers enforce this on
    # their own paths; this boundary covers readers that assemble their result
    # outside a driver. A deferred/lazy result is not checked: nothing has been
    # read yet, so emptiness is not yet knowable at this point.
    if files is not None and result is not None:
        from .readers.drivers import _validate_nonempty_result

        if not type(result).__module__.startswith("dask"):
            inputs = files if isinstance(files, list | tuple) else [files]
            result = _validate_nonempty_result(result, inputs, source)
    return result


def virtualize(
    source: str,
    files,
    output: str,
    backend: str = "kerchunk",
    concat_dim: str = "time",
    **kwargs,
):
    """Pre-compute and persist VirtualiZarr references for a supported source.

    Parameters
    ----------
    source : str
        Registered reader source name (e.g. ``"gfs"`` or ``"merra2"``).
    files : str | list[str]
        Input file path(s) or glob expression.
    output : str
        Output reference path. For ``backend="kerchunk"``, this is a JSON file.
        For ``backend="icechunk"``, this is an Icechunk repository URL/path.
    backend : {"kerchunk", "icechunk"}, optional
        Virtualization backend, by default ``"kerchunk"``.
    concat_dim : str, optional
        Dimension used when combining multiple files, by default ``"time"``.
    **kwargs : dict
        Additional keyword arguments forwarded to :func:`load`.

    Returns
    -------
    xarray.Dataset
        Dataset opened through the virtualized references.
    """
    if backend not in {"kerchunk", "icechunk"}:
        raise ValueError(f"Invalid backend '{backend}'. Expected one of: 'kerchunk', 'icechunk'.")

    use_icechunk = backend == "icechunk"
    return load(
        source,
        files=files,
        use_virtualizarr=True,
        virtualizarr_file=None if use_icechunk else output,
        use_icechunk=use_icechunk,
        icechunk_url=output if use_icechunk else None,
        concat_dim=concat_dim,
        **kwargs,
    )


# --- PEP 562 lazy attribute resolution -------------------------------------


def __getattr__(name: str):
    """Resolve package attributes on demand, preserving every legacy surface.

    ``__getattr__`` is only called after normal attribute lookup fails, so real
    attributes (``load``, ``available_sources``, ``__version__``, the helper
    functions below) are never shadowed by a source lookup. Resolution order is
    fixed.
    """
    # 1. legacy facade convenience attribute (monetio.airnow -> monetio.obs.airnow),
    #    which today binds the deprecated wrapper carrying add_data(). If a
    #    declared legacy path has no facade module, fall through to the reader.
    legacy = _LEGACY_ATTRS.get(name)
    if legacy is not None:
        try:
            module = importlib.import_module(f"monetio.{legacy}")
        except ImportError:
            module = None
        if module is not None:
            globals()[name] = module
            return module

    # 2. a declared source with no legacy facade (gfs, icap_mme, ufs, ...) ->
    #    bind its reader module.
    if _lazy.is_source(name):
        module = _lazy.import_reader(name)
        globals()[name] = module
        return module

    # 3. a package submodule (grids, util, cli, or a facade package itself).
    if name in _PACKAGE_SUBMODULES:
        module = importlib.import_module(f"monetio.{name}")
        globals()[name] = module
        return module

    # 4. a facade submodule that is not a manifest source (e.g. openaq_v3,
    #    which today is eagerly imported and must stay reachable).
    for facade in ("obs", "models", "sat", "profile"):
        try:
            module = importlib.import_module(f"monetio.{facade}.{name}")
        except ImportError:
            continue
        globals()[name] = module
        return module

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    names = set(globals()) | set(_LEGACY_ATTRS) | set(all_source_names())
    names |= set(_PACKAGE_SUBMODULES)
    return sorted(names)


# ``__all__`` lists names resolved lazily via ``__getattr__`` (star-import
# triggers attribute access). Kept complete so ``from monetio import *`` works
# — this is the R2 Defect B fix (icap_mme was previously listed but never
# importable, so star-import raised AttributeError).
__all__ = [
    "__version__",
    "load",
    "virtualize",
    "available_sources",
    "source_info",
    "source_capabilities",
    #
    # utility functions
    "rename_latlon",
    "rename_to_monet_latlon",
    "dataset_to_monet",
    "coards_to_netcdf",
    #
    # utility modules
    "grids",
    #
    # point obs
    "airnow",
    "aeronet",
    "aqs",
    "cems",
    "crn",
    "improve",
    "ish",
    "ish_lite",
    "nadp",
    "openaq",
    "openaq_v2",
    "openaq_v3",
    "pams",
    #
    # profile obs
    "geoms",
    "gml_ozonesonde",
    "icartt",
    "tolnet",
    #
    # satellite obs
    "goes",
    #
    # models
    "camx",
    "cmaq",
    "hysplit",
    "hytraj",
    "icap_mme",
    "ncep_grib",
    "pardump",
    "raqms",
    "chimere",
    "gfs",
    "gefs",
    "gdas",
    "hrrr",
    "nam",
    "rap",
]


def rename_latlon(ds):
    """Rename latitude/longitude to ``'lat'``/``'lon'``.

    Parameters
    ----------
    ds : xarray.Dataset

    Returns
    -------
    xarray.Dataset
        Dataset with possibly renamed latitude/longitude.
    """
    if "latitude" in ds.coords:
        return ds.rename({"latitude": "lat", "longitude": "lon"})
    elif "Latitude" in ds.coords:
        return ds.rename({"Latitude": "lat", "Longitude": "lon"})
    elif "Lat" in ds.coords:
        return ds.rename({"Lat": "lat", "Lon": "lon"})
    else:
        return ds


def rename_to_monet_latlon(ds):
    """Rename latitude/longitude to ``'latitude'``/``'longitude'``.

    Parameters
    ----------
    ds : xarray.Dataset

    Returns
    -------
    xarray.Dataset
        Dataset with possibly renamed latitude/longitude.

    See Also
    --------
    rename_latlon : renames to ``'lat'``/``'lon'`` instead
    """
    if "lat" in ds.coords:
        return ds.rename({"lat": "latitude", "lon": "longitude"})
    elif "Latitude" in ds.coords:
        return ds.rename({"Latitude": "latitude", "Longitude": "longitude"})
    elif "Lat" in ds.coords:
        return ds.rename({"Lat": "latitude", "Lon": "longitude"})
    elif "grid_lat" in ds.coords:
        return ds.rename({"grid_lat": "latitude", "grid_lon": "longitude"})
    else:
        return ds


def dataset_to_monet(ds, *, lat_name="lat", lon_name="lon", latlon2d=None):
    """Apply :func:`coards_to_netcdf` if `latlon2d` is False.

    Parameters
    ----------
    ds : xarray.Dataset
    lat_name, lon_name : str
        Current latitude and longitude names in `ds`.
    latlon2d : bool, optional
        If not provided, the value will be detected by examining ``.ndim``
        of the latitude variable.

    Returns
    -------
    xarray.Dataset
    """
    if latlon2d is None:
        ndim_lat = ds[lat_name].ndim
        assert ndim_lat <= 2
        latlon2d = ndim_lat == 2

    if latlon2d is False:
        ds = coards_to_netcdf(ds, lat_name=lat_name, lon_name=lon_name)

    ds = rename_to_monet_latlon(ds)

    return ds


def coards_to_netcdf(ds, *, lat_name="lat", lon_name="lon"):
    """Assign 2-D latitude/longitude grid from 1-D latitude/longitude variables,
    setting ``'x'`` and ``'y'`` as 1-D zero-based index arrays.

    Also normalizes the latitude/longitude names to ``'latitude'``/``'longitude'``,
    with dimensions ``('y', 'x')``.

    .. note::
       The name is a reference to the COARDS conventions.

    Parameters
    ----------
    ds : xarray.Dataset
    lat_name, lon_name : str
        Current latitude and longitude names in `ds`.

    Returns
    -------
    xarray.Dataset
    """
    from numpy import arange, meshgrid

    lon = ds[lon_name]
    lat = ds[lat_name]
    assert lon.ndim == lat.ndim == 1
    lons, lats = meshgrid(lon, lat)
    x = arange(len(lon))
    y = arange(len(lat))
    ds = ds.rename({lon_name: "x", lat_name: "y"})
    ds.coords["longitude"] = (("y", "x"), lons)
    ds.coords["latitude"] = (("y", "x"), lats)
    ds["x"] = x
    ds["y"] = y
    ds = ds.set_coords(["latitude", "longitude"])
    return ds


# No eager grib2io import here. Importing grib2io pulls in zarr (and its whole
# codec stack), which would defeat lazy package initialization: a bare
# ``import monetio`` must not load optional third-party backends. grib2io
# registers its zarr v3 codecs itself on import (``grib2io/__init__`` imports
# its ``codecs`` submodule), and every code path that needs those codecs already
# imports grib2io — the ``engine="grib2io"`` xarray dispatch and the GRIB2
# VirtualiZarr pipeline both do — so registration happens exactly when needed
# without costing the bare import.
