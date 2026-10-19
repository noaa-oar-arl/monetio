"""Single source of truth for MONETIO data sources.

Every loadable source is declared exactly once here as a :class:`SourceSpec`.
This module MUST stay import-free of readers and heavy third-party packages:
it imports only the standard library and
``typing``. ``monetio/readers/base.py`` depends on this module, never the
reverse.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

__all__ = [
    "ScaleCapabilities",
    "SourceSpec",
    "SOURCE_MANIFEST",
    "MANIFEST_BY_NAME",
    "MANIFEST_BY_ALIAS",
    "all_source_names",
    "canonical_name",
    "module_for",
    "resolve",
    "build_reader_modules",
]

Kind = Literal["gridded", "point"]


@dataclass(frozen=True)
class ScaleCapabilities:
    """Per-source declaration of honored large-data behavior.

    Defaults are all ``False``: a source must *earn* a capability, and the
    conformance suite asserts every ``True`` is backed by observed
    behavior. ``reason`` explains a ``deferred=False``.
    """

    deferred: bool = False
    concurrent_files: bool = False
    parallel_metadata: bool = False
    remote: bool = False
    distributed: bool = False
    deferred_wide: bool = False
    chunk_controls: tuple[str, ...] = ()
    reason: str | None = None


# Honest default: nothing is claimed deferred until it is validated.
_UNVALIDATED = ScaleCapabilities(reason="deferred loading not yet validated for this source")

# --- earned capability declarations ----------------------------------------
# Every ``True`` below is backed by implemented behavior and asserted by the
# scale test suite (the conformance suite independently exercises each claim),
# so adding a flag without the matching implementation and test causes a
# failure. ``reason`` is supplied for every ``deferred=False`` source, which is
# what the not-yet-migrated sources use above.
#
# ``remote`` is intentionally left ``False`` everywhere: the object-store
# plumbing (streaming reads, credential-error reporting) was validated at the
# shared driver layer, not per source, so no individual reader has *earned* it
# yet. The deferred-wide flag is reserved for the readers that actually defer a
# long->wide reshape; a source whose frames are already wide does not claim it.

# Point reader with a genuinely deferred wide path: the frame stays LONG through
# the DataFrame stage and the (time, siteid) reshape is deferred to
# to_xarray/ds_to_2d, which stays dask-backed. The deferred graph is verified to
# evaluate equivalently on an in-process distributed scheduler.
_DEFERRED_WIDE = ScaleCapabilities(deferred=True, deferred_wide=True, distributed=True)

# Point reader that defers loading but whose frames are already wide (one column
# per wavelength), so there is no long->wide reshape to defer -- only the
# dask-backed DataFrame and its lazy to_xarray conversion are deferred.
# Distributed evaluation is not yet asserted for this source, so
# ``distributed`` stays False.
_DEFERRED = ScaleCapabilities(deferred=True)

# Gridded reader: open_mfdataset-backed, so defer == dask-backed open; chunk
# controls (`chunks`) pass through to xarray; ``parallel=True`` reaches
# xr.open_mfdataset with result-identical metadata parsing, and the deferred
# graph is verified to evaluate equivalently on an in-process distributed
# scheduler.
_GRID_DEFER = ScaleCapabilities(
    deferred=True,
    parallel_metadata=True,
    distributed=True,
    chunk_controls=("chunks",),
)

# Gridded reader whose multi-file path goes through a custom ``read_method``
# (per-file group merge), so the ``dask.delayed`` fan-out applies (verified
# result-identical, order-preserving). Distributed evaluation is not yet
# asserted for this source, so ``distributed`` stays False.
_GRID_CONCURRENT = ScaleCapabilities(
    deferred=True,
    concurrent_files=True,
    chunk_controls=("chunks",),
)


@dataclass(frozen=True)
class SourceSpec:
    """Authoritative declaration of one data source."""

    name: str
    module: str
    kind: Kind
    capabilities: ScaleCapabilities = _UNVALIDATED
    aliases: tuple[str, ...] = ()
    legacy_paths: tuple[str, ...] = field(default=())


def _s(name, module, kind, *, aliases=(), caps=_UNVALIDATED, legacy=()):
    return SourceSpec(
        name=name, module=module, kind=kind, capabilities=caps, aliases=aliases, legacy_paths=legacy
    )


# The registered source names (canonical + aliases), grouped by family.
# Kinds match the reader base classes exactly (verified against READER_REGISTRY).
SOURCE_MANIFEST: tuple[SourceSpec, ...] = (
    # ---- models (gridded) ----
    _s("cmaq", ".readers.cmaq", "gridded", caps=_GRID_DEFER, legacy=("models.cmaq",)),
    _s("camx", ".readers.camx", "gridded", legacy=("models.camx",)),
    _s("chimere", ".readers.chimere", "gridded", legacy=("models.chimere",)),
    _s("hysplit", ".readers.hysplit", "gridded", legacy=("models.hysplit",)),
    _s("hytraj", ".readers.hytraj", "point", legacy=("models.hytraj",)),
    _s("icap_mme", ".readers.icap_mme", "gridded", legacy=("models.icap_mme",)),
    _s("ncep_grib", ".readers.ncep_grib", "gridded", legacy=("models.ncep_grib",)),
    _s("pardump", ".readers.pardump", "point", legacy=("models.pardump",)),
    _s("raqms", ".readers.raqms", "gridded", legacy=("models.raqms",)),
    _s("ufs", ".readers.ufs", "gridded", legacy=("models.ufs",)),
    _s("wrfchem", ".readers.wrfchem", "gridded"),
    _s("grib2", ".readers.grib2", "gridded"),
    _s("rrfs", ".readers.rrfs", "gridded"),
    _s("gfs", ".readers.gfs", "gridded", aliases=("gefs", "gdas")),
    _s("hrrr", ".readers.hrrr", "gridded"),
    _s("nam", ".readers.nam", "gridded"),
    _s("rap", ".readers.rap", "gridded"),
    _s("ncep_reanalysis", ".readers.ncep_reanalysis", "gridded"),
    _s(
        "era5",
        ".readers.era5",
        "gridded",
        caps=ScaleCapabilities(
            chunk_controls=("chunks",),
            reason="Deferred loading is not advertised; chunks are honored with use_dask=True.",
        ),
    ),
    # ---- point observations ----
    _s("airnow", ".readers.airnow", "point", caps=_DEFERRED_WIDE, legacy=("obs.airnow",)),
    _s("aeronet", ".readers.aeronet", "point", caps=_DEFERRED, legacy=("obs.aeronet",)),
    _s("aqs", ".readers.aqs", "point", legacy=("obs.aqs",)),
    _s("cems", ".readers.cems", "point", legacy=("obs.cems",)),
    _s("crn", ".readers.crn", "point", legacy=("obs.crn",)),
    _s("improve", ".readers.improve", "point", legacy=("obs.improve",)),
    _s("eprofile", ".readers.eprofile", "gridded", legacy=("obs.eprofile",)),
    _s("ish", ".readers.ish", "point", legacy=("obs.ish",)),
    _s("ish_lite", ".readers.ish_lite", "point", legacy=("obs.ish_lite",)),
    _s("nadp", ".readers.nadp", "point", legacy=("obs.nadp",)),
    _s("openaq", ".readers.openaq", "point", legacy=("obs.openaq",)),
    _s("openaq_v2", ".readers.openaq_v2", "point", legacy=("obs.openaq_v2",)),
    _s("openaq_aws", ".readers.openaq_aws", "point", legacy=("obs.openaq_aws",)),
    _s("pams", ".readers.pams", "point", legacy=("obs.pams",)),
    _s("ndbc", ".readers.ndbc", "point"),
    _s("surfrad", ".readers.surfrad", "point"),
    _s("solrad", ".readers.solrad", "point", legacy=("obs.solrad",)),
    _s("ndacc", ".readers.ndacc", "gridded", legacy=("obs.ndacc",)),
    _s("pandora", ".readers.pandora", "gridded", legacy=("obs.pandora",)),
    _s("skynet", ".readers.skynet", "point", legacy=("obs.skynet",)),
    _s("madis", ".readers.madis", "point"),
    _s("amdar", ".readers.amdar", "point"),
    _s("ameriflux", ".readers.ameriflux", "point"),
    _s("tccon", ".readers.tccon", "point"),
    # ---- profile ----
    _s("icartt", ".readers.icartt", "point", legacy=("profile.icartt",)),
    _s("tolnet", ".readers.tolnet", "gridded", caps=_GRID_CONCURRENT, legacy=("profile.tolnet",)),
    _s("geoms", ".readers.geoms", "gridded", legacy=("profile.geoms",)),
    _s("gml_ozonesonde", ".readers.gml_ozonesonde", "point", legacy=("profile.gml_ozonesonde",)),
    _s("igra2", ".readers.igra2", "point"),
    _s("mplnet", ".readers.mplnet", "gridded", legacy=("obs.mplnet",)),
    _s("earlinet", ".readers.earlinet", "gridded", legacy=("obs.earlinet",)),
    _s("actris", ".readers.actris", "point", legacy=("obs.actris",)),
    _s("iagos", ".readers.iagos", "point", legacy=("obs.iagos",)),
    _s("umbc_aerosol", ".readers.umbc_aerosol", "gridded", legacy=("profile.umbc_aerosol",)),
    # ---- satellite (gridded) ----
    _s("goes", ".readers.goes", "gridded", legacy=("sat.goes",)),
    _s(
        "nesdis_edr_viirs", ".readers.nesdis_edr_viirs", "gridded", legacy=("sat.nesdis_edr_viirs",)
    ),
    _s(
        "nesdis_eps_viirs", ".readers.nesdis_eps_viirs", "gridded", legacy=("sat.nesdis_eps_viirs",)
    ),
    _s("modis_ornl", ".readers.modis_ornl", "gridded", legacy=("sat.modis_ornl",)),
    _s("nasa_modis", ".readers.nasa_modis", "gridded", legacy=("sat.nasa_modis",)),
    _s("modis_l2", ".readers.modis_l2", "gridded", legacy=("sat.modis_l2",)),
    _s("nesdis_frp", ".readers.nesdis_frp", "gridded", legacy=("sat.nesdis_frp",)),
    _s("omps", ".readers.omps", "gridded"),
    _s("omps_nadir", ".readers.omps_nadir", "gridded", legacy=("sat.omps_nadir",)),
    _s("mopitt", ".readers.mopitt", "gridded"),
    _s("tempo", ".readers.tempo", "gridded"),
    _s("tropomi", ".readers.tropomi", "gridded"),
    _s(
        "merra2",
        ".readers.merra2",
        "gridded",
        caps=ScaleCapabilities(
            chunk_controls=("chunks",),
            reason="Deferred loading is not advertised; chunks are honored with use_dask=True.",
        ),
    ),
    _s("nesdis_viirs_jrr", ".readers.nesdis_viirs_jrr", "gridded", aliases=("viirs_jrr",)),
    _s("calipso", ".readers.calipso", "gridded"),
    _s("earthcare", ".readers.earthcare", "gridded"),
    _s("gems", ".readers.gems", "gridded"),
    _s("gpm_imerg", ".readers.gpm_imerg", "gridded"),
    _s("jpss_atms", ".readers.jpss_atms", "gridded"),
    _s("jpss_cris", ".readers.jpss_cris", "gridded"),
    _s("mrms", ".readers.mrms", "gridded"),
    _s("sentinel4", ".readers.sentinel4", "gridded"),
    _s("smap", ".readers.smap", "gridded"),
)


# --- derived indices (built once; O(1) lookups) ----------------------------

MANIFEST_BY_NAME: dict[str, SourceSpec] = {s.name: s for s in SOURCE_MANIFEST}
MANIFEST_BY_ALIAS: dict[str, SourceSpec] = {a: s for s in SOURCE_MANIFEST for a in s.aliases}


def canonical_name(name: str) -> str:
    """Map a name or alias to its canonical source name (raises if unknown)."""
    if name in MANIFEST_BY_NAME:
        return name
    if name in MANIFEST_BY_ALIAS:
        return MANIFEST_BY_ALIAS[name].name
    raise KeyError(name)


def resolve(name: str) -> SourceSpec:
    """Return the :class:`SourceSpec` for a canonical name or alias."""
    if name in MANIFEST_BY_NAME:
        return MANIFEST_BY_NAME[name]
    if name in MANIFEST_BY_ALIAS:
        return MANIFEST_BY_ALIAS[name]
    raise KeyError(name)


def module_for(name: str) -> str:
    """Relative import path (e.g. ``".readers.cmaq"``) for a name or alias."""
    return resolve(name).module


def all_source_names() -> list[str]:
    """Every loadable name, canonical + aliases, sorted."""
    return sorted([*MANIFEST_BY_NAME, *MANIFEST_BY_ALIAS])


def build_reader_modules() -> dict[str, str]:
    """Derived ``name -> module`` view replacing the hand-maintained index."""
    out: dict[str, str] = {}
    for s in SOURCE_MANIFEST:
        out[s.name] = s.module
        for a in s.aliases:
            out[a] = s.module
    return out
