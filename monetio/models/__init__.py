"""Deprecated ``monetio.models`` facade — now resolved lazily."""

from __future__ import annotations

from .. import _lazy

_SUBMODULES = (
    "_cesm_fv_mm",
    "_cesm_se_mm",
    "camx",
    "chimere",
    "cmaq",
    "hysplit",
    "hytraj",
    "icap_mme",
    "ncep_grib",
    "pardump",
    "raqms",
    "ufs",
)

__all__ = [
    "_cesm_fv_mm",
    "_cesm_se_mm",
    "camx",
    "chimere",
    "cmaq",
    "hysplit",
    "hytraj",
    "icap_mme",
    "ncep_grib",
    "pardump",
    "raqms",
    "ufs",
]


def __getattr__(name: str):
    module = _lazy.facade_attribute("models", name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = module
    return module


def __dir__():
    return sorted(set(globals()) | set(_SUBMODULES))
