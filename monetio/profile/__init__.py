"""Deprecated ``monetio.profile`` facade — now resolved lazily."""

from __future__ import annotations

from .. import _lazy

_SUBMODULES = ("geoms", "gml_ozonesonde", "icartt", "tolnet", "umbc_aerosol")

__all__ = ["tolnet", "icartt", "geoms", "gml_ozonesonde", "umbc_aerosol"]


def __getattr__(name: str):
    module = _lazy.facade_attribute("profile", name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = module
    return module


def __dir__():
    return sorted(set(globals()) | set(_SUBMODULES))
