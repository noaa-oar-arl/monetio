"""Deprecated ``monetio.sat`` facade — now resolved lazily."""

from __future__ import annotations

from .. import _lazy

_SUBMODULES = (
    "goes",
    "gridded_eos",
    "modis_l2",
    "modis_ornl",
    "mopitt_l3",
    "nesdis_edr_viirs",
    "nesdis_eps_viirs",
    "nesdis_frp",
    "omps_l3",
    "omps_nadir",
    "tempo_l2",
    "tropomi_l2_no2",
)

__all__ = [
    "gridded_eos",
    "modis_l2",
    "mopitt_l3",
    "omps_l3",
    "omps_nadir",
    "tempo_l2",
    "tropomi_l2_no2",
    "goes",
    "modis_ornl",
    "nesdis_edr_viirs",
    "nesdis_eps_viirs",
    "nesdis_frp",
]


def __getattr__(name: str):
    module = _lazy.facade_attribute("sat", name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = module
    return module


def __dir__():
    return sorted(set(globals()) | set(_SUBMODULES))
