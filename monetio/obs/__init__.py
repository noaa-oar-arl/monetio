"""Deprecated ``monetio.obs`` facade — now resolved lazily.

Submodules are imported on first attribute access instead of eagerly, so
``import monetio`` no longer drags in every observation reader. The submodule
files themselves (the ``@deprecated_wrapper`` shims) are unchanged.
"""

from __future__ import annotations

from .. import _lazy

# Names previously eagerly imported here, preserved as the facade surface so
# ``from monetio.obs import airnow`` etc. keep working unchanged.
_SUBMODULES = (
    "actris",
    "aeronet",
    "airnow",
    "aqs",
    "cems",
    "crn",
    "epa_util",
    "eprofile",
    "iagos",
    "improve",
    "ish",
    "ish_lite",
    "nadp",
    "ndacc",
    "openaq",
    "openaq_v2",
    "openaq_v3",
    "pams",
    "pandora",
    "skynet",
)

__all__ = [
    "actris",
    "aeronet",
    "airnow",
    "aqs",
    "crn",
    "eprofile",
    "epa_util",
    "iagos",
    "improve",
    "ish",
    "ish_lite",
    "cems",
    "nadp",
    "ndacc",
    "openaq",
    "openaq_v2",
    "openaq_v3",
    "pams",
    "pandora",
    "skynet",
]


def __getattr__(name: str):
    module = _lazy.facade_attribute("obs", name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = module
    return module


def __dir__():
    return sorted(set(globals()) | set(_SUBMODULES))


# ish = ish_mod.ISH()
# airnow = airnow_mod.AirNow()
# aqs = aqs_mod.AQS()
# aeronet = aeronet_mod.AERONET()
# crn = crn_mod.crn()
# improve = improve_mod.IMPROVE()
# tolnet = tolnet_mod.TOLNet()
# cems = cems_mod.CEMS()
# nadp = nadp_mod.NADP()
# icartt = icartt_mod.icartt()
