"""Per-family offline-sample specifications.

The capture and generation scripts (``scripts/record_samples.py`` and
``scripts/make_synthetic_sample.py``) are generic machinery; the knowledge of
*what* to fetch for a given source — the exact remote location, how to keep the
payload minimal, and which load arguments the reader needs — lives here, split
by family so each family can be extended independently.

Each module exposes one of:

``CAPTURES``
    ``source -> spec`` for a payload recorded from a remote service. The spec
    keys are documented in ``record_samples.py``.

``SYNTHETICS``
    ``source -> (build_fn, reason, load_kwargs)`` for a format reproduced by a
    checked-in generator.

``record_samples.py`` merges the ``CAPTURES`` of every family module;
``make_synthetic_sample.py`` merges the ``SYNTHETICS``. A source appears in
exactly one family.
"""

from __future__ import annotations

from . import aerosol, airquality, gridded, model, point, satellite

# Recorded payloads, by family.
CAPTURES: dict[str, dict] = {
    **point.CAPTURES,
    **airquality.CAPTURES,
    **aerosol.CAPTURES,
    **satellite.CAPTURES,
    **gridded.CAPTURES,
}

# Synthetic format reproductions. Each family may add builders to its own
# ``SYNTHETICS`` dict (source -> (builder, reason, load_kwargs)); model output
# lives in ``model.py`` but a satellite/gridded source whose format is not
# reproducible from a redistributable payload can synthesize in its own file.
SYNTHETICS: dict[str, tuple] = {
    **getattr(model, "SYNTHETICS", {}),
    **getattr(satellite, "SYNTHETICS", {}),
    **getattr(gridded, "SYNTHETICS", {}),
    **getattr(point, "SYNTHETICS", {}),
    **getattr(airquality, "SYNTHETICS", {}),
    **getattr(aerosol, "SYNTHETICS", {}),
}

# Reasoned exclusions: sources that cannot be sampled offline (format not
# reproducible AND payload not redistributable, or gated behind a credential the
# offline suite cannot present). Each family records its own reasons; the
# coverage gate asserts every source is captured, generated, or excluded here.
EXCLUSIONS: dict[str, str] = {
    **getattr(point, "EXCLUSIONS", {}),
    **getattr(airquality, "EXCLUSIONS", {}),
    **getattr(aerosol, "EXCLUSIONS", {}),
    **getattr(satellite, "EXCLUSIONS", {}),
    **getattr(gridded, "EXCLUSIONS", {}),
    **getattr(model, "EXCLUSIONS", {}),
}

# Union of every source a family accounts for (captured, generated, or excluded),
# used to catch a source that no family claims.
KNOWN: set[str] = set(CAPTURES) | set(SYNTHETICS) | set(EXCLUSIONS)

__all__ = [
    "CAPTURES",
    "SYNTHETICS",
    "EXCLUSIONS",
    "KNOWN",
    "point",
    "airquality",
    "aerosol",
    "satellite",
    "gridded",
    "model",
]
