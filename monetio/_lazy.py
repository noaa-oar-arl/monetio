"""Lazy module-resolution primitives shared by ``monetio/__init__.py`` and the
four legacy facade packages.

Everything here is dependency-light: importing this module imports only the
standard library plus :mod:`monetio._manifest` (itself import-free).
That is what makes genuinely lazy package initialization possible.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from ._manifest import MANIFEST_BY_ALIAS, MANIFEST_BY_NAME, all_source_names, resolve

if TYPE_CHECKING:  # pragma: no cover - typing only, never imported at runtime
    import types

__all__ = [
    "is_source",
    "import_reader",
    "get_reader_class",
    "unknown_source_error",
    "facade_attribute",
    "legacy_attribute",
]


def is_source(name: str) -> bool:
    """True if ``name`` is a declared source name or alias."""
    return name in MANIFEST_BY_NAME or name in MANIFEST_BY_ALIAS


def _import(rel_or_dotted: str) -> types.ModuleType:
    """Import a module path, relative to ``monetio`` when it starts with ``.``."""
    if rel_or_dotted.startswith("."):
        return importlib.import_module(rel_or_dotted, package="monetio")
    return importlib.import_module(rel_or_dotted)


def import_reader(name: str) -> types.ModuleType:
    """Import the reader module providing ``name`` (canonical or alias).

    Raises :class:`KeyError` for an unknown name (callers convert via
    :func:`unknown_source_error`). An import failure re-raises the **original**
    exception chained, naming the source and module —
    never a fallback to an unrelated reader.
    """
    spec = resolve(name)
    try:
        return _import(spec.module)
    except BaseException as exc:  # re-raised below, chained; not swallowed
        raise ImportError(
            f"monetio: failed to import source {name!r} from module {spec.module!r}: {exc}"
        ) from exc


def get_reader_class(name: str):
    """Resolve ``name`` to its registered reader class.

    Imports the declaring module on first use; registration is a side effect of
    that import. Idempotent: a second call performs no re-import.
    """
    from .readers.base import READER_REGISTRY

    spec = resolve(name)
    if name not in READER_REGISTRY:
        import_reader(name)
    if name not in READER_REGISTRY:
        # Module imported cleanly but never registered the declared name: a
        # genuine internal inconsistency, not a user error.
        raise RuntimeError(
            f"Source {name!r} is declared in the manifest (module {spec.module!r}) "
            "but its module did not register it."
        )
    return READER_REGISTRY[name]


def unknown_source_error(name: str, *, verb: str = "Unknown source") -> ValueError:
    """``ValueError`` naming the bad source and listing what is available."""
    return ValueError(f"{verb} {name!r}. Available sources: {', '.join(all_source_names())}")


def legacy_attribute(facade: str, name: str) -> types.ModuleType:
    """Import a legacy facade module, e.g. ``legacy_attribute("obs", "airnow")``.

    Used for the deprecated top-level convenience attributes
    (``monetio.airnow`` -> ``monetio.obs.airnow``), which today bind to the
    facade wrappers carrying ``add_data()``. Raises ``ImportError`` when the
    facade module does not exist so callers can fall through.
    """
    return importlib.import_module(f"monetio.{facade}.{name}")


def facade_attribute(facade: str, name: str) -> types.ModuleType | None:
    """Resolve an attribute on a facade package.

    Prefers a real submodule of the facade (the deprecated wrapper, preserving
    today's behavior), and falls back to the manifest's reader module for
    sources the facade never had (e.g. ``monetio.models.gfs``) — an additive
    convenience. Returns ``None`` when neither exists so the caller can raise
    the conventional ``AttributeError``.
    """
    try:
        return importlib.import_module(f"monetio.{facade}.{name}")
    except ImportError:
        pass
    if is_source(name):
        try:
            return import_reader(name)
        except ImportError:
            return None
    return None
