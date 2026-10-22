"""Reader package.

Historically this module eagerly imported every reader to populate a registry,
which is exactly what made ``import monetio`` slow (53 reader
modules loaded on a bare import). It now contains **no eager reader imports**:
readers register themselves as a side effect of being imported on demand
through :mod:`monetio._lazy`, and the source list is derived from the
dependency-free :mod:`monetio._manifest`.

The names re-exported here are the enumeration API only. Importing them pulls
in :mod:`monetio._manifest` (standard library + ``typing`` only),
never a reader module.
"""

from __future__ import annotations

from .._manifest import all_source_names, canonical_name, module_for, resolve

__all__ = [
    "all_source_names",
    "canonical_name",
    "module_for",
    "resolve",
]
