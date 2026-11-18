"""Property 5: Reader Structure Uniformity

For each reader module in monetio/readers/, inspect structure:
- Verify single @register_reader class (or known multi-class module)
- Correct inheritance from GriddedReader or PointReader (directly or transitively)
- Required methods: open_dataset() and harmonize()
- Non-empty module-level docstring

**Validates: Requirements 10.1, 10.2, 10.3, 10.4, 10.7**

Since we are checking a fixed set of modules, we use pytest.mark.parametrize
over all reader module names rather than Hypothesis.
"""

import importlib
import inspect

import pytest

from monetio._manifest import SOURCE_MANIFEST
from monetio.readers.base import READER_REGISTRY, GriddedReader, PointReader

# Reader modules come from the manifest (the single source of truth): only
# modules that declare a source are audited, so the infrastructure modules
# (base, drivers, *_specs, *_utils, _deprecation) are excluded by
# construction rather than by a hand-maintained list.
MANIFEST_MODULE_NAMES = sorted({s.module.rsplit(".", 1)[-1] for s in SOURCE_MANIFEST})

# Modules that legitimately contain multiple distinct registered classes are
# exactly the modules whose manifest spec declares aliases — the aliases are
# what signal additional registrations (e.g. gfs also registers gefs/gdas).
MULTI_CLASS_MODULES = {s.module.rsplit(".", 1)[-1] for s in SOURCE_MANIFEST if s.aliases}


def _get_reader_module_names():
    """All audited reader module names, derived from the manifest."""
    return MANIFEST_MODULE_NAMES


READER_MODULE_NAMES = _get_reader_module_names()


def _import_reader_module(module_name):
    """Import a reader module and return it."""
    return importlib.import_module(f"monetio.readers.{module_name}")


def _get_registered_classes(mod, module_name):
    """Get unique classes from this module that are in READER_REGISTRY."""
    full_module = f"monetio.readers.{module_name}"
    registered = []
    for _name, obj in inspect.getmembers(mod, inspect.isclass):
        if obj.__module__ != full_module:
            continue
        for _reg_name, reg_cls in READER_REGISTRY.items():
            if reg_cls is obj and obj not in registered:
                registered.append(obj)
    return registered


# Feature: virtualizarr-reader-refactor, Property 5: Reader Structure Uniformity


@pytest.mark.parametrize("module_name", READER_MODULE_NAMES)
def test_reader_structure_contract(module_name):
    """Check each reader module's complete structural contract in one case.

    The former five tests repeated module import and reader-class discovery for
    every module. Keep every check and aggregate failures so a single case still
    reports all structural defects for the affected module.
    """
    mod = _import_reader_module(module_name)
    failures = []

    if not (mod.__doc__ and mod.__doc__.strip()):
        failures.append("module docstring is missing or empty")

    registered = _get_registered_classes(mod, module_name)
    if not registered:
        failures.append("no registered reader class found")
    elif module_name not in MULTI_CLASS_MODULES and len(registered) != 1:
        failures.append(
            f"found {len(registered)} registered classes "
            f"({[cls.__name__ for cls in registered]}), expected exactly one"
        )

    for cls in registered:
        if not issubclass(cls, GriddedReader | PointReader):
            failures.append(
                f"{cls.__name__} does not inherit from GriddedReader or PointReader "
                f"(MRO: {[base.__name__ for base in cls.__mro__]})"
            )
        for method in ("open_dataset", "harmonize"):
            if not callable(getattr(cls, method, None)):
                failures.append(f"{cls.__name__}.{method} is missing or not callable")

    assert not failures, (
        f"Module {module_name} violates reader structure contract:\n- " + "\n- ".join(failures)
    )
