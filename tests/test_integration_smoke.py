"""Integration smoke tests for the monetio reader refactor.

**Validates: Requirements 11.1, 11.2, 11.3**

- Verify all reader modules import without error.
- Verify ``monetio.load()`` with each source name doesn't crash on import.
- Verify ``cdump2netcdf.py`` and ``epa_util.py`` have no deprecation infrastructure.
"""

import importlib
import inspect
import unittest.mock as mock
from pathlib import Path

import pytest

import monetio
from monetio._manifest import SOURCE_MANIFEST

# ---------------------------------------------------------------------------
# All reader-module paths, derived from the manifest (the single source of
# truth). Source modules come from SOURCE_MANIFEST; infrastructure modules
# (base, drivers, spec/util helpers) are whatever else lives in the readers
# package. Neither list is hand-maintained, so neither can drift.
# ---------------------------------------------------------------------------


def _reader_package_modules() -> list[str]:
    pkg_dir = Path(importlib.import_module("monetio.readers").__file__).parent
    names = sorted(p.stem for p in pkg_dir.glob("*.py") if p.stem != "__init__")
    return [f"monetio.readers.{n}" for n in names]


_READER_MODULES = _reader_package_modules()

# Modules the manifest declares, as a sanity cross-check of the discovery:
# every manifest module must exist on disk.
_declared = {f"monetio.readers.{s.module.rsplit('.', 1)[-1]}" for s in SOURCE_MANIFEST}
_missing = _declared - set(_READER_MODULES)
assert not _missing, f"manifest declares modules missing on disk: {sorted(_missing)}"


@pytest.mark.parametrize("module_path", _READER_MODULES)
def test_reader_module_imports_without_error(module_path):
    """Each reader module should import without raising ImportError."""
    mod = importlib.import_module(module_path)
    assert mod is not None


# ---------------------------------------------------------------------------
# All source names (canonical + aliases), enumerated from the manifest
# ---------------------------------------------------------------------------

_ALL_SOURCE_NAMES = monetio.available_sources()


@pytest.mark.parametrize("source_name", _ALL_SOURCE_NAMES)
def test_load_source_resolves_reader(source_name):
    """monetio.load() with each source name should resolve the reader without crash.

    We mock the reader's open_dataset to prevent actual file I/O, but verify
    that the reader class is found and instantiated.
    """
    from monetio.readers.base import READER_REGISTRY

    # Ensure the reader module is imported (lazy loading)
    if source_name not in READER_REGISTRY:
        importlib.import_module(monetio.source_info(source_name).module, package="monetio")

    assert source_name in READER_REGISTRY, (
        f"Source '{source_name}' not found in READER_REGISTRY after import"
    )

    reader_cls = READER_REGISTRY[source_name]

    # Mock open_dataset to prevent real I/O
    with mock.patch.object(reader_cls, "open_dataset", return_value=mock.MagicMock()):
        result = monetio.load(source_name, files="dummy.nc")
        assert result is not None


# ---------------------------------------------------------------------------
# Verify cdump2netcdf.py has no deprecation infrastructure
# ---------------------------------------------------------------------------


def test_cdump2netcdf_has_no_deprecation():
    """cdump2netcdf.py should not import or use deprecation infrastructure."""
    mod = importlib.import_module("monetio.models.cdump2netcdf")
    source = inspect.getsource(mod)

    assert "deprecated_wrapper" not in source, "cdump2netcdf.py should not use deprecated_wrapper"
    assert "_deprecation" not in source, "cdump2netcdf.py should not import from _deprecation"
    assert "DeprecationWarning" not in source, "cdump2netcdf.py should not emit DeprecationWarning"


# ---------------------------------------------------------------------------
# Verify epa_util.py has no deprecation infrastructure
# ---------------------------------------------------------------------------


def test_epa_util_has_no_deprecation():
    """epa_util.py should not import or use deprecation infrastructure."""
    mod = importlib.import_module("monetio.obs.epa_util")
    source = inspect.getsource(mod)

    assert "deprecated_wrapper" not in source, "epa_util.py should not use deprecated_wrapper"
    assert "_deprecation" not in source, "epa_util.py should not import from _deprecation"
    assert "DeprecationWarning" not in source, "epa_util.py should not emit DeprecationWarning"


# ---------------------------------------------------------------------------
# Verify READER_REGISTRY completeness
# ---------------------------------------------------------------------------


def test_reader_registry_has_all_sources():
    """READER_REGISTRY should contain all source names from _READER_MODULES."""
    from monetio.readers.base import READER_REGISTRY

    # Force all lazy imports
    for source_name, module_path in monetio._READER_MODULES.items():
        if source_name not in READER_REGISTRY:
            importlib.import_module(module_path, package="monetio")

    missing = set(monetio._READER_MODULES.keys()) - set(READER_REGISTRY.keys())
    assert not missing, f"READER_REGISTRY is missing sources: {missing}"
