"""US1 lazy-resolution acceptance tests (tasks.md T011).

Expands the previous one-line ``import monetio`` smoke test into the
normative assertions of ``contracts/lazy-resolution.md`` C-LAZY-2/3/4/6:
attribute resolution order, ``AttributeError`` behavior, star-import
completeness (R2 Defect B), alias identity, idempotent resolution,
no-double-registration for direct submodule imports, and preserved
deprecation-warning ``stacklevel``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import warnings
from pathlib import Path

import pytest

import monetio

_HERE = Path(__file__)
_ROOT = _HERE.parent.parent


# --- C-LAZY-2: resolution order ------------------------------------------


def test_real_attributes_are_never_shadowed():
    """Step 1: a real module attribute wins over any source lookup."""
    assert callable(monetio.load)
    assert callable(monetio.virtualize)
    assert callable(monetio.available_sources)
    assert monetio.__version__ == "0.2.7"
    for helper in (
        "rename_latlon",
        "rename_to_monet_latlon",
        "dataset_to_monet",
        "coards_to_netcdf",
    ):
        assert callable(getattr(monetio, helper)), helper


def test_legacy_facade_binds_wrapper_not_reader():
    """A source WITH a legacy path binds the facade module (has add_data)."""
    airnow = monetio.airnow
    assert airnow.__name__ == "monetio.obs.airnow"
    assert hasattr(airnow, "add_data")  # the deprecated facade surface (FR-001)


def test_source_without_facade_binds_reader():
    """A source with NO legacy path binds its reader module (step 2)."""
    assert monetio.gfs.__name__ == "monetio.readers.gfs"


def test_facade_source_binds_facade():
    """A source WITH a facade binds the wrapper (icap_mme -> models.icap_mme)."""
    assert monetio.icap_mme.__name__ == "monetio.models.icap_mme"


def test_package_submodule_binds_real_module():
    """Step 3: package submodules resolve to themselves (lazy, C-LAZY-1)."""
    assert monetio.grids.__name__ == "monetio.grids"
    assert monetio.util.__name__ == "monetio.util"


# --- FR-010: unknown attribute -------------------------------------------


def test_unknown_attribute_raises_attribute_error():
    with pytest.raises(AttributeError, match="no attribute"):
        monetio.airnoww


# --- R2 Defect B: star-import completeness -------------------------------


def test_star_import_includes_icap_mme():
    """``from monetio import *`` must succeed and include icap_mme.

    Historically this raised ``AttributeError: module 'monetio' has no
    attribute 'icap_mme'`` because it was in ``__all__`` but never bound.
    """
    ns: dict = {}
    exec("from monetio import *", ns)  # noqa: S102 - exercises the real star-import
    assert "icap_mme" in ns
    assert "airnow" in ns
    assert "load" in ns


# --- FR-014: alias identity ----------------------------------------------


def test_alias_attributes_resolve_to_same_module():
    assert monetio.gefs is monetio.gfs
    assert monetio.gdas is monetio.gfs
    assert monetio.viirs_jrr is monetio.nesdis_viirs_jrr


# --- C-LAZY-3 idempotency ------------------------------------------------


def test_repeated_access_does_not_reimport():
    """Second access is served from the bound global, not re-resolved."""
    first = monetio.cmaq
    mods_before = set(sys.modules)
    second = monetio.cmaq
    assert first is second
    assert "cmaq" in monetio.__dict__  # __getattr__ caches into globals()
    assert set(sys.modules) == mods_before


# --- C-LAZY-4: direct submodule import, no double registration -----------


def test_direct_submodule_import_no_double_register():
    """``import monetio.models.hysplit`` without a root-first access must
    leave exactly one registry entry for the source (spec edge case)."""
    code = (
        "import monetio.models.hysplit\n"
        "from monetio.readers.base import READER_REGISTRY\n"
        "import json\n"
        "print(json.dumps({'hysplit': 'hysplit' in READER_REGISTRY,\n"
        "                 'n': sum(1 for k in READER_REGISTRY if k == 'hysplit')}))"
    )
    proc = subprocess.run(
        [sys.executable, "-W", "ignore", "-c", code],
        capture_output=True,
        text=True,
        cwd=_ROOT,
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["hysplit"] is True
    assert result["n"] == 1


# --- C-LAZY-6: deprecation stacklevel ------------------------------------


def test_deprecation_warning_points_at_caller():
    """The lazy facade must not shift the warning's stacklevel."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            monetio.cmaq.open_dataset("does-not-exist.nc")
        except Exception:  # noqa: BLE001 - load may fail; the warning fires first
            pass
    dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert dep, "no DeprecationWarning emitted"
    # stacklevel=2 in the wrapper -> the warning's filename is THIS test file,
    # not _deprecation.py or the facade module.
    assert Path(dep[0].filename).resolve() == _HERE.resolve(), dep[0].filename
