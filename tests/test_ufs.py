import itertools
from dataclasses import dataclass
from pathlib import Path

import dask.array as da
import pytest
import xarray as xr

from monetio.models.ufs import open_mfdataset


@dataclass
class DataForTest:
    surf_only: bool
    expected_nz: int
    expected_to_be_loaded: tuple[str, ...] = ("dz_m", "surfalt_m", "pres_pa_mid", "alt_msl_m_full")


@pytest.mark.parametrize(
    "test_data",
    [
        DataForTest(surf_only=True, expected_nz=1),
        DataForTest(surf_only=False, expected_nz=64),
    ],
    ids=lambda x: f"surf_only={x.surf_only}",
)
def test_open_mfdataset(data_dir: Path, test_data: DataForTest) -> None:
    ufs_data_dir = data_dir / "ufs"
    actual = open_mfdataset(str(ufs_data_dir / "aqm.t12z.dyn.f*.nc"), surf_only=test_data.surf_only)

    for var in actual.data_vars.values():
        shape_dict = {dim: actual.sizes[dim] for dim in var.dims}
        # Assert there is only one level when extracting surface data
        if "z" in shape_dict:
            assert shape_dict["z"] == test_data.expected_nz
        try:
            assert isinstance(var.data, da.Array)
        except AssertionError:
            # Some variables are loaded from disk for pre-processing or calculated at runtime
            assert var.name in test_data.expected_to_be_loaded

    if test_data.surf_only:
        assert "alt_msl_m_full" not in actual.data_vars
    else:
        assert "alt_msl_m_full" in actual.data_vars
        # Baseline is for full level profile
        _compare_with_baseline_(actual, ufs_data_dir / "baseline-20250514-1622.nc")


def test_deprecated_rrfs_cmaq_mm() -> None:
    from monetio.models._rrfs_cmaq_mm import open_mfdataset  # noqa: F401


def _compare_with_baseline_(actual: xr.Dataset, baseline_path: Path) -> None:
    with xr.open_dataset(baseline_path) as baseline:
        try:
            assert actual.identical(baseline)
        except AssertionError:
            # Let's dig into the variables a bit more
            for var_name, var in itertools.chain(actual.data_vars.items(), actual.coords.items()):
                try:
                    assert var.identical(baseline[var_name])
                except AssertionError:
                    print(var.to_series().describe())
                    raise
            # If there are no assertion issues here, then it's related to global attributes (probably)
            raise
