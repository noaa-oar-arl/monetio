import numpy as np
import pytest
import xarray as xr

from monetio.readers.base import _scientific_hygiene


def create_test_ds():
    """Create a test dataset for hygiene checks."""
    ds = xr.Dataset(
        data_vars={
            "O3": (
                ("time", "y", "x"),
                np.ones((2, 3, 3)),
                {"units": " ppb ", "long_name": " Ozone "},
            ),
            "latitude": (("y", "x"), np.zeros((3, 3)), {"units": "degrees_north"}),
            "longitude": (("y", "x"), np.zeros((3, 3)), {"units": "degrees_east"}),
        },
        coords={
            "time": (("time",), [0, 1], {"units": "hours since 2023-01-01"}),
            "z": (("z",), [0, 1, 2], {"units": "m"}),
        },
        attrs={"project": " MONETIO ", "history": "Created."},
    )
    return ds


@pytest.mark.parametrize("lazy", [False, True])
def test_scientific_hygiene_logic(lazy):
    """Test _scientific_hygiene with Eager and Lazy backends."""
    ds = create_test_ds()
    if lazy:
        ds = ds.chunk({"time": 1})

    ds_clean = _scientific_hygiene(ds)

    # 1. Verify Standard Coordinates are set
    assert "latitude" in ds_clean.coords
    assert "longitude" in ds_clean.coords
    assert "time" in ds_clean.coords

    # 2. Verify Non-Standard Coordinates are PRESERVED
    # 'z' was already a coord, it should stay one
    assert "z" in ds_clean.coords

    # 3. Verify Whitespace Stripping
    assert ds_clean.O3.attrs["units"] == "ppb"
    assert ds_clean.O3.attrs["long_name"] == "Ozone"
    assert ds_clean.attrs["project"] == "MONETIO"

    # 4. Verify History Update
    assert "Applied scientific hygiene" in ds_clean.attrs["history"]
    assert ds_clean.attrs["history"].startswith("Created.")

    # 5. Verify Backend preservation
    if lazy:
        assert ds_clean.O3.chunks is not None
    else:
        assert ds_clean.O3.chunks is None


def test_scientific_hygiene_consistency():
    """Explicitly verify Eager/Lazy consistency for hygiene."""
    ds_eager = create_test_ds()
    ds_lazy = ds_eager.chunk({"time": 1})

    res_eager = _scientific_hygiene(ds_eager)
    res_lazy = _scientific_hygiene(ds_lazy).compute()

    xr.testing.assert_allclose(res_eager, res_lazy)
    # Check attributes specifically since assert_allclose might skip them depending on options
    assert res_eager.attrs == res_lazy.attrs
    for var in res_eager.data_vars:
        assert res_eager[var].attrs == res_lazy[var].attrs
