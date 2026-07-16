import numpy as np
import xarray as xr

from monetio.readers.modis_l2 import modis_l2_preprocess


def test_modis_l2_preprocess_eager_lazy_consistency():
    """Test that MODIS L2 preprocessing is consistent between eager and lazy backends."""
    # 1. Create mock MODIS-like data
    n_rows = 10
    n_cols = 5

    # Coordinates
    lat = np.linspace(30, 40, n_rows)
    lon = np.linspace(-100, -90, n_cols)
    lon_2d, lat_2d = np.meshgrid(lon, lat)

    # Data Variables
    # AOD: 0 to 1 with some "bad" values
    aod_data = np.random.rand(n_rows, n_cols).astype(np.float32)
    aod_data[0, 0] = -0.05  # Below minimum
    aod_data[1, 1] = 1.5  # Above maximum

    # Quality Flag: 0 to 3
    qf_data = np.random.randint(0, 4, size=(n_rows, n_cols)).astype(np.int8)
    qf_data[2, 2] = 0  # Should be masked if thresh=1
    qf_data[3, 3] = 3  # Should be kept if thresh=3

    # Scan Start Time: seconds since 1993-01-01
    # 2023-01-01 00:00:00 is 946684800 seconds after 1993-01-01
    time_data = np.full((n_rows,), 946684800.0)

    ds_base = xr.Dataset(
        data_vars={
            "AOD": (("Cell_Along_Swath", "Cell_Across_Swath"), aod_data),
            "Quality_Assurance": (("Cell_Along_Swath", "Cell_Across_Swath"), qf_data),
            "Scan_Start_Time": (("Cell_Along_Swath",), time_data),
        },
        coords={
            "Latitude": (("Cell_Along_Swath", "Cell_Across_Swath"), lat_2d),
            "Longitude": (("Cell_Along_Swath", "Cell_Across_Swath"), lon_2d),
        },
    )

    variable_dict = {
        "AOD": {"scale": 1.0, "minimum": 0.0, "maximum": 1.0, "quality_flag": 1},
        "Quality_Assurance": {},
    }

    # 2. Run Eager (NumPy)
    ds_eager = modis_l2_preprocess(ds_base.copy(), variable_dict=variable_dict)

    # 3. Run Lazy (Dask)
    ds_lazy = modis_l2_preprocess(
        ds_base.chunk({"Cell_Along_Swath": 5}).copy(), variable_dict=variable_dict
    )

    # 4. Assertions
    # Check that 'time' coordinate was added
    assert "time" in ds_eager.coords
    assert "time" in ds_lazy.coords
    # Allow for different datetime64 precision [s] vs [ns] depending on pandas/numpy version
    assert ds_eager.time.dtype.kind == "M"
    assert ds_lazy.time.dtype.kind == "M"

    # Check dimensions were renamed
    assert "y" in ds_eager.dims
    assert "x" in ds_eager.dims
    assert "y" in ds_lazy.dims
    assert "x" in ds_lazy.dims

    # Check AOD values
    # Eager check
    assert np.isnan(ds_eager.AOD.values[0, 0])  # < 0.0
    assert np.isnan(ds_eager.AOD.values[1, 1])  # > 1.0
    if qf_data[2, 2] < 1:
        assert np.isnan(ds_eager.AOD.values[2, 2])
    else:
        assert not np.isnan(ds_eager.AOD.values[2, 2])

    # Consistency check
    xr.testing.assert_allclose(ds_eager, ds_lazy.compute())

    # Ensure history was updated
    assert (
        "Preprocessed MODIS L2 data using standardized preprocessing." in ds_eager.attrs["history"]
    )
    assert (
        "Preprocessed MODIS L2 data using standardized preprocessing." in ds_lazy.attrs["history"]
    )


def test_modis_l2_multiple_quality_flags():
    """Test that multiple quality flags are applied correctly."""
    n_rows, n_cols = 5, 5
    data = np.ones((n_rows, n_cols))
    qf1 = np.zeros((n_rows, n_cols))
    qf1[0, 0] = 3  # Only (0,0) passes qf1 >= 3

    qf2 = np.zeros((n_rows, n_cols))
    qf2[0, 0] = 3
    qf2[1, 1] = 0  # (1,1) fails qf2 >= 1
    qf2[:, :] = 3
    qf2[1, 1] = 0

    ds = xr.Dataset(
        data_vars={
            "VAR": (("Cell_Along_Swath", "Cell_Across_Swath"), data),
            "QF1": (("Cell_Along_Swath", "Cell_Across_Swath"), qf1),
            "QF2": (("Cell_Along_Swath", "Cell_Across_Swath"), qf2),
        },
        coords={
            "Latitude": (("Cell_Along_Swath", "Cell_Across_Swath"), np.ones((n_rows, n_cols))),
            "Longitude": (("Cell_Along_Swath", "Cell_Across_Swath"), np.ones((n_rows, n_cols))),
        },
    )

    vdict = {
        "VAR": {"quality_flag": 0},  # Dummy
        "QF1": {"quality_flag": 3},
        "QF2": {"quality_flag": 1},
    }

    ds_proc = modis_l2_preprocess(ds, variable_dict=vdict)

    # (0,0) should be 1.0 (passes both)
    assert ds_proc.VAR.values[0, 0] == 1.0
    # (1,1) should be NaN (fails QF2)
    assert np.isnan(ds_proc.VAR.values[1, 1])
    # (2,2) should be NaN (fails QF1)
    assert np.isnan(ds_proc.VAR.values[2, 2])
