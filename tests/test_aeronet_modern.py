import numpy as np
import pandas as pd
import pytest

from monetio.readers.aeronet import _calc_new_aod_values, _vectorized_tspack_interp


def test_vectorized_tspack_interp_basic():
    """Test basic interpolation with mock data."""
    # Source wavelengths
    wvs = np.array([440.0, 500.0, 675.0, 870.0])
    # Mock AOD values (2 points, 4 wvs)
    # Using a simple linear-ish relationship for testing
    aods = np.array([[1.0, 0.8, 0.5, 0.3], [0.5, 0.4, 0.25, 0.15]])
    new_wvs = np.array([550.0, 600.0])

    # We need to mock pytspack since it is not installed in the environment
    # but we want to test the vectorization logic.
    class MockInterp:
        def __call__(self, x):
            # Just return something based on x to verify it's called
            return np.ones_like(x) * 0.7

    class MockTsPack:
        def interpolate(self, x, y):
            return MockInterp()

    import sys
    from unittest.mock import MagicMock

    mock_pytspack = MagicMock()
    mock_pytspack.TsPack.return_value = MockTsPack()

    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "pytspack", mock_pytspack)

        res = _vectorized_tspack_interp(wvs, aods, new_wvs)

        assert res.shape == (2, 2)
        assert np.allclose(res, 0.7)


def test_calc_new_aod_values_pandas():
    """Test pandas integration of new AOD calculation."""
    df = pd.DataFrame(
        {
            "aod_440nm": [1.0, 0.5],
            "aod_500nm": [0.8, 0.4],
            "aod_675nm": [0.5, 0.25],
            "aod_870nm": [0.3, 0.15],
        }
    )
    new_wv = [550, 600]

    # Mock pytspack
    class MockInterp:
        def __call__(self, x):
            return np.ones_like(x) * 0.7

    class MockTsPack:
        def interpolate(self, x, y):
            return MockInterp()

    import sys
    from unittest.mock import MagicMock

    mock_pytspack = MagicMock()
    mock_pytspack.TsPack.return_value = MockTsPack()

    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "pytspack", mock_pytspack)

        res = _calc_new_aod_values(df, new_wv)

        assert "aod_550nm" in res.columns
        assert "aod_600nm" in res.columns
        assert res["aod_550nm"].iloc[0] == 0.7
        assert len(res.columns) == 6  # 4 original + 2 new
        assert "Interpolated AOD to new wavelengths" in res.attrs.get("history", "")


def test_calc_new_aod_values_eager_lazy_consistency():
    """Test consistency between Eager (Pandas) and Lazy (Dask) backends."""
    df = pd.DataFrame(
        {
            "aod_440nm": [1.0, 0.5, 0.8],
            "aod_500nm": [0.8, 0.4, 0.6],
            "aod_675nm": [0.5, 0.25, 0.4],
            "aod_870nm": [0.3, 0.15, 0.2],
        }
    )
    new_wv = [550]

    # Mock pytspack
    class MockInterp:
        def __call__(self, x):
            # Return mean AOD as a dummy interpolation
            return np.mean(x) * np.ones_like(x)

    class MockTsPack:
        def interpolate(self, x, y):
            return MockInterp()

    import sys
    from unittest.mock import MagicMock

    mock_pytspack = MagicMock()
    mock_pytspack.TsPack.return_value = MockTsPack()

    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "pytspack", mock_pytspack)

        # 1. Eager (Pandas)
        res_eager = _calc_new_aod_values(df, new_wv)

        # 2. Lazy (Dask)
        import dask.dataframe as dd

        ddf = dd.from_pandas(df, npartitions=2)
        # In AERONETReader, _calc_new_aod_values is called inside map_partitions via read_aeronet_csv
        res_lazy = ddf.map_partitions(_calc_new_aod_values, new_wv=new_wv).compute()

        # Compare
        pd.testing.assert_frame_equal(res_eager, res_lazy)


def test_calc_new_aod_values_no_pytspack():
    """Test that it raises RuntimeError if pytspack is missing."""
    df = pd.DataFrame({"aod_440nm": [1.0]})
    import sys

    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "pytspack", None)
        with pytest.raises(RuntimeError, match="You must install pytspack"):
            _calc_new_aod_values(df, [550])


def test_vectorized_tspack_interp_with_nans():
    """Test that NaNs are handled correctly."""
    wvs = np.array([440.0, 500.0, 675.0, 870.0])
    aods = np.array(
        [
            [1.0, np.nan, 0.5, 0.3],  # Valid (2+ points)
            [1.0, np.nan, np.nan, np.nan],  # Invalid (<2 points)
        ]
    )
    new_wvs = np.array([550.0])

    class MockInterp:
        def __call__(self, x):
            return np.array([0.7])

    class MockTsPack:
        def interpolate(self, x, y):
            # Verify that we only got the non-nan points
            assert len(x) >= 2
            return MockInterp()

    import sys
    from unittest.mock import MagicMock

    mock_pytspack = MagicMock()
    mock_pytspack.TsPack.return_value = MockTsPack()

    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(sys.modules, "pytspack", mock_pytspack)
        res = _vectorized_tspack_interp(wvs, aods, new_wvs)

        assert res.shape == (2, 1)
        assert res[0, 0] == 0.7
        assert np.isnan(res[1, 0])
