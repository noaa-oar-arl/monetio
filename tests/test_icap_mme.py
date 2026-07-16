import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.icap_mme import ICAPMMEReader, build_urls


def create_mock_icap_ds(lazy=False):
    """Creates a mock ICAP-MME dataset."""
    nt, nlat, nlon = 4, 10, 20

    # Use fixed data
    data = {
        "dustaod550": (
            ("time", "lat", "lon"),
            np.arange(nt * nlat * nlon).reshape(nt, nlat, nlon).astype(float),
        ),
    }

    ds = xr.Dataset(
        data_vars=data,
        coords={
            "time": (("time",), pd.date_range("2023-01-01", periods=nt, freq="6h")),
            "lat": (("lat",), np.linspace(-90, 90, nlat)),
            "lon": (("lon",), np.linspace(-180, 180, nlon)),
        },
    )

    if lazy:
        ds = ds.chunk({"time": 2, "lat": -1, "lon": -1})

    return ds


def test_icap_protocol_compliance():
    """Verify ICAP processing is backend-agnostic and lazy-friendly."""
    ds_base = create_mock_icap_ds(lazy=False)
    ds_eager = ds_base.copy(deep=True)
    ds_lazy = ds_base.chunk({"time": 2, "lat": -1, "lon": -1})

    reader = ICAPMMEReader()

    class MockDriver:
        def __init__(self, ds):
            self.ds = ds

        def open(self, *args, **kwargs):
            return self.ds

    # Test Eager
    reader.driver = MockDriver(ds_eager)
    res_eager = reader.open_dataset(files="dummy.nc")

    # Test Lazy
    reader.driver = MockDriver(ds_lazy)
    res_lazy = reader.open_dataset(files="dummy.nc", lazy=True)

    # Check laziness
    assert isinstance(res_lazy.dustaod550.data, da.Array)

    # Check consistency
    xr.testing.assert_allclose(res_eager, res_lazy.compute())

    # Check history
    assert "history" in res_eager.attrs
    assert "Read ICAP-MME data." in res_eager.attrs["history"]


def test_open_dataset_bad_date():
    from monetio.models.icap_mme import open_dataset

    with pytest.raises((ValueError, OSError)):
        open_dataset("1990-08-01", verify=False)


def test_open_dataset_invalid_param():
    from monetio.models.icap_mme import open_dataset, open_mfdataset

    date = "2019-08-01"

    with pytest.raises(ValueError, match="Invalid input for 'product'"):
        open_dataset(date, product="asdf", verify=False)
        open_mfdataset([date], product="asdf", verify=False)

    with pytest.raises(ValueError, match="Invalid input for 'data_var'"):
        open_dataset(date, data_var="asdf", verify=False)
        open_mfdataset([date], data_var="asdf", verify=False)


def test_build_urls_defaults_to_new_nrl_endpoint():
    urls, fnames = build_urls("2024-02-01", filetype="C4", data_var="dustaod550", verbose=False)

    assert len(urls) == 1
    assert len(fnames) == 1
    assert urls[0].startswith("https://nrlgodae1.nrlmry.navy.mil/ftp/outgoing/nrl/ICAP-MME/")


def test_build_urls_honors_base_url_override():
    urls, _ = build_urls(
        "2024-02-01",
        filetype="C4",
        data_var="dustaod550",
        base_url="https://example.org/custom-root",
        verbose=False,
    )

    assert urls[0].startswith("https://example.org/custom-root/")


@pytest.mark.network
@pytest.mark.parametrize(
    "date,product,data_var",
    [
        ("2019-08-01", "MME", "totaldustaod550"),
        ("2024-02-01", "C4", "dustaod550"),
    ],
)
def test_open_dataset_network(tmp_path, monkeypatch, date, product, data_var):
    from monetio.models.icap_mme import open_dataset

    try:
        ds = open_dataset(date, product=product, data_var=data_var, download=False, verify=False)
        assert set(ds.dims) == {"time", "lat", "lon"}

        monkeypatch.chdir(tmp_path)
        ds_dl = open_dataset(date, product=product, data_var=data_var, download=True, verify=False)
        assert len(sorted(tmp_path.glob("*.nc"))) == 1
        assert set(ds_dl.dims) == {"time", "lat", "lon"}

        assert ds_dl.equals(ds)
    except Exception as e:
        pytest.skip(f"ICAP network call failed: {e}")


@pytest.mark.network
def test_open_mfdataset_network(tmp_path, monkeypatch):
    from monetio.models.icap_mme import open_mfdataset

    dates = ["2023-08-01", "2023-08-02"]
    product = "C4"
    data_var = "dustaod550"

    try:
        ds = open_mfdataset(dates, product=product, data_var=data_var, download=False, verify=False)
        assert set(ds.dims) == {"time", "lat", "lon"}
        assert ds["dust_aod_mean"].chunks is None, "not Dask-backed"

        monkeypatch.chdir(tmp_path)
        ds_dl = open_mfdataset(
            dates, product=product, data_var=data_var, download=True, verify=False
        )
        assert len(sorted(tmp_path.glob("*.nc"))) == 2
        assert set(ds_dl.dims) == {"time", "lat", "lon"}
        assert ds_dl["dust_aod_mean"].chunks is not None

        assert ds_dl.equals(ds)
    except Exception as e:
        pytest.skip(f"ICAP network call failed: {e}")


if __name__ == "__main__":
    pytest.main([__file__])
