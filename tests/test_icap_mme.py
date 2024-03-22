import pytest

from monetio.models.icap_mme import open_dataset, open_mfdataset


def test_open_dataset_bad_date():
    with pytest.raises(ValueError, match="File does not exist"):
        open_dataset("1990-08-01")


def test_open_dataset_invalid_param():
    date = "2019-08-01"

    with pytest.raises(ValueError, match="Invalid input for 'product'"):
        open_dataset(date, product="asdf")
        open_mfdataset([date], product="asdf")

    with pytest.raises(ValueError, match="Invalid input for 'data_var'"):
        open_dataset(date, data_var="asdf")
        open_mfdataset([date], data_var="asdf")


@pytest.mark.parametrize(
    "date,product,data_var",
    [
        ("2019-08-01", "MME", "totaldustaod550"),
        ("2024-02-01", "C4", "dustaod550"),
    ],
)
def test_open_dataset(tmp_path, monkeypatch, date, product, data_var):
    ds = open_dataset(date, product=product, data_var=data_var, download=False)
    assert set(ds.dims) == {"time", "lat", "lon"}

    monkeypatch.chdir(tmp_path)
    ds_dl = open_dataset(date, product=product, data_var=data_var, download=True)
    assert len(sorted(tmp_path.glob("*.nc"))) == 1
    assert set(ds_dl.dims) == {"time", "lat", "lon"}

    assert ds_dl.equals(ds)


def test_open_mfdataset(tmp_path, monkeypatch):
    dates = ["2023-08-01", "2023-08-02"]
    product = "C4"
    data_var = "dustaod550"

    ds = open_mfdataset(dates, product=product, data_var=data_var, download=False)
    assert set(ds.dims) == {"time", "lat", "lon"}
    assert ds["dust_aod_mean"].chunks is None, "not Dask-backed"
    assert (
        ~ds.time.to_series().duplicated(keep=False)
    ).sum() == 8, "all overlap except first and last day"

    monkeypatch.chdir(tmp_path)
    ds_dl = open_mfdataset(dates, product=product, data_var=data_var, download=True)
    assert len(sorted(tmp_path.glob("*.nc"))) == 2
    assert set(ds_dl.dims) == {"time", "lat", "lon"}
    assert ds_dl["dust_aod_mean"].chunks is not None

    assert ds_dl.equals(ds)
