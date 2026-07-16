from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import xarray as xr

from monetio.readers.iagos import IAGOSReader
from monetio.readers.ndacc import NDACCReader


def test_iagos_reader_init():
    reader = IAGOSReader()
    assert reader.fixed_location is False


def test_iagos_open_dataset_empty():
    reader = IAGOSReader()
    with pytest.raises(ValueError, match="Must provide either 'files' or 'dates'"):
        reader.open_dataset()


def test_ndacc_open_dataset_empty():
    reader = NDACCReader()
    with pytest.raises(ValueError, match="Must provide either 'files' or 'dates'"):
        reader.open_dataset()


def test_iagos_harmonize():
    reader = IAGOSReader()
    ds = xr.Dataset(
        {
            "lon": (("time"), [1.0]),
            "lat": (("time"), [2.0]),
            "o3": (("time"), [3.0]),
        },
        coords={"time": [pd.Timestamp("2023-01-01")]},
    )

    # Set units to ppb as expected by harmonize
    ds["o3"].attrs["units"] = "ppb"

    ds_h = reader.harmonize(ds)
    assert "longitude" in ds_h.variables
    assert "latitude" in ds_h.variables
    assert "ozone" in ds_h.variables
    assert ds_h["ozone"].attrs["units"] == "ppb"
    assert ds_h["ozone"].attrs["standard_name"] == "mole_fraction_of_ozone_in_air"


def test_ndacc_harmonize():
    reader = NDACCReader()
    ds = xr.Dataset(
        {
            "o3_mixing_ratio_volume": (("time"), [10.0]),
        },
        coords={"time": [pd.Timestamp("2023-01-01")]},
    )

    ds_h = reader.harmonize(ds)
    assert "ozone" in ds_h.variables
    assert ds_h["ozone"].values[0] == 10.0


def test_iagos_build_urls_no_key():
    reader = IAGOSReader()
    with pytest.warns(UserWarning, match="IAGOS retrieval requires an API key"):
        urls = reader.build_urls(pd.to_datetime(["2023-01-01"]))
        assert urls == []


def test_ndacc_build_urls_no_site():
    reader = NDACCReader()
    with pytest.warns(UserWarning, match="NDACC retrieval requires 'siteid'"):
        urls = reader.build_urls(pd.to_datetime(["2023-01-01"]))
        assert urls == []


@patch("fsspec.filesystem")
def test_ndacc_build_urls_mock(mock_fs):
    reader = NDACCReader()
    mock_instance = MagicMock()
    mock_fs.return_value = mock_instance
    mock_instance.ls.return_value = [
        "https://www-air.larc.nasa.gov/pub/NDACC/PUBLIC/stations/mauna.loa.hi/ftir/h2o_ftir_maunaloa_20230101t120000z_001.h5",
        "https://www-air.larc.nasa.gov/pub/NDACC/PUBLIC/stations/mauna.loa.hi/ftir/h2o_ftir_maunaloa_20230102t120000z_001.h5",
    ]

    dates = pd.to_datetime(["2023-01-01"])
    urls = reader.build_urls(dates, siteid="mauna.loa.hi", instrument="ftir")

    assert len(urls) == 1
    assert "20230101" in urls[0]
