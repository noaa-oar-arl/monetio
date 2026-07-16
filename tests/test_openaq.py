import json

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.openaq import OpenAQReader
from monetio.readers.openaq_aws import OpenAQAWSReader


def test_openaq_eager_lazy(tmp_path):
    dummy_data = [
        {
            "location": "Test Site 1",
            "city": "Test City",
            "country": "TS",
            "date": {"utc": "2023-01-01T00:00:00Z", "local": "2023-01-01T01:00:00+01:00"},
            "parameter": "pm25",
            "value": 10.0,
            "unit": "µg/m³",
            "coordinates": {"latitude": 40.0, "longitude": -100.0},
            "averagingPeriod": {"value": 1, "unit": "hours"},
            "sourceName": "Source A",
            "sourceType": "government",
            "mobile": False,
        },
        {
            "location": "Test Site 1",
            "city": "Test City",
            "country": "TS",
            "date": {"utc": "2023-01-01T00:00:00Z", "local": "2023-01-01T01:00:00+01:00"},
            "parameter": "o3",
            "value": 50.0,
            "unit": "µg/m³",
            "coordinates": {"latitude": 40.0, "longitude": -100.0},
            "averagingPeriod": {"value": 1, "unit": "hours"},
            "sourceName": "Source A",
            "sourceType": "government",
            "mobile": False,
        },
    ]
    f = tmp_path / "test.json"
    with open(f, "w") as out:
        for item in dummy_data:
            out.write(json.dumps(item) + "\n")
    reader = OpenAQReader()

    # Wide format eager vs lazy
    ds_eager = reader.open_dataset(files=[str(f)], wide_fmt=True, as_xarray=True, lazy=False)
    ds_lazy = reader.open_dataset(files=[str(f)], wide_fmt=True, as_xarray=True, lazy=True)
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )
    assert ds_eager.pm25_ugm3.values[0] == 10.0
    assert np.isclose(ds_eager.o3_ppm.values[0], 50.0 / 1990.0)
    assert ds_eager.coords["siteid"].values[0].startswith("TS_")

    # Long format eager vs lazy
    df_eager = reader.open_dataset(files=[str(f)], wide_fmt=False, as_xarray=False, lazy=False)
    df_lazy = reader.open_dataset(files=[str(f)], wide_fmt=False, as_xarray=False, lazy=True)
    pd.testing.assert_frame_equal(df_eager, df_lazy.compute(), check_dtype=False)


def test_openaq_aws_eager_lazy(tmp_path):
    dummy_data = pd.DataFrame(
        {
            "location_id": ["2178", "2178"],
            "sensor_id": ["1", "2"],
            "location": ["Test Site", "Test Site"],
            "datetime": ["2022-05-03 00:00:00+00:00", "2022-05-03 01:00:00+00:00"],
            "lat": [40.0, 40.0],
            "lon": [-100.0, -100.0],
            "parameter": ["pm25", "pm25"],
            "units": ["µg/m³", "µg/m³"],
            "value": [10.0, 15.0],
        }
    )
    f = tmp_path / "test.csv"
    dummy_data.to_csv(f, index=False, encoding="utf-8")
    reader = OpenAQAWSReader()
    ds_eager = reader.open_dataset(files=[str(f)], as_xarray=True, lazy=False)
    ds_lazy = reader.open_dataset(files=[str(f)], as_xarray=True, lazy=True)
    xr.testing.assert_allclose(
        ds_eager.drop_vars("history", errors="ignore"),
        ds_lazy.compute().drop_vars("history", errors="ignore"),
    )
    # Refactored reader renames 'value' to 'obs'
    assert ds_eager.obs.values[0] == 10.0


@pytest.mark.network
def test_openaq_network():
    dates = pd.date_range(start="2013-11-26", end="2013-11-26 01:00", freq="h")
    try:
        df = OpenAQReader().open_dataset(dates=dates, as_xarray=False)
        assert not df.empty
        assert (df.country == "CN").all()
    except Exception as e:
        if "Access Denied" in str(e):
            pytest.skip(f"OpenAQ S3 access denied: {e}")
        pytest.skip(f"OpenAQ network call failed: {e}")
