"""Station readers share one UGRID layout before and after expansion."""

import numpy as np
import pandas as pd
import pytest

from monetio._lazy import get_reader_class
from monetio._manifest import SOURCE_MANIFEST
from monetio.readers.aeronet import AERONETReader
from monetio.readers.ish import ISH, ISHReader
from monetio.readers.ish_lite import ISHLiteReader


@pytest.mark.parametrize("reader_class", [ISHReader, ISHLiteReader, AERONETReader])
@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
def test_station_readers_return_time_node_layout(monkeypatch, ish_history, reader_class, lazy):
    times = pd.date_range("2020-09-01", periods=2, freq="h")
    frame = pd.DataFrame(
        {
            "time": np.repeat(times, 2),
            "siteid": list(ish_history.station_id) * 2,
            "measurement": [1.0, 2.0, 3.0, 4.0],
        }
    )
    if reader_class is AERONETReader:
        frame["latitude"] = list(ish_history.latitude) * 2
        frame["longitude"] = list(ish_history.longitude) * 2
    monkeypatch.setattr(
        ISH,
        "read_ish_history",
        lambda self, dates=None: setattr(self, "history", ish_history.copy()),
    )
    if lazy:
        import dask.dataframe as dd

        frame = dd.from_pandas(frame, npartitions=2)
    reader = reader_class()
    monkeypatch.setattr(reader.driver, "open", lambda *args, **kwargs: frame.copy())

    result = reader.open_dataset(files=["station-fixture.csv"], as_xarray=True, lazy=lazy)

    assert dict(result.sizes) == {"time": 2, "node": 2}
    assert result.measurement.dims == ("time", "node")
    assert result.latitude.dims == result.longitude.dims == result.siteid.dims == ("node",)
    assert result.mesh.attrs["cf_role"] == "mesh_topology"
    assert result.mesh.attrs["node_coordinates"] == "longitude latitude"
    assert result.measurement.attrs["mesh"] == "mesh"
    assert result.measurement.attrs["location"] == "node"
    np.testing.assert_array_equal(result.measurement.compute(), [[1.0, 2.0], [3.0, 4.0]])


def test_explicit_unexpanded_points_keep_node_dimension():
    frame = pd.DataFrame(
        {
            "time": pd.to_datetime(["2020-09-01", "2020-09-01"]),
            "siteid": ["A", "B"],
            "latitude": [30.0, 40.0],
            "longitude": [-90.0, -80.0],
            "measurement": [1.0, 2.0],
        }
    )
    result = ISHLiteReader().to_xarray(frame, expand2d=False)
    assert dict(result.sizes) == {"node": 2}
    assert result.time.dims == ("node",)


@pytest.mark.parametrize("source", [spec.name for spec in SOURCE_MANIFEST if spec.kind == "point"])
@pytest.mark.parametrize("expanded", [False, True], ids=["records", "time-node"])
@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
def test_all_point_reader_converters_share_dimensions(source, expanded, lazy):
    frame = pd.DataFrame(
        {
            "time": np.repeat(pd.date_range("2020-09-01", periods=2, freq="h"), 2),
            "siteid": ["A", "B", "A", "B"],
            "latitude": [30.0, 40.0, 30.0, 40.0],
            "longitude": [-90.0, -80.0, -90.0, -80.0],
            "measurement": [1.0, 2.0, 3.0, 4.0],
        }
    )
    if lazy:
        import dask.dataframe as dd

        frame = dd.from_pandas(frame, npartitions=2)
    reader = get_reader_class(source)()
    result = reader.to_xarray(frame, expand2d=expanded)
    assert dict(result.sizes) == ({"time": 2, "node": 2} if expanded else {"node": 4})
    assert result.measurement.dims == (("time", "node") if expanded else ("node",))
    assert result.siteid.dims == ("node",)
    if not expanded:
        assert result.time.dims == ("node",)
    elif reader.fixed_location:
        assert result.latitude.dims == result.longitude.dims == ("node",)
    assert result.measurement.attrs["mesh"] == "mesh"
    assert result.measurement.attrs["location"] == "node"
    np.testing.assert_array_equal(
        result.measurement.compute(), [[1.0, 2.0], [3.0, 4.0]] if expanded else [1.0, 2.0, 3.0, 4.0]
    )
