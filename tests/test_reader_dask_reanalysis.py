"""Local reanalysis reader tests for eager and Dask-backed execution."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from monetio.readers.era5 import ERA5Reader
from monetio.readers.merra2 import MERRA2Reader


def _reanalysis_data(variable, value):
    values = np.arange(24, dtype="f4").reshape(2, 3, 4) + value
    return xr.Dataset(
        {variable: (("time", "lat", "lon"), values)},
        coords={
            "time": pd.date_range("2024-01-01", periods=2, freq="h"),
            "lat": np.linspace(-30, 30, 3, dtype="f4"),
            "lon": np.linspace(0, 270, 4, dtype="f4"),
        },
    )


def _assert_parity(reader, dataset, path, variable):
    dataset.to_netcdf(path, engine="h5netcdf")
    eager = reader.open_dataset(str(path), engine="h5netcdf")
    lazy = reader.open_dataset(str(path), engine="h5netcdf", use_dask=True)
    assert hasattr(lazy[variable].data, "dask")
    # Reader-level Dask support is expected to preserve metadata and exact
    # values; approximate array-only comparison would hide provenance drift.
    xr.testing.assert_identical(eager, lazy.compute())


def test_era5_local_reader_use_dask_parity(tmp_path):
    _assert_parity(
        ERA5Reader(), _reanalysis_data("t2m", 270), tmp_path / "era5.nc", "temperature_2m"
    )


def test_era5_local_reader_honors_dask_chunks(tmp_path):
    path = tmp_path / "era5-chunks.nc"
    _reanalysis_data("t2m", 270).to_netcdf(path, engine="h5netcdf")
    result = ERA5Reader().open_dataset(
        str(path), engine="h5netcdf", use_dask=True, chunks={"time": 1, "lat": 2}
    )
    chunks = result["temperature_2m"].chunks
    assert chunks is not None
    assert chunks[result["temperature_2m"].get_axis_num("time")] == (1, 1)
    assert chunks[result["temperature_2m"].get_axis_num("y")] == (2, 1)
    result.close()


def test_merra2_local_reader_honors_dask_chunks(tmp_path):
    path = tmp_path / "merra2-chunks.nc"
    _reanalysis_data("PS", 90000).to_netcdf(path, engine="h5netcdf")
    result = MERRA2Reader().open_dataset(
        str(path), engine="h5netcdf", use_dask=True, chunks={"time": 1, "lat": 2}
    )
    chunks = result["surface_pressure"].chunks
    assert chunks is not None
    assert chunks[result["surface_pressure"].get_axis_num("time")] == (1, 1)
    assert chunks[result["surface_pressure"].get_axis_num("y")] == (2, 1)
    result.close()


def test_reanalysis_readers_reject_unknown_chunk_dimensions(tmp_path):
    path = tmp_path / "unknown-chunk-dim.nc"
    _reanalysis_data("t2m", 270).to_netcdf(path, engine="h5netcdf")
    with pytest.raises(ValueError, match="not_a_dimension"):
        ERA5Reader().open_dataset(
            str(path),
            engine="h5netcdf",
            use_dask=True,
            chunks={"not_a_dimension": 1},
        )


def test_reanalysis_multifile_rejects_unknown_chunk_dimensions(tmp_path):
    paths = []
    for index in range(2):
        path = tmp_path / f"era5-{index}.nc"
        _reanalysis_data("t2m", 270 + index).to_netcdf(path, engine="h5netcdf")
        paths.append(str(path))

    with pytest.raises(ValueError, match="not_a_dimension"):
        ERA5Reader().open_dataset(
            paths,
            engine="h5netcdf",
            use_dask=True,
            chunks={"not_a_dimension": 1},
            combine="nested",
            concat_dim="time",
        )


def test_merra2_local_reader_use_dask_parity(tmp_path):
    _assert_parity(
        MERRA2Reader(), _reanalysis_data("PS", 90000), tmp_path / "merra2.nc", "surface_pressure"
    )


@pytest.mark.parametrize(
    "reader,source_variable,output_variable",
    [
        (ERA5Reader, "t2m", "temperature_2m"),
        (MERRA2Reader, "PS", "surface_pressure"),
    ],
)
def test_reanalysis_readers_select_requested_variables(
    tmp_path, reader, source_variable, output_variable
):
    path = tmp_path / f"{source_variable}-selective.nc"
    source = _reanalysis_data(source_variable, 270).assign(
        unrelated=("time", np.array([99, 99], dtype="f4"))
    )
    source.to_netcdf(path, engine="h5netcdf")

    result = reader().open_dataset(
        str(path), engine="h5netcdf", use_dask=True, drop_variables=["unrelated"]
    )
    try:
        assert "unrelated" not in result
        assert hasattr(result[output_variable].data, "dask")
        np.testing.assert_array_equal(
            result[output_variable].compute().values,
            source[source_variable].values,
        )
    finally:
        result.close()
