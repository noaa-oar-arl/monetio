import gzip
from unittest.mock import patch

import pandas as pd
import pytest
import xarray as xr
from click.testing import CliRunner

from monetio.cli import cli


@pytest.fixture
def mock_ish_history():
    return pd.DataFrame(
        {
            "station_id": ["72224400358"],
            "usaf": ["722244"],
            "wban": ["00358"],
            "latitude": [38.9],
            "longitude": [-76.9],
            "ctry": ["US"],
            "state": ["MD"],
            "station name": ["Site 1"],
            "elev(m)": [20.0],
            "begin": [pd.Timestamp("1970-01-01")],
            "end": [pd.Timestamp("2025-01-01")],
        }
    )


def test_cli_to_zarr_and_append(tmp_path, mock_ish_history):
    import importlib.util

    if importlib.util.find_spec("zarr") is None:
        pytest.skip("zarr not installed")

    # Setup mock data - Step 1
    line1 = "2020 09 01 00  256  184 10185  220   40 0 0 0"
    fn1 = tmp_path / "722244-00358-2020_1.gz"
    with gzip.open(fn1, "wb") as f:
        f.write((line1 + "\n").encode())

    output_zarr = tmp_path / "test_output.zarr"

    runner = CliRunner()

    def side_effect(self, dates=None):
        self.history = mock_ish_history

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        # 1. Initial save
        result = runner.invoke(cli, ["to-zarr", "ish_lite", "-f", str(fn1), "-o", str(output_zarr)])
        assert result.exit_code == 0
        assert "Saved to" in result.output

        # 2. Setup more data for append
        line2 = "2020 09 01 01  260  190 10190  220   45 0 0 0"
        fn2 = tmp_path / "722244-00358-2020_2.gz"
        with gzip.open(fn2, "wb") as f:
            f.write((line2 + "\n").encode())

        # 3. Append
        result = runner.invoke(
            cli, ["to-zarr", "ish_lite", "-f", str(fn2), "-o", str(output_zarr), "--append"]
        )
        assert result.exit_code == 0
        assert "Appended to" in result.output

    # Verify content
    ds = xr.open_zarr(output_zarr)
    assert ds.sizes["time"] == 2
    assert "t2m" in ds.data_vars
    assert ds.attrs["Conventions"] == "CF-1.8 UGRID-1.0"


def test_cli_to_icechunk_logic(tmp_path, mock_ish_history):
    # Setup mock data
    line1 = "2020 09 01 00  256  184 10185  220   40 0 0 0"
    fn = tmp_path / "722244-00358-2020.gz"
    with gzip.open(fn, "wb") as f:
        f.write((line1 + "\n").encode())

    icechunk_url = str(tmp_path / "test_repo")

    runner = CliRunner()

    def side_effect(self, dates=None):
        self.history = mock_ish_history

    # Mock icechunk if not installed
    import importlib.util

    if importlib.util.find_spec("icechunk") is None:
        import sys
        from unittest.mock import MagicMock

        mock_icechunk = MagicMock()
        sys.modules["icechunk"] = mock_icechunk

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        with patch("xarray.Dataset.to_zarr") as mock_to_zarr:
            result = runner.invoke(
                cli, ["to-icechunk", "ish_lite", "-f", str(fn), "--icechunk-url", icechunk_url]
            )

    assert result.exit_code == 0
    if "Error: Icechunk not installed" in result.output:
        pytest.skip("Icechunk not installed, verified error message")

    assert "Saved to Icechunk repository" in result.output
    # Verify it called to_zarr (with the store)
    assert mock_to_zarr.called


def test_cli_to_icechunk_append_logic(tmp_path, mock_ish_history):
    # Setup mock data
    line1 = "2020 09 01 00  256  184 10185  220   40 0 0 0"
    fn = tmp_path / "722244-00358-2020.gz"
    with gzip.open(fn, "wb") as f:
        f.write((line1 + "\n").encode())

    icechunk_url = str(tmp_path / "test_repo")

    runner = CliRunner()

    def side_effect(self, dates=None):
        self.history = mock_ish_history

    # Mock icechunk
    import sys
    from unittest.mock import MagicMock

    mock_icechunk = MagicMock()
    sys.modules["icechunk"] = mock_icechunk

    with patch("monetio.readers.ish.ISH.read_ish_history", autospec=True, side_effect=side_effect):
        with patch("xarray.Dataset.to_zarr") as mock_to_zarr:
            result = runner.invoke(
                cli,
                [
                    "to-icechunk",
                    "ish_lite",
                    "-f",
                    str(fn),
                    "--icechunk-url",
                    icechunk_url,
                    "--append",
                ],
            )

    assert result.exit_code == 0
    assert "Appended to Icechunk repository" in result.output
    # Verify it called to_zarr with mode='a'
    args, kwargs = mock_to_zarr.call_args
    assert kwargs["mode"] == "a"
    assert kwargs["append_dim"] == "time"
