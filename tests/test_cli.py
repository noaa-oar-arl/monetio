from unittest.mock import patch

import pandas as pd
import xarray as xr
from click.testing import CliRunner

import monetio
from monetio.cli import cli


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "aeronet" in result.output
    assert "airnow" in result.output
    assert "aqs" in result.output
    assert "openaq" in result.output


def test_cli_load_generic():
    runner = CliRunner()
    with patch("monetio.load") as mock_load:
        mock_load.return_value = xr.Dataset()
        result = runner.invoke(
            cli, ["load", "cmaq", "-d", "2023-01-01", "-k", "mech=cb6r3", "-k", "surf_only=True"]
        )
        assert result.exit_code == 0
        assert "Loading cmaq data" in result.output

        mock_load.assert_called_once()
        args, kwargs = mock_load.call_args
        assert args[0] == "cmaq"
        assert kwargs["dates"] == ["2023-01-01"]
        assert kwargs["mech"] == "cb6r3"
        assert kwargs["surf_only"] is True


def test_cli_load_observation():
    runner = CliRunner()
    with patch("monetio.load") as mock_load:
        mock_load.return_value = pd.DataFrame({"a": [1]})
        result = runner.invoke(cli, ["load", "ish", "-d", "2023-01-01", "--as-pandas"])
        assert result.exit_code == 0
        assert "Loading ish data" in result.output

        mock_load.assert_called_once()
        assert mock_load.call_args[0][0] == "ish"
        assert mock_load.call_args[1]["as_xarray"] is False


def test_cli_load_list_kwargs():
    runner = CliRunner()
    with patch("monetio.load") as mock_load:
        mock_load.return_value = xr.Dataset()
        result = runner.invoke(
            cli, ["load", "cmaq", "-k", "var_list=O3", "-k", "var_list=NO2", "-k", "var_list=PM25"]
        )
        assert result.exit_code == 0

        mock_load.assert_called_once()
        kwargs = mock_load.call_args[1]
        assert kwargs["var_list"] == ["O3", "NO2", "PM25"]


@patch("monetio.load")
def test_aeronet_cli(mock_load):
    # Mock return value
    mock_load.return_value = xr.Dataset()

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["aeronet", "-d", "2023-01-01", "-o", "test.nc"])
        assert result.exit_code == 0
        assert "Loading AERONET data" in result.output
        assert "Saved to test.nc (NetCDF)" in result.output

        # Verify mock_load call
        mock_load.assert_called_once()
        args, kwargs = mock_load.call_args
        assert args[0] == "aeronet"
        assert kwargs["dates"] == ["2023-01-01"]


@patch("monetio.load")
def test_aeronet_cli_csv(mock_load):
    # Mock return value
    mock_load.return_value = pd.DataFrame({"a": [1]})

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli, ["aeronet", "-d", "2023-01-01", "-o", "test.csv", "--as-pandas"]
        )
        assert result.exit_code == 0
        assert "Saved to test.csv (CSV)" in result.output


@patch("monetio.load")
def test_airnow_cli(mock_load):
    mock_load.return_value = xr.Dataset()

    runner = CliRunner()
    result = runner.invoke(cli, ["airnow", "-d", "2023-01-01:2023-01-02"])
    assert result.exit_code == 0
    assert "Loading AirNow data" in result.output

    mock_load.assert_called_once()
    kwargs = mock_load.call_args[1]
    assert len(kwargs["dates"]) > 1  # It should be a range


@patch("monetio.load")
def test_aqs_cli(mock_load):
    mock_load.return_value = xr.Dataset()

    runner = CliRunner()
    result = runner.invoke(cli, ["aqs", "-d", "2023-01-01", "-p", "OZONE", "-p", "PM2.5"])
    assert result.exit_code == 0
    assert "Loading AQS data" in result.output

    mock_load.assert_called_once()
    kwargs = mock_load.call_args[1]
    assert kwargs["param"] == ["OZONE", "PM2.5"]


@patch("monetio.load")
def test_openaq_cli(mock_load):
    mock_load.return_value = xr.Dataset()

    runner = CliRunner()
    result = runner.invoke(cli, ["openaq", "-d", "2023-01-01"])
    assert result.exit_code == 0
    assert "Loading OpenAQ data" in result.output


# --- T017: manifest-driven enumeration and validation ---------------------


def test_cli_sources_command_lists_manifest():
    """`monetio sources` prints exactly available_sources(), aliases included."""
    runner = CliRunner()
    result = runner.invoke(cli, ["sources"])
    assert result.exit_code == 0
    listed = result.output.split()
    assert listed == monetio.available_sources()
    assert "cmaq" in listed
    assert "gefs" in listed  # an alias must be listed too (FR-014)


def test_cli_load_rejects_unknown_source():
    runner = CliRunner()
    result = runner.invoke(cli, ["load", "definitely-not-a-source"])
    assert result.exit_code != 0
    assert "Unknown source" in result.output


def test_cli_load_accepts_alias_without_validate_flag():
    """An alias passes manifest validation and reaches monetio.load unchanged."""
    runner = CliRunner()
    with patch("monetio.load") as mock_load:
        mock_load.return_value = xr.Dataset()
        result = runner.invoke(cli, ["load", "gefs"])
        assert result.exit_code == 0
        mock_load.assert_called_once()
        assert mock_load.call_args[0][0] == "gefs"


def test_cli_load_no_validate_bypasses_check():
    runner = CliRunner()
    with patch("monetio.load") as mock_load:
        mock_load.return_value = xr.Dataset()
        result = runner.invoke(cli, ["load", "whatever", "--no-validate"])
        assert result.exit_code == 0
        assert mock_load.call_args[0][0] == "whatever"
