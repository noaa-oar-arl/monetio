"""Tests for XarrayDriver Icechunk backend support (Tasks 2.1–2.4)."""

import unittest.mock as mock

import pytest
import xarray as xr

from monetio.readers.drivers import (
    XarrayDriver,
    _build_s3_config,
    _open_via_icechunk,
    _select_store,
)

# ---------------------------------------------------------------------------
# Task 2.1: virtualizarr_backend / icechunk_repo parameters & validation
# ---------------------------------------------------------------------------


class TestVirtualizarrBackendValidation:
    """Validate the new virtualizarr_backend parameter on XarrayDriver.open()."""

    def test_invalid_backend_raises_valueerror(self, tmp_path):
        driver = XarrayDriver()
        f = tmp_path / "dummy.nc"
        f.touch()
        with pytest.raises(ValueError, match="Invalid virtualizarr_backend 'badvalue'"):
            driver.open(str(f), virtualizarr_backend="badvalue")

    def test_kerchunk_is_default(self):
        """Kerchunk should be the default backend (no error when not specified)."""
        import inspect

        sig = inspect.signature(XarrayDriver.open)
        assert sig.parameters["virtualizarr_backend"].default == "kerchunk"

    def test_icechunk_repo_parameter_exists(self):
        import inspect

        sig = inspect.signature(XarrayDriver.open)
        assert "icechunk_repo" in sig.parameters
        assert sig.parameters["icechunk_repo"].default is None

    def test_valid_backends_accepted(self, tmp_path):
        """Both 'kerchunk' and 'icechunk' should pass validation."""
        driver = XarrayDriver()
        f = tmp_path / "dummy.nc"
        xr.Dataset({"x": [1, 2, 3]}).to_netcdf(f)
        # kerchunk — should not raise ValueError (may raise other errors due to missing VZ deps)
        # We just verify the validation itself passes by catching only non-ValueError exceptions
        for backend in ("kerchunk", "icechunk"):
            try:
                driver.open(str(f), virtualizarr_backend=backend)
            except ValueError as e:
                if "Invalid virtualizarr_backend" in str(e):
                    pytest.fail(f"Backend '{backend}' should be accepted but was rejected")


# ---------------------------------------------------------------------------
# Task 2.2: _select_store() helper
# ---------------------------------------------------------------------------


class TestSelectStore:
    """Test the extracted _select_store() module-level function."""

    @pytest.fixture(autouse=True)
    def _mock_obstore(self):
        """Inject mock obstore/obspec_utils modules so _select_store can import them."""
        self.MockS3Store = mock.MagicMock()
        self.MockHTTPStore = mock.MagicMock()
        self.MockLocalStore = mock.MagicMock()
        self.MockRegistry = mock.MagicMock()

        mock_obstore = mock.MagicMock()
        mock_obstore_store = mock.MagicMock()
        mock_obstore_store.S3Store = self.MockS3Store
        mock_obstore_store.HTTPStore = self.MockHTTPStore
        mock_obstore_store.LocalStore = self.MockLocalStore

        mock_obspec = mock.MagicMock()
        mock_obspec_registry = mock.MagicMock()
        mock_obspec_registry.ObjectStoreRegistry = self.MockRegistry

        with mock.patch.dict(
            "sys.modules",
            {
                "obstore": mock_obstore,
                "obstore.store": mock_obstore_store,
                "obspec_utils": mock_obspec,
                "obspec_utils.registry": mock_obspec_registry,
            },
        ):
            yield

    def test_s3_store_selected_for_s3_paths(self):
        files = ["s3://my-bucket/data/file1.nc", "s3://my-bucket/data/file2.nc"]
        registry, result_files = _select_store(files, {"anon": True})

        self.MockS3Store.assert_called_once_with(
            "my-bucket", config={"skip_signature": "true", "region": "us-east-1"}
        )
        self.MockRegistry.return_value.register.assert_called_once_with(
            "s3://my-bucket", self.MockS3Store.return_value
        )
        assert result_files == files

    def test_http_store_selected_for_http_paths(self):
        files = ["https://example.com/data/file1.nc"]
        registry, result_files = _select_store(files, {})

        self.MockHTTPStore.assert_called_once()
        reg = self.MockRegistry.return_value
        assert reg.register.call_count == 2
        reg.register.assert_any_call("http://", self.MockHTTPStore.return_value)
        reg.register.assert_any_call("https://", self.MockHTTPStore.return_value)
        assert result_files == files

    def test_local_store_selected_for_local_paths(self):
        files = ["/data/file1.nc", "/data/file2.nc"]
        registry, result_files = _select_store(files, {})

        self.MockLocalStore.assert_called_once_with(prefix="/")
        self.MockRegistry.return_value.register.assert_called_once_with(
            "file:///", self.MockLocalStore.return_value
        )
        assert result_files == ["file:///data/file1.nc", "file:///data/file2.nc"]

    def test_local_files_already_prefixed_not_doubled(self):
        files = ["file:///data/file1.nc"]
        _, result_files = _select_store(files, {})
        assert result_files == ["file:///data/file1.nc"]


# ---------------------------------------------------------------------------
# Task 2.2 helper: _build_s3_config
# ---------------------------------------------------------------------------


class TestBuildS3Config:
    def test_anon_true_sets_skip_signature(self):
        config = _build_s3_config({"anon": True})
        assert config["skip_signature"] == "true"

    def test_anon_false_no_skip_signature(self):
        config = _build_s3_config({"anon": False})
        assert "skip_signature" not in config

    def test_region_extracted_from_client_kwargs(self):
        config = _build_s3_config({"anon": False, "client_kwargs": {"region_name": "us-west-2"}})
        assert config["region"] == "us-west-2"

    def test_empty_options_defaults_to_skip_signature(self):
        # anon defaults to True when not present (via .get("anon", True))
        config = _build_s3_config({})
        assert config["skip_signature"] == "true"


# ---------------------------------------------------------------------------
# Task 2.3: _open_via_icechunk()
# ---------------------------------------------------------------------------


class TestOpenViaIcechunk:
    def test_import_error_when_icechunk_missing(self):
        """Should raise ImportError with install instructions when icechunk is not installed."""
        vds = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"icechunk": None}):
            with pytest.raises(ImportError, match="pip install monetio\\[icechunk\\]"):
                _open_via_icechunk(vds, "/tmp/repo", None)

    def test_icechunk_workflow(self):
        """Verify the full icechunk workflow: open_or_create → write → commit → read."""
        vds = mock.MagicMock()
        mock_icechunk = mock.MagicMock()
        mock_repo = mock_icechunk.Repository.open_or_create.return_value
        mock_session = mock_repo.writable_session.return_value
        mock_store = mock_session.store
        mock_readonly_session = mock_repo.readonly_session.return_value

        expected_ds = xr.Dataset({"temp": [1, 2, 3]})

        with (
            mock.patch.dict("sys.modules", {"icechunk": mock_icechunk}),
            mock.patch("xarray.open_zarr", return_value=expected_ds) as mock_open_zarr,
        ):
            result = _open_via_icechunk(vds, "/tmp/repo", None)

        # Verify workflow
        mock_icechunk.Repository.open_or_create.assert_called_once_with("/tmp/repo")
        mock_repo.writable_session.assert_called_once_with("main")
        vds.virtualize.to_icechunk.assert_called_once_with(mock_store)
        mock_session.commit.assert_called_once_with("VirtualiZarr references")
        mock_repo.readonly_session.assert_called_once()
        mock_open_zarr.assert_called_once_with(mock_readonly_session.store, consolidated=False)
        assert result is expected_ds


# ---------------------------------------------------------------------------
# Task 2.4: Wiring icechunk backend into VirtualiZarr code path
# ---------------------------------------------------------------------------


class TestIcechunkWiring:
    def test_icechunk_backend_calls_open_via_icechunk(self, tmp_path):
        """When virtualizarr_backend='icechunk', the icechunk path should be taken."""
        driver = XarrayDriver()
        f = tmp_path / "dummy.nc"
        xr.Dataset({"x": [1, 2, 3]}).to_netcdf(f)

        expected_ds = xr.Dataset({"x": [1, 2, 3]})
        mock_vds = mock.MagicMock()

        # Create mock modules for the local imports inside open()
        mock_virtualizarr = mock.MagicMock()
        mock_virtualizarr.open_virtual_mfdataset.return_value = mock_vds
        mock_parsers = mock.MagicMock()

        with (
            mock.patch.dict(
                "sys.modules",
                {
                    "ujson": mock.MagicMock(),
                    "zarr": mock.MagicMock(),
                    "virtualizarr": mock_virtualizarr,
                    "virtualizarr.parsers": mock_parsers,
                },
            ),
            mock.patch("monetio.readers.drivers._select_store") as mock_select,
            mock.patch(
                "monetio.readers.drivers._open_via_icechunk", return_value=expected_ds
            ) as mock_ice,
        ):
            mock_select.return_value = (mock.MagicMock(), [str(f)])

            result = driver.open(
                str(f),
                use_virtualizarr=True,
                virtualizarr_backend="icechunk",
                icechunk_repo="/tmp/test-repo",
            )

        mock_ice.assert_called_once_with(mock_vds, "/tmp/test-repo", None)
        assert result is expected_ds

    def test_kerchunk_backend_does_not_call_icechunk(self, tmp_path):
        """When virtualizarr_backend='kerchunk', the icechunk path should NOT be taken."""
        driver = XarrayDriver()
        f = tmp_path / "dummy.nc"
        xr.Dataset({"x": [1, 2, 3]}).to_netcdf(f)

        mock_vds = mock.MagicMock()
        mock_vds.vz.to_kerchunk.return_value = {"version": 1, "refs": {}}

        mock_virtualizarr = mock.MagicMock()
        mock_virtualizarr.open_virtual_mfdataset.return_value = mock_vds
        mock_parsers = mock.MagicMock()

        with (
            mock.patch.dict(
                "sys.modules",
                {
                    "ujson": mock.MagicMock(),
                    "zarr": mock.MagicMock(),
                    "virtualizarr": mock_virtualizarr,
                    "virtualizarr.parsers": mock_parsers,
                },
            ),
            mock.patch("monetio.readers.drivers._select_store") as mock_select,
            mock.patch("monetio.readers.drivers._open_via_icechunk") as mock_ice,
            mock.patch("fsspec.get_mapper"),
            mock.patch("xarray.open_dataset") as mock_open_ds,
        ):
            mock_select.return_value = (mock.MagicMock(), [str(f)])
            mock_open_ds.return_value = xr.Dataset({"x": [1, 2, 3]})

            driver.open(
                str(f),
                use_virtualizarr=True,
                virtualizarr_backend="kerchunk",
            )

        mock_ice.assert_not_called()
