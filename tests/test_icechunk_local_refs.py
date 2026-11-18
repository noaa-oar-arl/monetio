"""Regression tests: icechunk virtual references for LOCAL files.

``_open_via_icechunk`` must register local-filesystem virtual chunk
containers for ``file://`` (and plain absolute) manifest paths; otherwise
virtualizarr refuses to write the store with "No Virtual Chunk Containers
set". Remote (s3/http) prefixes keep their existing container handling.
"""

import numpy as np
import pytest
import xarray as xr

icechunk = pytest.importorskip("icechunk")
virtualizarr = pytest.importorskip("virtualizarr")

from monetio.readers.drivers import _open_via_icechunk  # noqa: E402


def _virtual_dataset_with_paths(tmp_path, paths):
    """Build a virtual dataset whose manifest references the given paths."""
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    from zarr.core.dtype import get_data_type_from_native_dtype
    from zarr.core.metadata.v3 import ArrayV3Metadata

    chunks = {}
    for i, path in enumerate(paths):
        chunks[f"{i}.0"] = {"path": path, "offset": 0, "length": 16}
    manifest = ChunkManifest(chunks)
    metadata = ArrayV3Metadata(
        shape=(len(paths), 4),
        data_type=get_data_type_from_native_dtype(np.dtype("float32")),
        chunk_grid={"name": "regular", "configuration": {"chunk_shape": (1, 4)}},
        chunk_key_encoding={"name": "default", "configuration": {"separator": "."}},
        fill_value=0.0,
        codecs=[{"name": "bytes", "configuration": {"endian": "little"}}],
        attributes={},
        dimension_names=None,
    )
    marr = ManifestArray(chunkmanifest=manifest, metadata=metadata)
    return xr.Dataset({"var": (("time", "x"), marr)})


def test_local_file_urls_get_containers(tmp_path):
    data_file = tmp_path / "sample.grib2"
    data_file.write_bytes(b"\x00" * 64)
    vds = _virtual_dataset_with_paths(tmp_path, [data_file.as_uri()])

    store_dir = tmp_path / "repo"
    ds = _open_via_icechunk(vds, str(store_dir), None)

    assert "var" in ds.variables
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_dir)))
    containers = repo.config.virtual_chunk_containers or {}
    assert any(prefix.startswith("file://") for prefix in containers), containers


def test_plain_absolute_paths_get_containers(tmp_path):
    data_file = tmp_path / "sample.grib2"
    data_file.write_bytes(b"\x00" * 64)
    vds = _virtual_dataset_with_paths(tmp_path, [str(data_file)])

    store_dir = tmp_path / "repo"
    ds = _open_via_icechunk(vds, str(store_dir), None)

    assert "var" in ds.variables
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_dir)))
    containers = repo.config.virtual_chunk_containers or {}
    assert any(prefix.startswith("file://") for prefix in containers), containers


def test_local_refs_roundtrip_reads_data(tmp_path):
    payload = np.arange(4, dtype=np.float32).tobytes()
    data_file = tmp_path / "sample.bin"
    data_file.write_bytes(payload)
    vds = _virtual_dataset_with_paths(tmp_path, [data_file.as_uri()])

    store_dir = tmp_path / "repo"
    ds = _open_via_icechunk(vds, str(store_dir), None)

    values = ds["var"].isel(time=0).values
    np.testing.assert_array_equal(values, np.arange(4, dtype=np.float32))
