import numpy as np
import pytest

import monetio.sat.hdfio as hdfio

filename = "test.hdf"
nx = 2
ny = 3
nz = 5
x = np.linspace(0, 1, nx, dtype=np.float64)
y = np.linspace(0, 1, ny, dtype=np.float64)
z = np.linspace(0, 1, nz, dtype=np.float64)
xfield, yfield, zfield = np.meshgrid(x, y, z, indexing="ij")
u = np.empty((nx, ny, nz), dtype=np.float32)
v = np.empty((nx, ny, nz), dtype=np.float32)
w = np.empty((nx, ny, nz), dtype=np.float32)
u[:, :, :] = 1
v[:, :, :] = xfield[:, :, :] * yfield[:, :, :] * zfield[:, :, :]
w[:, :, :] = xfield[:, :, :] ** 2 + yfield[:, :, :] ** 2


@pytest.fixture
def sample_file(tmp_path):
    fp = str(tmp_path / filename)
    fileid = hdfio.hdf_create(fp)
    hdfio.hdf_write_coord(fileid, "x", x)
    hdfio.hdf_write_coord(fileid, "y", y)
    hdfio.hdf_write_coord(fileid, "z", z)
    hdfio.hdf_write_field(fileid, "u", ("x", "y", "z"), u)
    hdfio.hdf_write_field(fileid, "v", ("x", "y", "z"), v)
    hdfio.hdf_write_field(fileid, "w", ("x", "y", "z"), w)
    hdfio.hdf_close(fileid)
    return fp


def test_hdf_list(sample_file):
    fileid = hdfio.hdf_open(sample_file)
    datasets, indices = hdfio.hdf_list(fileid)
    assert sorted(indices) == list(range(6))
    assert set(datasets) == {"x", "y", "z", "u", "v", "w"}
    hdfio.hdf_close(fileid)


def test_hdf_read(sample_file):
    fileid = hdfio.hdf_open(sample_file)
    u = hdfio.hdf_read(fileid, "u")
    v = hdfio.hdf_read(fileid, "v")
    w = hdfio.hdf_read(fileid, "w")
    np.testing.assert_equal(u, u)
    np.testing.assert_equal(v, v)
    np.testing.assert_equal(w, w)
    hdfio.hdf_close(fileid)
