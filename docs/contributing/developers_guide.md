# Developer's Guide

Welcome to the MONETIO developer's guide. This document provides information for developers who want to contribute to the core library or understand its inner workings.

## Architecture Overview

MONETIO is designed around a unified reader system that promotes backend agnosticism and consistency.

### Unified Readers

All readers are located in `monetio/readers/` and are registered in `monetio/__init__.py` for lazy loading.

- **`BaseReader`**: Abstract base class.
- **`GriddedReader`**: For gridded data (Models, Satellites). Uses `XarrayDriver`.
- **`PointReader`**: For point/tabular data (Observations). Uses `PandasDriver`.

### Drivers

Drivers handle the underlying I/O operations and provide a consistent interface for opening files, whether they are local or on remote storage (S3, HTTP).

- **`XarrayDriver`**: Wraps `xr.open_dataset` and `xr.open_mfdataset`. Supports virtualization via Kerchunk/Icechunk.
- **`PandasDriver`**: Wraps `pd.read_csv` and other pandas-based readers.

## Coding Standards

We follow strict standards to ensure the codebase remains maintainable and efficient.

### Backend Agnosticism

Code must support both Eager (NumPy/Pandas) and Lazy (Dask) backends.

- Avoid `.values`, `.compute()`, and `.load()` on the data payload; a deferred
  load must not silently materialize the observation values. (Bounded
  preparation reads — including parsing partitions to discover row lengths
  and the join-key values a wide reshape structurally needs — are allowed only
  when documented and measured. They may parse payload input and retain O(rows)
  coordinate keys; data columns remain deferred and can be reread later.)
- Do not force chunking on the default path: `chunks=`/`defer=` are the
  caller's decision at the I/O stage, and an un-asked load returns
  numpy-backed arrays.
- Use `xr.apply_ufunc` with `dask='parallelized'` for complex operations.
- Ensure all logic is vectorized.

These three standards — **No Hidden Computes**, **No Forced Chunking**, and
**Dual-Backend Testing** — are not just conventions: they are enforced
automatically for every registered source by the reader conformance suite in
`tests/test_conformance.py`. A new reader that violates one fails the suite in
CI; the rules it trips (C-06, C-07, C-05) name the source and the rule in the
failure message. Declare what your reader actually supports in the source
manifest's `ScaleCapabilities` — the suite checks that every capability you
claim is observable, and that a claim you cannot back carries a reason.

### Linting and Formatting

We use `ruff` for linting and formatting.

```bash
ruff check .
ruff format .
```

### Documentation

We use NumPy-style docstrings. Every public function and class should be documented.

## Testing

Testing is a critical part of the MONETIO development process.

### Dual-Backend Testing

Most tests should verify consistency between Eager and Lazy backends. The
conformance suite does this for every deferred-capable source at scale
(rule C-05: the deferred and eager loads must agree on values, variables, and
sizes), so a reader only needs the focused unit test below for its own
transformations.

```python
import pytest
import numpy as np
import dask.array as da
from my_module import my_function


@pytest.mark.parametrize("use_dask", [True, False])
def test_my_function(use_dask):
    # Setup data
    data = np.random.rand(10)
    if use_dask:
        data = da.from_array(data, chunks=5)

    # Run function
    result = my_function(data)

    # Verify results
    if use_dask:
        assert isinstance(result.data, da.Array)
    else:
        assert isinstance(result.data, np.ndarray)
```

### Running Tests

Run tests using `pytest`:

```bash
python -m pytest
```

To exclude network-dependent tests:

```bash
python -m pytest -m "not network"
```

## Contribution Workflow

1.  **Fork and Clone**: Fork the repository and clone it locally.
2.  **Branch**: Create a new branch for your feature or bugfix.
3.  **Implement**: Write your code and tests.
4.  **Verify**: Run `ruff` and `pytest`.
5.  **Submit**: Create a Pull Request.

## Virtualization

MONETIO supports virtualization of large datasets to avoid the overhead of opening many files.

- **Kerchunk**: Pre-compute JSON references for HDF5/NetCDF files.
- **Icechunk**: A transactional Zarr-like storage format.

Refer to the `monetio.virtualize` function and the `monetio/readers/drivers.py` for implementation details.
