# Adding a New Reader

MONETIO uses a unified reader architecture based on a common base class. This ensures consistency across different data sources and simplifies the addition of new models and observation networks.

## Core Concepts

All readers are located in the `monetio/readers/` directory and inherit from `BaseReader`.

- **`BaseReader`**: The abstract base class that defines the `open_dataset` interface.
- **`GriddedReader`**: A base class for gridded data (Models, Satellites) that utilizes the `XarrayDriver`.
- **`PointReader`**: A base class for point/tabular data (Observations) that utilizes the `PandasDriver`.
- **`READER_REGISTRY`**: A global dictionary where all readers register themselves using the `@register_reader("name")` decorator.
- **`SOURCE_MANIFEST`** (in `monetio/_manifest.py`): the single authoritative list of data sources. Each source is declared exactly once here; the package's view of "what can be loaded, and from which module" is derived from it. The `@register_reader` decorator validates the name against this manifest at import time.

## Design Principles

Architecting scientific pipelines in MONETIO requires balancing flexibility, maintainability, and provenance. All new readers should adhere to these principles:

### 1. Backend Agnostic (Optional Dask)
Code must run **Eagerly (NumPy) by default** and **Lazily (Dask) optionally**.
- **Generic Inputs**: Write functions that accept generic `xr.DataArray` or `xr.Dataset` inputs. Do not assume a specific backend.
- **No Hidden Computes**: Never call `.compute()`, `.load()`, or `.values` inside a processing function. This breaks laziness for Dask users.
- **No Forced Chunking**: Do not hardcode `.chunk()` inside functions. Chunking is the user's responsibility at the I/O stage.
- **Vectorization**: Use `xarray.apply_ufunc` with `dask='parallelized'` to support both backends simultaneously.

### 2. Maintainability and Documentation
- **NumPy Docstrings**: Every function must have a docstring following the [NumPy format](https://numpydoc.readthedocs.io/en/latest/format.html) (Parameters, Returns, Examples).
- **Type Hinting**: Use `xarray.DataArray` or `xarray.Dataset` types. Avoid backend-specific types like `dask.array.Array`.

### 3. Provenance (Scientific Hygiene)
- **History Tracking**: Automatically track data lineage by updating `ds.attrs['history']` whenever data is transformed.
- **Coordinate Integrity**: Never drop coordinates (like vertical levels) unless explicitly requested.

### 4. Quality and Validation
- **Dual-Backend Testing**: Every reader or processing logic should be verified with a unit test that runs twice: once with Eager (NumPy) data and once with Lazy (Dask) data.
- **Pre-Commit**: Use `pre-commit run --all-files` to ensure code style and linting standards are met.

## Steps to Add a New Reader

Adding a source is a **two-edit** process: the reader module itself, and one
entry in the manifest. Everything else — `monetio.load()`, the source
enumeration in `available_sources()`, the CLI's source list, and the `load()`
docstring — is derived from those two edits, so it cannot drift out of sync.

### 1. Create the Reader Module

Create a new Python file in `monetio/readers/`, for example `monetio/readers/mynewmodel.py`,
with the `@register_reader` decorator on your class:

```python
from typing import List, Union
import xarray as xr
from .base import GriddedReader, register_reader


@register_reader("mynewmodel")
class MyNewModelReader(GriddedReader):
    def open_dataset(
        self,
        files: Union[str, List[str]],
        **kwargs,
    ) -> xr.Dataset:
        # 1. Open the dataset using the driver
        # The driver handles local files, S3, and common Xarray arguments
        ds = self.driver.open(files, **kwargs)

        # 2. Perform reader-specific processing
        # e.g., Rename dimensions, calculate coordinates, handle units
        ds = ds.rename({"old_dim": "x"})

        # 3. Harmonize (Apply standard naming conventions)
        ds = self.harmonize(ds)

        return ds

    def harmonize(self, ds: xr.Dataset) -> xr.Dataset:
        # Optional: Standardize variable names to MONETIO conventions
        return ds
```

### 2. Declare the Source in the Manifest

Add one entry to `SOURCE_MANIFEST` in `monetio/_manifest.py`:

```python
(_s("mynewmodel", ".readers.mynewmodel", "gridded"),)
```

The three fields are the source name (which must match the
`@register_reader("...")` name — the decorator validates against the manifest
and raises if the name is undeclared), the module path relative to the
`monetio` package, and the kind (`"gridded"` for `GriddedReader` subclasses,
`"point"` for `PointReader` subclasses; the registration checks the base class
agrees).

Optional keyword arguments on `_s(...)`:

- `aliases=("myalias", ...)` — extra loadable names that resolve to this same
  module. Every name a module registers with `@register_reader` must appear in
  the manifest, either as the canonical `name` or in `aliases` — the decorator
  validates against the manifest and raises `ValueError` for an undeclared
  name. A module that registers several distinct reader classes (like the GFS
  family, where `gfs`/`gefs`/`gdas` are three classes in one module) declares
  one manifest entry with the remaining names as aliases.
- `caps=ScaleCapabilities(...)` — only for sources that have implemented and
  tested the opt-in large-data options (deferred loading, concurrent file
  reads, parallel metadata parsing, and so on). Leave this out for a new
  reader; the honest default declares nothing.
- `legacy=(...)` — only when replacing an old facade module.

You do **not** need to edit `monetio/__init__.py`, `monetio/cli.py`, or any
docstring listing sources — those are generated from the manifest.

## Best Practices

### Eager defaults and opt-in scale behavior

Reader defaults are eager. Return NumPy-backed Xarray data or a pandas table unless
that source and output mode have passing evidence for an explicit option. Dask
support is mode-specific; a source may support deferred arrays for one output
form and reject them for another.

Before enabling a `ScaleCapabilities` flag:

- Add a deterministic, offline fixture and a passing case for the exact source,
  output form, backend, and relevant option combination.
- For deferred paths, provide validated post-preprocessing metadata where
  possible and record any schema, row-length, or coordinate preparation pass,
  its input and output bounds, later rereads, and resource ownership.
- Compare eager and evaluated deferred output for values, dtypes, ordering,
  masks, units, diagnostics, and meaningful provenance. Normalize only
  explicitly volatile provenance fields.
- Exercise the universal loader, direct reader, and shared driver boundaries
  as applicable. A skip for a missing optional dependency is not passing
  evidence.
- Record fixture and source fingerprints in `tests/data/dask_reader_audit.json`
  and run `python scripts/validate_dask_audit.py`.

See [Dask reader behavior](../dask_readers.md) for the current tested contract
and its limits.

### Provenance and Metadata

Always update the `history` attribute of the returned dataset or dataframe to record the transformation.

```python
import datetime

history = f"{datetime.datetime.now()}: Read MyNewModel data."
ds.attrs["history"] = ds.attrs.get("history", "") + "\n" + history
```

### Harmonization

Use standard coordinate names:

- Gridded: `time`, `x`, `y`, `z`, `latitude`, `longitude`.
- Point: `time`, `latitude`, `longitude`, `siteid`.

### Error Handling

When `error_missing=False` is passed (supported by the drivers), your reader should gracefully handle cases where files are not found, typically by returning an empty Dataset or DataFrame.
