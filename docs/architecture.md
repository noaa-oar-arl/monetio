# Documentation Architecture

MONETIO is transitioning to a more robust and maintainable documentation and code architecture.

## Unified Reader System

Previously, MONETIO was organized into submodules based on data types: `obs`, `models`, `sat`, and `profile`. These modules contained various readers with inconsistent interfaces.

We are moving to a unified reader system located in `monetio/readers/`.

### Key Benefits

- **Consistency**: All readers follow a common interface (`BaseReader`).
- **Discovery**: The `monetio.load()` function provides a single entry point for all data sources.
- **Maintainability**: Common logic (like S3 access, coordinate renaming, and lazy loading) is centralized in drivers and base classes.
- **Lazy registration**: Reader modules are imported only when a source is first loaded, so `import monetio` stays cheap (~25 ms) regardless of how many sources are installed. Enumerate what is available with `monetio.available_sources()` (every loadable name, canonical plus aliases); inspect one with `monetio.source_info(name)` (module, kind, capabilities) or `monetio.source_capabilities(name)`.
- **Virtualization**: MONETIO supports virtualization of large gridded datasets via Kerchunk and Icechunk.
    - **API**: Use `monetio.virtualize(source, files, output, backend)` to pre-compute and store metadata references. This provides a Pythonic way to generate virtualization maps for any supported gridded source.
    - **Kerchunk**: For large gridded datasets (MERRA2, GFS, ICAP), readers leverage `use_virtualizarr=True` via the Xarray driver to bypass `open_mfdataset` overhead. By pre-computing references to `virtualizarr_file`, datasets map into memory virtually via the Zarr engine.
    - **Icechunk**: A transactional Zarr-like storage format is supported via `use_icechunk=True` and `icechunk_url`.
    - **CLI**: The `monetio virtualizarr` command provides a command-line interface for generating these references.

## Loading and the scale envelope

How much data a load materializes is the caller's decision, not a hidden
default. There are two independent axes:

- **Module import** is always lazy — the reader code for a source is imported
  the first time that source is loaded and cached thereafter. This is what
  keeps `import monetio` fast; it says nothing about data.
- **Data materialization** is **eager by default**: `monetio.load(source,
  files=...)` returns a fully in-memory `xarray.Dataset` (numpy-backed) or
  `pandas.DataFrame`. Dask is the sole lazy-array backend; there is no
  alternative array backend.

To work with datasets that do not fit in memory, opt in explicitly:

- `defer=True` builds the load as a lazy graph and returns it unevaluated, so
  downstream slicing/selection reads only the bytes needed. For gridded
  sources this yields a dask-backed `Dataset`; for wide point products it
  defers the long→wide reshape.
- `chunks={...}` (gridded sources) controls dask chunking at the I/O stage.
- `concurrent_files=True` (multi-file sources whose per-file read is
  independent) overlaps file reads; `parallel_metadata` speeds the per-file
  metadata pass.

Not every source supports every option, and the package does not pretend
otherwise. `monetio.source_capabilities(name)` reports, per source, which of
these are actually validated (`deferred`, `deferred_wide`, `distributed`,
`concurrent_files`, `parallel_metadata`, `chunk_controls`); a source that has
not been checked carries a `reason` explaining the gap. As of this writing the
deferred-capable set is `{aeronet, airnow, cmaq, tolnet}`, of which
`{cmaq, airnow}` are additionally validated end-to-end on a `distributed`
cluster. A source whose capabilities are not validated loads eagerly. Unsupported
scale options are rejected with a source-specific reason rather than silently
ignored. Deferred point-to-Xarray conversion may parse partitions to discover
row lengths and coordinate keys; the payload columns remain deferred and can
be reread during evaluation. Per-source worker and cleanup support is tracked
by mode. See [Dask reader behavior](dask_readers.md) for current evidence and
limits.

## Interaction Modes

MONETIO is designed to support different user interaction patterns:

- **Static (Production Pipeline)**: Optimized for batch processing, utilizing virtualization and standardized diagnostic paths.
- **Interactive (Exploration)**: Flexible for Jupyter notebooks, providing easy-to-use plotting and interactive data discovery.

## Deprecation of Legacy Modules

The following legacy submodules are currently being deprecated in favor of the new `monetio/readers/` architecture:

- `monetio.obs`
- `monetio.models`
- `monetio.sat`
- `monetio.profile`

While these modules still exist as wrappers to maintain backward compatibility, new features and readers should be implemented directly in `monetio/readers/`. Users are encouraged to transition to the `monetio.load()` function.

### Example: Migration

**Legacy way:**

```python
from monetio.obs import airnow

df = airnow.add_data(dates)
```

**New way:**

```python
import monetio as mio

df = mio.load("airnow", files=dates)
```

## MONETIO standards

MONETIO documentation and code follow the **monetio standards** for scientific pipeline code:

1.  **Flexibility**: Eager by default, with explicit opt-in laziness via Dask (`defer=True`, `chunks=`) for datasets that exceed memory. Dask is the sole lazy-array backend.
2.  **Maintainability**: Comprehensive Type hints and NumPy-style docstrings.
3.  **Provenance**: Automatically updating `ds.attrs['history']` to track data transformations.
4.  **Visualization**: Integrated support for both static (Matplotlib/Cartopy) and interactive (HvPlot) visualization.
