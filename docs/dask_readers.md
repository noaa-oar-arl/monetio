# Dask behavior in readers

MONETIO reader defaults are eager. Deferred loading, chunk controls, concurrent
file opening, and distributed execution are opt-in and supported only for the
source, output form, backend, and option combination with matching passing
evidence in [`tests/data/dask_reader_audit.json`](../tests/data/dask_reader_audit.json).
Use `monetio.source_capabilities(name)` for the public source declaration and
consult the audit ledger for the evidence case IDs and fixture fingerprints.

## Deferred point conversion

A native deferred table can be constructed without evaluating its partitions
when the reader supplies an explicit post-preprocessing schema. Converting a
Dask table to the point-reader Xarray layout requires concrete partition
lengths and coordinate keys. MONETIO gathers each partition's length and its
`time` and `siteid` keys in a shared preparation task; the wide UGRID layout also
requires `variable` keys. For text readers, this invokes the parser once per
partition during preparation and retains O(rows) coordinate-key values. Payload columns remain Dask-backed;
evaluation can reread the input. The conversion does not persist the full input.
This cost is recorded as `point-xarray-partition-key-preparation` in the audit
ledger.

## Controls and limits

Pass `defer=True` for supported deferred point outputs. For gridded readers, pass
`use_dask=True` or an explicit `chunks` mapping only where the mode evidence
supports it. `chunks={}` requests backend-preferred chunking. MONETIO does not
create a distributed cluster or change the global Dask scheduler on the
caller's behalf. A caller-provided scheduler or client owns execution policy.

Whole-file parsers cannot generally satisfy selective reads within a file;
where such a reader has evidence, it may still defer conversion after parsing
the whole file. Opening several independent files concurrently is distinct
from deferring payload reads and does not by itself establish either lazy or
worker support.

A returned lazy object may depend on open backend resources. Close Xarray
objects when finished. Evaluation after explicit close is not guaranteed.
Worker execution, cleanup, and chunk controls remain mode-specific; a missing
case or an optional-dependency skip is not evidence of support. The strict audit
validator reports inventory modes that still lack required evidence.

For a caller selected Dask scheduler, keep control at evaluation time:

```python
import monetio

ds = monetio.load("airnow", files=paths, defer=True, wide_fmt=False)
subset = ds["obs"].isel(node=slice(0, 10_000))
values = subset.compute(scheduler="threads")
ds.close()
```

This example illustrates API placement; it does not establish distributed-worker
support for every source. Use only source/output modes with passing evidence in
the ledger, and pass a caller-owned `distributed.Client` where that mode has
been verified.
