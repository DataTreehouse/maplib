# Design proposal: iterating a stream on a persistent Model with per-iteration graphs

Status: proposal, not implemented.

## Problem

`ModelStream` today maps every batch into a fresh, empty model (`empty_model_like` in
`py_maplib/src/py_stream.rs`). That keeps memory bounded, but:

1. Steps cannot see anything outside the current batch: no joins against reference data or against
   what earlier batches produced (`test_stream_steps_do_not_see_other_batches`,
   `test_stream_steps_do_not_see_parent_triples_and_do_not_change_them`).
2. Nothing is kept: the parent Model is never changed, so an "increment the model" workflow has to
   write to a file and read it back.
3. Until this branch, blank-node counters were carried between the batches of one collect but not
   written back to the parent, so two streams from the same Model reused blank-node labels. This
   branch fixes that (`test_stream_blank_nodes_unique_across_collects_of_different_streams`).

Related, not stream-specific: `Model.update(q, graph=G)` evaluates the WHERE clause on `G` but
puts an INSERT without a `GRAPH` block in the default graph, while its docstring says `graph` is
"the graph to update". The stream step does the same as the eager call. The workflow below names
target graphs with `insert(target_graph=...)` or `GRAPH` blocks until this is settled.

The workflow we want: a long-lived Model holds reference data and an accumulating result. For each
iteration (batch), the batch is mapped into iteration-specific graphs, rules and updates join it
with the persistent graphs, some results are added to the persistent graphs (the increment), the
iteration's own output is written out, and the iteration-specific graphs are evicted before the next
batch.

## Proposed API

One new builder step, `ModelStream.on_model(evict=...)`, switches the stream from "fresh model per
batch" to "run every batch on this Model". Everything else stays the same builder.

```python
from maplib import Model, TRANSIENT_GRAPH

BATCH = "urn:iter:batch"      # the mapped batch
SCRATCH = "urn:iter:scratch"  # intermediate results of one iteration

m = Model()
m.read("reference.ttl")                    # persistent: default graph
m.add_template(tpl)

(
    m.map(tpl_iri, pl.scan_parquet("events/*.parquet"), graph=BATCH, stream=True)
    .on_model(evict=[BATCH, SCRATCH, TRANSIENT_GRAPH])
    # joins the batch with the persistent default graph
    # link_query uses INSERT { GRAPH <urn:iter:scratch> { ... } } WHERE { ... }
    .update(link_query, include_graphs=[BATCH])
    .infer(rules, include_graphs=[BATCH, SCRATCH])
    # the increment: kept in the default graph across iterations
    .insert(summary_construct, source_graph=SCRATCH, target_graph=None)
    # the per-iteration output
    .write("links.nt", graph=SCRATCH)
    .collect()
)
# m now holds reference data + all summaries; BATCH and SCRATCH are gone.
```

`on_model` takes:

- `evict: list[str | None]`: graphs that belong to one iteration. They are dropped at the end of
  every iteration. `None` means the default graph, so a user who keeps the increment in a named
  graph can evict the default graph instead. `TRANSIENT_GRAPH` is evicted only when listed.
- `compact_every: Optional[int] = None`: run `compact()` and dictionary garbage collection every N
  iterations (see Memory below).

The `map` call's `graph=` names where the batch lands; for map_json/map_xml it is the same
argument. Nothing else in the step signatures changes.

A plain, stream-free helper is useful on its own and is the building block:

```python
with m.scratch_graphs(BATCH, SCRATCH):   # evicted on exit, also on error
    m.map(tpl_iri, df, graph=BATCH)
    m.update(link_query, graph=SCRATCH)
    m.insert(summary_construct, source_graph=SCRATCH)
```

plus `Model.drop_graphs(graphs)` as the non-context-manager form. Users with their own Python loop
get the same semantics without the stream.

## Semantics per iteration

1. Take the Model's write lock for the iteration only. Between iterations the lock is released, so
   `m.query` from another thread, or `m.serve`, sees the Model after each finished iteration and
   never a half-mapped batch.
2. Map the batch into its graph, run the steps in order, run the write steps, exactly as now but on
   the Model instead of a batch model.
3. Evict: drop every graph in `evict`, in a `finally`, so a failing iteration still leaves no
   iteration data behind. Then release the lock.

Rules:

- **Start check.** At `collect()`, if a graph in `evict` is non-empty, raise. Silently evicting at
  start would delete data the user put there; leftovers from a crashed run are cleared with
  `m.drop_graphs(...)` explicitly.
- **Write steps are restricted.** In `on_model` mode a write step must name a graph in `evict`
  (or the transient graph); writing a persistent graph every iteration would write it N times and is
  quadratic. Checked when `write()` / `on_model()` is called, whichever comes last. A later extension
  can add `write(..., delta=True)`, which writes only the triples the steps of this iteration added,
  using the per-graph change reports that `Model.update` now returns with the triples
  (commit d69958fb).
- **No rollback of the increment.** If an iteration fails after an `insert` into a persistent graph,
  that insert stays; the iteration graphs are evicted and the error is raised. Atomic iterations
  would need copy-on-write per-iteration deltas (stage inserts into a hidden graph and merge on
  success), which can come later as `on_model(atomic=True)` once the merge is cheap.
- **Blank nodes.** Counters live on the Model, so they advance naturally and labels stay unique
  across iterations and across later streams. The copy-back added on this branch is then not needed.
- **map_default.** The template generated from the first batch is added to the Model so later
  iterations and later calls can use it.

## Implementation sketch

`py_maplib/src/py_stream.rs`:

- Add `target: StreamTarget` to `PyModelStream`, with
  `enum StreamTarget { FreshPerBatch, Model { evict: Vec<NamedGraph>, compact_every: Option<usize> } }`.
  `on_model` returns a copy with the target set, like the other builder steps.
- In `collect`, `run` dispatches on the target. For `Model`, per batch:
  `let mut inner = model.write_inner(); let r = run_batch_steps(&mut inner, ...); evict(&mut inner, evict); r`.
  `BatchState.carrier` is not needed in this mode; `default_template` is.
- Keep `py.detach` around each batch as now, so other Python threads run between iterations.

`lib/triplestore`:

- Add `Triplestore::drop_graphs(&mut self, graphs: &[NamedGraph]) -> Result<(), TriplestoreError>`.
  It removes the graphs from `graph_triples_map` (which records the change for the query cache), the
  FTS index and the vector index, and on a storage folder also deletes the graphs' segment files.
  `Triplestore::truncate` currently hits `todo!()` with a storage folder, and
  `remove_temporary_graph` leaves the files until the next reset, so neither is usable for many
  iterations on disk-backed models; this is the main new piece of Rust.
- `Model.truncate_graph` should call the same function, which fixes the `todo!()` panic it has today
  on disk-backed models.

## Memory

Evicting a graph frees its triple tables, but the global categorical dictionaries (`global_cats`)
keep every IRI and literal string ever encoded. Over many iterations with mostly new identifiers
they grow without bound. `Triplestore::garbage_collect` already removes dictionary entries no graph
refers to; `compact_every=N` runs `compact()` and then `garbage_collect()` every N iterations. The
right default needs a measurement (dictionary size versus GC cost) before choosing anything other
than `None`.

Indexes on evicted graphs disappear with them. Indexes on persistent graphs are updated
incrementally by inserts as today, so a large increment per iteration should use `compact_every`.

## Tests to add with the implementation

- Equivalence: a stream with `on_model` gives the same persistent graph and the same written output
  as a Python loop that maps, runs the steps, writes and drops the graphs per batch.
- Joins against persistent data find matches that the fresh-per-batch stream does not.
- Evicted graphs are empty after every iteration (observed from a step in the next iteration and
  from `m.graphs()` after collect), also after an error in a step, an error from the input iterator
  and an error from a write target.
- The start check raises on a non-empty evict graph and leaves it unchanged.
- A write step on a persistent graph is refused.
- Blank-node labels stay unique across iterations and across two streams.
- A query from another thread during collect only sees whole iterations.
- Disk storage: dropping graphs deletes their files, and many iterations keep the folder size bounded.
- `compact_every` keeps dictionary size bounded over many iterations with fresh IRIs (memory-capped
  test, run in a quiet window).
