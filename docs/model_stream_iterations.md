# Design proposal: iterating a stream on a persistent Model with per-iteration graphs

Status: implemented, except `write(..., delta=True)` and `atomic=True`.

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
iteration's own output is written out, and the iteration-specific graphs are dropped before the next
batch.

## Proposed API

The streaming API has two places for things, and the proposal adds one item to each:

- Options about how the input is mapped go on the `map*` call, next to `stream=True`, as `graph=`
  and `documents_per_batch=` already do. **`in_place=True`** runs every batch on the Model itself
  instead of a fresh batch model. The default, `in_place=False`, is today's behaviour.
- Steps are named after the Model method they run (`update`, `insert`, `infer`, `write`).
  **`.drop_graphs(graphs)`** is a step that runs the new `Model.drop_graphs(graphs)` on each
  batch, wherever it is placed in the pipeline.

```python
from maplib import Model, TRANSIENT_GRAPH

BATCH = "urn:iter:batch"      # the mapped batch
SCRATCH = "urn:iter:scratch"  # intermediate results of one iteration

m = Model()
m.read("reference.ttl")                    # persistent: default graph
m.add_template(tpl)

(
    m.map(tpl_iri, pl.scan_parquet("events/*.parquet"), graph=BATCH, stream=True, in_place=True)
    # joins the batch with the persistent default graph
    # link_query uses INSERT { GRAPH <urn:iter:scratch> { ... } } WHERE { ... }
    .update(link_query, include_graphs=[BATCH])
    .infer(rules, include_graphs=[BATCH, SCRATCH])
    # the increment: kept in the default graph across iterations
    .insert(summary_construct, source_graph=SCRATCH, target_graph=None)
    # the per-iteration output
    .write("links.nt", graph=SCRATCH)
    # the per-iteration graphs
    .drop_graphs([BATCH, SCRATCH, TRANSIENT_GRAPH])
    .collect()
)
# m now holds reference data + all summaries; BATCH and SCRATCH are gone.
```

On the `map`, `map_triples`, `map_default`, `map_json` and `map_xml` calls, with `stream=True`:

- `in_place: bool = False`: run every batch on this Model. Ignored without `stream=True`.
- `compact_every: Optional[int] = None`: with `in_place`, run `compact()` and dictionary garbage
  collection every N batches (see Memory below). An option of the whole run, so it is not a step.

The step:

- `ModelStream.drop_graphs(graphs: List[Optional[str]])`: drop these graphs from the batch model.
  `None` means the default graph, so a user who keeps the increment in a named graph can drop the
  default graph instead. `TRANSIENT_GRAPH` is dropped only when listed. Without `in_place` it is
  still useful: it frees a large intermediate graph before later steps of the same batch.

`Model.drop_graphs(graphs)` is the same operation outside a stream, and a context manager gives
users with their own Python loop the same per-iteration cleanup:

```python
with m.scratch_graphs(BATCH, SCRATCH):   # dropped on exit, also on error
    m.map(tpl_iri, df, graph=BATCH)
    m.update(link_query, include_graphs=[BATCH])   # INSERT { GRAPH <urn:iter:scratch> { ... } }
    m.insert(summary_construct, source_graph=SCRATCH)
```

Alternatives considered: `stream="model"` turns a bool into a bool-or-string, which is harder to
type and document. `collect(in_place=True, evict=[...])` is close to Polars, but it puts the
graphs to drop far from the steps that fill them, and only refuses write steps on persistent
graphs at the very end. An `on_model(evict=...)` builder method fits neither place: it is not an
option of the mapping and runs nothing per batch.

## Semantics per batch

1. With `in_place`, take the Model's write lock for the batch only. Between batches the lock is
   released, so `m.query` from another thread, or `m.serve`, sees the Model after each finished
   batch and never a half-mapped one.
2. Map the batch into its graph and run the steps in order, exactly as now but on the Model
   instead of a batch model.
3. If a step fails, run the batch's `drop_graphs` steps that have not run yet before raising, so a
   failing batch leaves no per-batch data behind. Then release the lock.

Rules, checked when the stream is collected (before anything runs), from the list of steps:

- **Start check.** With `in_place`, if a graph named in a `drop_graphs` step is non-empty, raise.
  Silently dropping it at start would delete data the user put there; leftovers from a crashed
  run are cleared with `m.drop_graphs(...)` explicitly.
- **Write steps are restricted.** With `in_place`, a write step must name a graph that a later
  `drop_graphs` step drops (or the transient graph); writing a persistent graph every batch would
  write it N times and is quadratic. A later extension can add `write(..., delta=True)`, which
  writes only the triples the steps of this batch added, using the per-graph change reports that
  `Model.update` now returns with the triples (commit d69958fb).
- **No rollback of the increment.** If a batch fails after an `insert` into a persistent graph,
  that insert stays; the per-batch graphs are dropped and the error is raised. Atomic batches
  would need copy-on-write deltas (stage inserts into a hidden graph and merge on success), which
  can come later as an `atomic=True` option on the map call once the merge is cheap.
- **Blank nodes.** With `in_place`, counters live on the Model, so they advance naturally and
  labels stay unique across batches and later streams; the copy-back in `collect()` is a no-op.
- **map_default.** With `in_place`, the template generated from the first batch is added to the
  Model so later batches and later calls can use it.

## Implementation sketch

`py_maplib/src/py_stream.rs`:

- Add `in_place: bool` and `compact_every: Option<usize>` to `PyModelStream` (set from the `map*`
  call in `py_model.rs`), and `StreamStep::DropGraphs { graphs: Vec<NamedGraph> }`.
- In `collect`, validate the rules above from `steps`, then per batch: with `in_place`,
  `let mut inner = model.write_inner();` and run the steps on it; otherwise use the fresh batch
  model as now. On an error, run the remaining `DropGraphs` steps before returning it.
  `BatchState.carrier` is only needed without `in_place`; `default_template` is needed in both.
- Keep `py.detach` around each batch as now, so other Python threads run between batches.


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

Indexes on dropped graphs disappear with them. Indexes on persistent graphs are updated
incrementally by inserts as today, so a large increment per iteration should use `compact_every`.

## Tests to add with the implementation

- Equivalence: a stream with `in_place=True` gives the same persistent graph and the same written output
  as a Python loop that maps, runs the steps, writes and drops the graphs per batch.
- Joins against persistent data find matches that the fresh-per-batch stream does not.
- Graphs named in `drop_graphs` are empty after every iteration (observed from a step in the next iteration and
  from `m.graphs()` after collect), also after an error in a step, an error from the input iterator
  and an error from a write target.
- The start check raises on a non-empty graph named in a `drop_graphs` step and leaves it unchanged.
- A write step on a persistent graph is refused with `in_place`, and allowed without it.
- `drop_graphs` without `in_place` frees the graph for the later steps of the same batch, and `drop_graphs` steps still run when an earlier step fails.
- Blank-node labels stay unique across iterations and across two streams.
- A query from another thread during collect only sees whole iterations.
- Disk storage: dropping graphs deletes their files, and many iterations keep the folder size bounded.
- `compact_every` keeps dictionary size bounded over many iterations with fresh IRIs (memory-capped
  test, run in a quiet window).
