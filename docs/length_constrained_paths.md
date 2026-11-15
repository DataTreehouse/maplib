# Length-constrained paths

SPARQL 1.1 property paths have no length bounds: `ex:p+` and `ex:p*` match paths of any length, and
they only say *whether* two nodes are connected, not how far apart they are. maplib has two
extensions for this:

- the **`{n,m}` path syntax**, for bounded repetition inside any property path expression, and
- the **`maplib:path` magic predicate**, which also returns the length, the shortest paths or the
  nodes of each path, and can exclude paths that revisit nodes.

## The `{n,m}` syntax

As in the SPARQL 1.1 working draft of January 2010 and in Apache Jena ARQ:

| Form       | Matches                        |
|------------|--------------------------------|
| `elt{n}`   | exactly n repetitions of `elt` |
| `elt{n,m}` | n to m repetitions             |
| `elt{n,}`  | n or more repetitions          |
| `elt{,m}`  | 0 to m repetitions             |

`elt` is any path element, so `(ex:p|^ex:q){2,4}` and `ex:a{2}/ex:b+` work.

```sparql
PREFIX ex: <http://example.org/>
SELECT ?manager WHERE { ex:alice ex:reportsTo{1,3} ?manager }
```

Like the other property paths in maplib, the result is a set: each pair of nodes is returned once,
however many paths connect them. The parser rewrites the bounds into SPARQL 1.1 operators:
`p{n,m}` becomes `p/…/p` (n times) followed by `p?/…/p?` (m − n times), and `p{n,}` becomes
`p/…/p` (n − 1 times) followed by `p+`. A query printed after parsing shows this form.
`{0}`, `{,0}` and `{n,m}` with n > m are syntax errors, and bounds are at most 1000.

## The `maplib:path` magic predicate

```sparql
PREFIX ex: <http://example.org/>
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
SELECT ?s ?o ?len WHERE {
  ?s maplib:path [
    maplib:predicate ex:p, ex:q ;     # a step follows ex:p or ex:q from subject to object
    maplib:inversePredicate ex:r ;    # ... or ex:r from object to subject
    maplib:minLength 2 ;
    maplib:maxLength 4 ;
    maplib:object ?o ;
    maplib:length ?len
  ]
}
```

The subject of `maplib:path` is the start of the path, and the blank node (or a variable) holds the
options:

| Option                    | Value                                  | Default    |
|---------------------------|----------------------------------------|------------|
| `maplib:predicate`        | IRI, can be repeated                   |            |
| `maplib:inversePredicate` | IRI, can be repeated                   |            |
| `maplib:minLength`        | non-negative integer                   | 1          |
| `maplib:maxLength`        | non-negative integer                   | unbounded  |
| `maplib:object`           | the end of the path: variable or term  | not bound  |
| `maplib:length`           | variable: the length (number of steps) |            |
| `maplib:pathMode`         | `"walk"`, `"acyclic"` or `"simple"`    | `"walk"`   |
| `maplib:shortest`         | `true` or `false`                      | `false`    |
| `maplib:pathIndex`        | variable: the number of the path       |            |
| `maplib:nodeIndex`        | variable: the position of a node       |            |
| `maplib:pathNode`         | variable: a node of the path           |            |

Each step follows one of the predicates, or one of the inverse predicates backwards. Without
`maplib:predicate` and `maplib:inversePredicate`, a step follows any predicate. The start and the
end can each be a variable, a constant, or bound by the rest of the query (other patterns of the
same basic graph pattern, `VALUES`, an enclosing join): the search starts from the bound end with
the fewest values, and searches backwards from the object when that is the one. The pattern works
inside `GRAPH` and with the `graph` argument of `Model.query`, and can be used with other patterns,
`FILTER`, `OPTIONAL` and `UNION` like any triple pattern. Unknown `maplib:` options are errors.

### What is returned

- Without `maplib:length` and path outputs: each pair (start, end) once, if some path between
  them has a length between the bounds.
- With `maplib:length`: each (start, end, length) once, for every length between the bounds at
  which a path exists.
- With `maplib:shortest true`: only the shortest length (at least `minLength`) for each pair.
- With `maplib:pathIndex`: one row per path. With `maplib:nodeIndex` or `maplib:pathNode`: one
  row per node of each path, with the node's position (0 for the start) and the path number, so
  that `ORDER BY ?pathIndex ?nodeIndex` lists the paths in order. A path is a sequence of nodes;
  parallel edges with different predicates between the same nodes give the same path.
  `maplib:shortest true` then returns all the shortest paths for each pair.

### Path modes

| Mode        | Allowed paths                                                                    |
|-------------|----------------------------------------------------------------------------------|
| `"walk"`    | nodes and edges may repeat (as in SPARQL property paths and GQL `WALK`)          |
| `"acyclic"` | no node occurs twice (GQL `ACYCLIC`)                                             |
| `"simple"`  | no node occurs twice, except that the path may end where it started (GQL `SIMPLE`) |

On a cycle there are walks of every length, so `"walk"` with `maplib:length` or path outputs
needs `maplib:maxLength` (or `maplib:shortest true`). Pairs and shortest lengths of walks need no
maximum. `minLength 0` matches the zero-length path from each start to itself; with an unbound
start that is every node that occurs in the steps (as for `ex:p*` in maplib). Unlike `ex:p{0,n}`,
`maplib:path` does not match a start that has no step triples at all, even when it is bound; use
`ex:p{0,n}` or `OPTIONAL` for that.

### Evaluation

The nodes of the steps' triples are numbered, and the steps become adjacency lists (compressed
sparse rows) over these numbers, as for maplib's other property paths. The search is a
breadth-first search from each start, one level per length, run in parallel over the starts:

- Walks without path outputs keep a set of nodes per level, so each (start, node, length) is
  visited once, and the cost is at most the number of nodes times the number of levels per start.
  For pairs and shortest lengths, a node is followed from length `minLength` on only the first
  time it is reached (a walk's continuations depend only on its node and on its length capped at
  `minLength`), so the search ends without `maxLength`, also on cycles.
- Acyclic and simple paths, and path outputs, enumerate the paths level by level as a tree of
  partial paths. Shortest walks keep only the partial paths that reach a node at its shortest
  length. The number of paths can grow exponentially with the length; when `max_rows` is set for
  the query, the search fails once it holds more partial paths or results than that.

## How other engines do it

- The SPARQL 1.1 [working draft of January 2010](https://www.w3.org/TR/2010/WD-sparql11-property-paths-20100126/)
  had `elt{n}`, `elt{n,m}`, `elt{n,}` and `elt{,n}`, with counting (bag) semantics for fixed
  lengths. They were [removed](https://www.w3.org/2009/sparql/wiki/CommentResponse_PropertyPathComments.html)
  from SPARQL 1.1, which also changed `*` and `+` to set semantics; the working group expected
  implementations to experiment with them. The SPARQL 1.2 work has not added them.
- [Apache Jena ARQ](https://jena.apache.org/documentation/query/property_paths.html) supports the
  four forms in its extended syntax (`Syntax.syntaxARQ`). `{n}` is compiled to triple patterns;
  the other forms are evaluated as paths. Complex paths can be named with `PathLib.install`.
- [Stardog path queries](https://docs.stardog.com/query-stardog/path-queries) are a separate query
  form, `PATHS [SHORTEST|ALL] [CYCLIC] START ?s … END ?e … VIA … [MAX LENGTH n]`, returning one
  row per edge with paths separated by rows. Shortest paths are the default; there is no minimum
  length.
- [GraphDB graph path search](https://graphdb.ontotext.com/documentation/10.8/graph-path-search.html)
  uses a `SERVICE path:search` block with `path:findPath path:shortestPath | path:allPaths |
  path:distance | path:cycle`, `path:sourceNode`, `path:destinationNode`, `path:minPathLength`,
  `path:maxPathLength` (default 8), and returns one row per edge with `path:pathIndex` and
  `path:resultBindingIndex`; `path:bidirectional` ignores edge direction.
- Blazegraph evaluates arbitrary-length paths with an
  [ArbitraryLengthPathOp](https://blazegraph.com/database/apidocs/com/bigdata/bop/paths/ArbitraryLengthPathOp.html)
  whose lower and upper bounds are the rounds of a fixed-point iteration, deduplicated with a
  hash map; its ALP service exposes them as minimum and maximum lengths.
- [Virtuoso](https://docs.openlinksw.com/virtuoso/rdfsparqlimplementatiotrans) puts a subquery in
  `OPTION (TRANSITIVE, t_in(?x), t_out(?y), t_min(2), t_max(4), t_step('step_no') AS ?dist,
  t_distinct, t_shortest_only, t_no_cycles)`, giving lengths and step-by-step paths.
- Oxigraph implements the SPARQL 1.1 paths only. QLever's transitive path operator has minimum and
  maximum distances internally (used for `+`, `*` and `?`), without `{n,m}` syntax.
- Cypher and GQL have [variable-length patterns](https://neo4j.com/docs/cypher-manual/current/patterns/reference/)
  `-[:R*2..4]->` and quantifiers `{2,4}`, with path modes `WALK`, `TRAIL` (no repeated edges,
  Cypher's default), `ACYCLIC` and `SIMPLE`, and `SHORTEST`/`ALL SHORTEST` selectors.

maplib follows Jena for the syntax (with set semantics, like its other paths), and GraphDB's
magic-predicate style and GQL's path modes for `maplib:path`.
