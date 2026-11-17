# LDBC Graphalytics validation graphs for weakly connected components

From [ldbc/ldbc_graphalytics](https://github.com/ldbc/ldbc_graphalytics) at commit
7b8bde76cf7aab5e90b25ecd4b38829e2f98b292,
`graphalytics-validation/src/main/resources/validation-graphs/`, under the Apache License 2.0
(`LICENSE`), unchanged:

* `wcc/dir-input`, `wcc/undir-input`: adjacency lists (a vertex, then its neighbours), and
  `wcc/dir-output`, `wcc/undir-output`: the component label of every vertex;
* `example/example-{directed,undirected}.v` and `.e`: the vertices, and the edges with weights, of the
  example graphs of the Graphalytics specification, and `-WCC`: their component labels.

The specification (arXiv:2011.15028, section 2.3.3 and 2.4) defines WCC as the components of the
graph with its edges taken both ways, and validates an output by equivalence: the labels may differ,
the partition may not. `py_maplib/tests/test_ldbc_graphalytics_wcc.py` checks `maplib:component`
against them.
