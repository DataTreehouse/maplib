Tests for CONSTRUCT templates with GRAPH blocks (quads), from Apache Jena ARQ.

Copyright The Apache Software Foundation. Licensed under the Apache License, Version 2.0, see
LICENSE-APACHE.

Syntax tests (used by test_construct_graph.py), unchanged copies from
https://github.com/apache/jena/tree/main/jena-arq/testing/ARQ/Syntax-ARQ:
`syntax-quad-construct-01.arq` to `-12.arq` are positive syntax tests in its `manifest.ttl`, and
`syntax-quad-construct-bad-01.arq` is a negative one. All of them parse; 09 has template
variables that its WHERE clause does not bind, which maplib rejects when the query runs. In
`syntax-quad-construct-07.arq`, `<urn:x-arq:DefaultGraphNode>` is Jena's internal name of the
default graph; maplib treats it as an ordinary graph IRI.

Evaluation tests (used by test_construct_graph_reference.py): the `testARQConstructQuad_*` methods
of jena-arq/src/test/java/org/apache/jena/sparql/api/TestAPI.java (commit d5b47b73), ported to
Python with the same dataset, queries and expected datasets. Jena's construct quad evaluation
tests are only in that Java file; its testing/ tree has no data-driven ones. Differences:

* maplib returns the constructed quads as a dataset, as Jena's `execConstructDataset`, so the
  duplicate quads that Jena's `execConstructQuads` returns (Duplicate_1, Duplicate_3) are one quad.
* `testARQConstructQuad_bnodes` (a blank node as the graph name, `GRAPH _:a { ... }`) is expected
  to fail: maplib only takes IRIs and variables as graph names in templates.
