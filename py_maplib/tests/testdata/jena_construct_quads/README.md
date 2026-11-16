Syntax tests for CONSTRUCT templates with GRAPH blocks (quads), from Apache Jena ARQ:
https://github.com/apache/jena/tree/main/jena-arq/testing/ARQ/Syntax-ARQ
(`syntax-quad-construct-*.arq`, listed in its `manifest.ttl` as positive syntax tests, and
`syntax-quad-construct-bad-01.arq` as a negative one).

Licensed under the Apache License, Version 2.0, see http://www.apache.org/licenses/LICENSE-2.0.
Copyright The Apache Software Foundation.

The files are unchanged. Tests 09 and 12 use Jena's bare `{ ... }` blocks inside the template,
which maplib does not support, and are expected to fail (see test_construct_graph.py).
