# SEP-0006 (LATERAL) test suites

Unmodified copies of the LATERAL tests of two SPARQL engines, used by
`py_maplib/tests/test_sep0006_lateral.py`.

- `jena/Lateral` and `jena/Syntax-Lateral`: from Apache Jena,
  https://github.com/apache/jena, directories `jena-arq/testing/ARQ/Lateral` and
  `jena-arq/testing/ARQ/Syntax-Lateral`. Apache Jena, Copyright 2011-2026 The Apache
  Software Foundation. Licensed under the Apache License, Version 2.0 (`LICENSE-APACHE`).
- `oxigraph/lateral`: from Oxigraph, https://github.com/oxigraph/oxigraph, directory
  `testsuite/oxigraph-tests/sparql/lateral`. Licensed under either the Apache License,
  Version 2.0 (`LICENSE-APACHE`) or the MIT license (`oxigraph/LICENSE-MIT`).

As of 2026-11-16 these are all the published LATERAL tests with expected results: the Jena and
Oxigraph directories above are complete, SEP-0006 and Jena's documentation give example queries
without data or results, and the W3C SPARQL 1.2 tests (w3c/rdf-tests `sparql/sparql12`) have no
LATERAL tests. `py_maplib/tests/test_sep0006_lateral_differential.py` covers the rest by comparing
maplib with Oxigraph (the `pyoxigraph` package) and with equivalent SPARQL 1.1 queries.
