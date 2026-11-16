# W3C R2RML test cases

The R2RML test cases of the W3C RDB2RDF Working Group
(<https://www.w3.org/2001/sw/rdb2rdf/test-cases/>), as maintained by the kg-construct
community group in `kg-construct/r2rml-test-cases-support` (Apache-2.0, see LICENSE):
`manifest.ttl`, the `R2RMLTC*` mappings with their expected outputs, and the `databases/*.sql`
scripts. `test_r2rml_w3c.py` runs the scripts in DuckDB (the PostgreSQL variant of d016) and
compares maplib's output with the expected N-Quads graph by graph.
