# RML test cases

Test cases of the RML community, run by `py_maplib/tests/test_rml_community.py` with
`Model.map_rml`. All three sets are licensed under the Creative Commons Attribution 4.0
International licence; each folder has the `LICENSE` of its source.

The test cases that maplib does not pass are listed, with the reason, in
`py_maplib/tests/rml_community_known_failures.txt`.

## core

The RML-Core test cases: the `test-cases` folder of <https://github.com/kg-construct/rml-core>
(branch `main`, fetched 2026-11-23), by the W3C Knowledge Graph Construction community group
(<https://www.w3.org/community/kg-construct/>).

Copied as they are: each `RMLTC*` folder with its mapping (`mapping.ttl`), input files, expected
output (`output.nq`, absent when the test case expects an error) and `README.md`, and
`metadata.csv`, which lists for every test case its base IRI, mapping, inputs, output and whether
an error is expected. The other files of the folder (the manifests, the HTML pages and the scripts
that generate them) are left out. Nothing was changed.

All 76 test cases have JSON sources and use the RML vocabulary `http://w3id.org/rml/`.

## io

The source test cases of RML-IO: the `RMLSTC*` folders of the `test-cases` folder of
<https://github.com/kg-construct/rml-io> (branch `main`, fetched 2026-11-23), by the same
community group. 32 test cases about sources: encodings, compression, null values, reference
formulations, several sources, and CSV, JSON and XML documents.

The folders are copied as they are. `metadata.csv` is derived from the `metadata.csv` of the
source: the columns that the test needs (`ID`, `title`, `mapping`, `output1`, `error`), and a
`base_iri` column with `http://example.com/`.

Left out: the 41 target test cases (`RMLTTC*`). They are about logical targets, which say where
the triples of a term map are written (a file, its serialization, encoding and compression).
`Model.map_rml` maps into the model and has no logical targets.

## rmlio

The test cases of the first RML specification: the `test-cases` folder of
<https://github.com/RMLio/rml-test-cases> (branch `master`, fetched 2026-11-23), by RML.io, IDLab,
Ghent University - imec. They use the vocabulary `http://semweb.mmlab.be/ns/rml#` with `rr:` term
maps.

Copied as they are: the 117 test cases with CSV, JSON and XML sources that the `metadata.csv` of
the source lists (39 CSV, 40 JSON, 38 XML). `metadata.csv` here is derived from it: `ID`, `title`,
`mapping`, `output1` (when the folder has an `output.nq`), `error` (its "error expected?" column),
and a `base_iri` column with `http://example.com/base/`, the `@base` of the mappings.

Left out: the 206 test cases with MySQL, PostgreSQL, SQL Server and SPARQL sources, whose sources
are described as database and endpoint connections in the mapping. `Model.map_r2rml` maps
databases through a Python database object instead, and is tested with the W3C R2RML test cases.
`RMLTC0002g-JSON` is left out because the `metadata.csv` of the source does not list it.

## Updating

Replace the test case folders of a set with those of a newer `test-cases` folder, bring its
`metadata.csv` up to date, run the tests, and update the known failures.
