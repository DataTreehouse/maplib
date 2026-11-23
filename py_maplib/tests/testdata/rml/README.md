# RML test cases

Test cases of the W3C Knowledge Graph Construction community group for RML, run by
`py_maplib/tests/test_rml_community.py` with `Model.map_rml`.

## core

The RML-Core test cases: the `test-cases` folder of <https://github.com/kg-construct/rml-core>
(branch `main`, fetched 2026-11-23), by the Knowledge Graph Construction community group
(<https://www.w3.org/community/kg-construct/>). Licensed under the Creative Commons Attribution 4.0
International licence, see `core/LICENSE`.

Copied as they are: each `RMLTC*` folder with its mapping (`mapping.ttl`), input files, expected
output (`output.nq`, absent when the test case expects an error) and `README.md`, and
`metadata.csv`, which lists for every test case its base IRI, mapping, inputs, output and whether
an error is expected. The other files of the folder (the manifests, the HTML pages and the scripts
that generate them) are left out. Nothing was changed.

All 76 test cases have JSON sources and use the RML vocabulary `http://w3id.org/rml/`.

The test cases that maplib does not pass are listed, with the reason, in
`py_maplib/tests/rml_community_known_failures.txt`.

## Updating

Replace the `RMLTC*` folders and `metadata.csv` of `core` with those of a newer `test-cases`
folder, run the tests, and update the known failures.
