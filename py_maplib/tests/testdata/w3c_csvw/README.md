This README is for the W3C CSV Working Group's test suite.
This test suite contains xxx kinds of tests:

* CSV to JSON Test (`csvt:JsonToRdfTest`) - the result of processing
  a CSV to JSON.

* CSV to RDF Test (`csvt:CsvToRdfTest`) - the result of processing
  a CSV to RDF (Turtle) and testing results using RDF isomorphism.

* Metadata to JSON Test (`csvt:JsonToRdfTest`) - the result of processing
  Metadata to JSON with one or more referenced CSV files.

* Metadata to RDF Test (`csvt:CsvToRdfTest`) - the result of processing
  Metadata to RDF (Turtle) with one or more referenced CSV files and testing results using RDF isomorphism.

The manifest.ttl file in this directory lists all of the tests in the
CSV WG's test suite. Each test is one of the above tests. All
tests have a name (`mf:name`) and an input (`mf:action`). Evaluation
tests have an expected result (`mf:result`).

* An implementation passes a Mapping test if it parses the input
  into a form which can be directly compared with the expected result.

Tests may have options which should be used to change processor behavior:

* noProv - Do not output provenance triples for RDF tests; these triples are optional and make comparison using RDF isomorphism impossible.

Tests may also have a `csvt:link` property indicating an HTTP Link header which should be returned when serving the `mf:action` file; processors should act as if this value was received in an HTTP request if it is not otherwise set.

The home of the test suite is <http://www.w3.org/2013/csvw/tests/>.
The base IRI for parsing each file is `mf:action`.  For example, the test test001j and
test001r require relative IRI resolution against a base of
<http://www.w3.org/2013/csvw/tests/test001.csv>.
