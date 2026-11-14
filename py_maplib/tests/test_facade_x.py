"""
Facade-X compliance tests for map_json, map_xml and map_df.

There is no official Facade-X test suite. The reference implementation, SPARQL Anything,
tests its triplifiers by comparing the graph of an input file with an expected Turtle file.
``testdata/facade_x`` is a copy of those pairs from
https://github.com/SPARQL-Anything/sparql.anything (commit
d0395728d43b92576b1c8f13e9e51735dc98f93f, Apache License 2.0, see
``testdata/facade_x/README.md``), restricted to the tests whose options maplib can express,
plus the "Default Transformation" examples of its JSON and XML format documentation.

Each input is mapped with maplib and compared, up to blank node renaming, with the expected
graph after both graphs are brought to a common form that removes the deliberate differences
between maplib and SPARQL Anything:

* Container IRIs (``urn:maplib:...`` and SPARQL Anything's ``http://www.example.org/document...``
  with ``blank-nodes=false``) are treated as blank nodes.
* Children: SPARQL Anything uses ``rdf:_1, rdf:_2, ...``, maplib uses ``fx:child`` with
  ``fx:childNumber`` on child containers. Both become ``fx:child``, and ``fx:childNumber`` is
  dropped, so the order of children is not compared.
* The root type ``fx:Root`` (SPARQL Anything) is compared as ``fx:root`` (maplib).
* Numbers are compared by value: SPARQL Anything gives JSON numbers as ``xsd:decimal``,
  maplib as ``xsd:long`` or ``xsd:double``.
* CSV files are read with Polars with all columns as strings, as SPARQL Anything does, and
  mapped with map_df. Only tests with ``csv.headers=true`` are included, since a DataFrame
  always has column names.

Tests that maplib currently does not pass are listed in ``facade_x_known_failures.txt`` and
are marked as strict xfails, so a test that starts passing is reported (XPASS) and should be
removed from that list. Run pytest with ``--runxfail`` to see the actual failures.
"""

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List

import polars as pl
import pytest

from maplib import Model

from .w3c_rdf import dump_dataset, format_quads, isomorphic

TESTDATA = Path(__file__).parent / "testdata" / "facade_x"
KNOWN_FAILURES = Path(__file__).parent / "facade_x_known_failures.txt"

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XSD = "http://www.w3.org/2001/XMLSchema#"
FX = "http://sparql.xyz/facade-x/ns/"

CONTAINER_PREFIXES = ("urn:maplib:", "http://www.example.org/document")
NUMERIC_TYPES = {
    XSD + t
    for t in (
        "decimal", "integer", "long", "int", "short", "byte", "double", "float",
        "nonNegativeInteger", "positiveInteger", "nonPositiveInteger", "negativeInteger",
        "unsignedLong", "unsignedInt", "unsignedShort", "unsignedByte",
    )
}

# (test id, mapper, input, expected result, keyword arguments for the CSV reader)
# The SPARQL Anything options of each test are given in the comments, blank-nodes and slice
# do not change the graph up to the normalization above.
CASES = [
    # MoreJSONTriplifierTest
    ("json/Object", "json", "json/Object.json", "json/Object.ttl", {}),
    ("json/ValueTypes_1", "json", "json/ValueTypes_1.json", "json/ValueTypes_1.ttl", {}),
    ("json/SliceArray", "json", "json/SliceArray.json", "json/SliceArray.ttl", {}),
    ("json/SliceArray_2", "json", "json/SliceArray_2.json", "json/SliceArray_2.ttl", {}),
    # JSONNullValuesTest, json.null-values=true
    ("json/NullValues", "json", "json/NullValues.json", "json/NullValues.ttl", {}),
    ("json/NullValues_2", "json", "json/NullValues_2.json", "json/NullValues_2.ttl", {}),
    # MoreXMLTriplifierTest
    ("xml/Simple", "xml", "xml/Simple.xml", "xml/Simple.ttl", {}),
    ("xml/Books", "xml", "xml/Books.xml", "xml/Books.ttl", {}),
    ("xml/Lang", "xml", "xml/Lang.xml", "xml/Lang.ttl", {}),
    # xml.lang-tags=true, the default
    ("xml/LangTags", "xml", "xml/LangTags.xml", "xml/LangTags.ttl", {}),
    # XMLTriplifierTest.issue_611
    ("xml/test2", "xml", "xml/test2.xml", "xml/test2.ttl", {}),
    # TSVTest, csv.headers=true, csv.delimiter=\t
    ("csv/Tsv", "csv", "csv/Tsv.tsv", "csv/Tsv.ttl", {"separator": "\t"}),
    # QuoteCharTest, csv.headers=true
    ("csv/QuoteCharTrue", "csv", "csv/QuoteCharTrue.csv", "csv/QuoteCharTrue.ttl", {}),
    # QuoteCharTest, csv.headers=true, csv.delimiter=\t, csv.quote-char=false
    ("csv/NoQuote", "csv", "csv/NoQuote.csv", "csv/NoQuote.ttl",
     {"separator": "\t", "quote_char": None}),
    # docs/formats/JSON.md and XML.md, Default Transformation
    ("docs/default_json", "json", "docs/default.json", "docs/default_json.ttl", {}),
    ("docs/default_xml", "xml", "docs/default.xml", "docs/default_xml.ttl", {}),
]


def load_known_failures() -> dict:
    known = {}
    if KNOWN_FAILURES.exists():
        for line in KNOWN_FAILURES.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tid, _, reason = line.partition("#")
            known[tid.strip()] = reason.strip() or "known failure"
    return known


def build_params():
    known = load_known_failures()
    params = []
    for tid, mapper, action, result, csv_kwargs in CASES:
        marks = []
        if tid in known:
            marks.append(pytest.mark.xfail(reason=known[tid], strict=True))
        params.append(pytest.param(mapper, action, result, csv_kwargs, id=tid, marks=marks))
    return params


def map_input(mapper: str, path: Path, csv_kwargs: dict) -> Model:
    m = Model()
    if mapper == "json":
        m.map_json(path)
    elif mapper == "xml":
        m.map_xml(path)
    else:
        m.map_df(pl.read_csv(path, infer_schema=False, **csv_kwargs))
    return m


def normalize_term(t):
    if t[0] == "uri":
        if t[1].startswith(CONTAINER_PREFIXES):
            return ("bnode", t[1])
        if t[1] == FX + "Root":
            return ("uri", FX + "root")
        return t
    if t[0] == "literal" and t[2] in NUMERIC_TYPES:
        try:
            value = Decimal(t[1].strip())
        except InvalidOperation:
            return t
        if value.is_finite():
            # 1.0 and 1 are the same number.
            value = value.normalize()
            return ("literal", format(value, "f"), "number", None, None)
    return t


def normalize(quads: List[tuple]) -> List[tuple]:
    out = set()
    for s, p, o, _ in quads:
        if p == ("uri", FX + "childNumber"):
            continue
        if p[0] == "uri" and p[1].startswith(RDF + "_") and p[1][len(RDF) + 1:].isdigit():
            p = ("uri", FX + "child")
        # SPARQL Anything puts the graph in a named graph with blank-nodes=false, maplib in the
        # default graph, the graph name is not compared.
        out.add((normalize_term(s), p, normalize_term(o), None))
    return list(out)


@pytest.mark.parametrize("mapper,action,result,csv_kwargs", build_params())
def test_facade_x(mapper: str, action: str, result: str, csv_kwargs: dict):
    actual = normalize(dump_dataset(map_input(mapper, TESTDATA / action, csv_kwargs)))
    expected_model = Model()
    expected_model.read(TESTDATA / result, format="turtle")
    expected = normalize(dump_dataset(expected_model))
    assert isomorphic(expected, actual), (
        f"Graphs differ after normalization.\n"
        f"Expected:\n{format_quads(expected, limit=80)}\nActual:\n{format_quads(actual, limit=80)}"
    )
