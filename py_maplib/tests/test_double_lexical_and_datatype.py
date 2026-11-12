import json

import pytest

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"
FLOAT_ONE = f'"1"^^<{XSD}float>'


def bindings(m: Model, query: str):
    return json.loads(m.query(query, return_format="json"))["results"]["bindings"]


@pytest.mark.parametrize(
    "expression,lexical,datatype",
    [
        ("1.0e0 / 0", "INF", "double"),
        ("-1.0e0 / 0", "-INF", "double"),
        ("0.0e0 / 0", "NaN", "double"),
        (f"{FLOAT_ONE} / 0", "INF", "float"),
        ("1.5e0", "1.5", "double"),
    ],
)
def test_json_results_use_xsd_lexical_forms(expression, lexical, datatype):
    (b,) = bindings(Model(), f"SELECT ({expression} AS ?v) (STR({expression}) AS ?s) WHERE {{}}")
    assert b["v"] == {"type": "literal", "value": lexical, "datatype": XSD + datatype}
    assert b["s"]["value"] == lexical


def special_values_model() -> Model:
    m = Model()
    m.insert(
        """CONSTRUCT {
            <http://example.org/s> <http://example.org/inf> ?a .
            <http://example.org/s> <http://example.org/neginf> ?b .
            <http://example.org/s> <http://example.org/nan> ?c .
        } WHERE { BIND(1.0e0 / 0 AS ?a) BIND(-1.0e0 / 0 AS ?b) BIND(0.0e0 / 0 AS ?c) }"""
    )
    return m


@pytest.mark.parametrize("format", ["ntriples", "turtle", "nquads", "trig"])
def test_serialized_special_doubles(format):
    s = special_values_model().writes(format=format)
    assert "inf" not in s.replace("http://example.org/inf", "").replace(
        "http://example.org/neginf", ""
    )
    for lexical in ['"INF"', '"-INF"', '"NaN"']:
        assert lexical in s


@pytest.mark.parametrize("format", ["ntriples", "turtle"])
def test_special_doubles_round_trip(format):
    m = special_values_model()
    m2 = Model()
    m2.reads(m.writes(format=format), format=format)
    df = m2.query(
        "SELECT (STR(?o) AS ?v) (DATATYPE(?o) AS ?t) WHERE { ?s ?p ?o } ORDER BY ?v"
    )
    assert df["v"].to_list() == ["-INF", "INF", "NaN"]
    assert set(df["t"].to_list()) == {f"<{XSD}double>"}


def test_datatype_of_unbound_is_unbound():
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        :a :q 1 ; :l "hei"@no ; :n 5 .
        :b :q 2 .""",
        format="turtle",
    )
    rows = bindings(
        m,
        """PREFIX : <http://example.org/>
        SELECT ?s (DATATYPE(?l) AS ?dl) (DATATYPE(?n) AS ?dn) WHERE {
            ?s :q ?q OPTIONAL { ?s :l ?l } OPTIONAL { ?s :n ?n }
        } ORDER BY ?s""",
    )
    a, b = rows
    assert a["dl"]["value"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
    assert a["dn"]["value"] == XSD + "integer"
    assert "dl" not in b and "dn" not in b


def test_datatype_of_error_is_unbound():
    (b,) = bindings(Model(), "SELECT (DATATYPE(1 / 0) AS ?t) (DATATYPE(1 / 2) AS ?u) WHERE {}")
    assert "t" not in b
    assert b["u"]["value"] == XSD + "decimal"
