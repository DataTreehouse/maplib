"""SELECT results as SPARQL JSON (`query(..., return_json=True)`), written with maplib's own
N-Triples serializers in parallel chunks. Exact output for each kind of term, validity as read by
rdflib, and order and unbound values over results that span several chunks."""
import io
import json

import pytest
import rdflib
from maplib import Model

EX = "http://ex.org/"
X = "http://www.w3.org/2001/XMLSchema#"
PFX = f"PREFIX : <{EX}> PREFIX xsd: <{X}> "


def bindings(m, q):
    return json.loads(m.query(PFX + q, return_json=True))["results"]["bindings"]


def one(m, ntriples_object):
    m = Model()
    m.reads(f"<{EX}s> <{EX}p> {ntriples_object} .", format="ntriples")
    (b,) = bindings(m, "SELECT ?o WHERE { :s :p ?o }")
    return b["o"]


@pytest.mark.parametrize(
    "obj,expected",
    [
        (f"<{EX}o>", {"type": "uri", "value": f"{EX}o"}),
        ('"plain"', {"type": "literal", "value": "plain"}),
        ('"hei"@nb', {"type": "literal", "value": "hei", "xml:lang": "nb"}),
        ('"hello"@en-GB', {"type": "literal", "value": "hello", "xml:lang": "en-gb"}),
        (
            '"q\\"b\\\\n\\nt\\tc\\u0001d\\u0008f\\u000C"',
            {"type": "literal", "value": 'q"b\\n\nt\tc\x01d\x08f\x0c'},
        ),
        ('"æøå 日本 😀"', {"type": "literal", "value": "æøå 日本 😀"}),
        (f'"1"^^<{X}integer>', {"type": "literal", "value": "1", "datatype": f"{X}integer"}),
        (f'"-42"^^<{X}int>', {"type": "literal", "value": "-42", "datatype": f"{X}int"}),
        (f'"1.5"^^<{X}double>', {"type": "literal", "value": "1.5", "datatype": f"{X}double"}),
        (f'"INF"^^<{X}double>', {"type": "literal", "value": "INF", "datatype": f"{X}double"}),
        (f'"3.14"^^<{X}decimal>', {"type": "literal", "value": "3.14", "datatype": f"{X}decimal"}),
        (f'"true"^^<{X}boolean>', {"type": "literal", "value": "true", "datatype": f"{X}boolean"}),
        (f'"2020-01-01"^^<{X}date>', {"type": "literal", "value": "2020-01-01", "datatype": f"{X}date"}),
        (
            f'"2020-01-01T12:30:00Z"^^<{X}dateTime>',
            {"type": "literal", "value": "2020-01-01T12:30:00Z", "datatype": f"{X}dateTime"},
        ),
        (
            f'"2020-01-01T12:30:00+02:00"^^<{X}dateTime>',
            {"type": "literal", "value": "2020-01-01T12:30:00+02:00", "datatype": f"{X}dateTime"},
        ),
        (f'"custom"^^<{EX}myType>', {"type": "literal", "value": "custom", "datatype": f"{EX}myType"}),
    ],
)
def test_terms(obj, expected):
    assert one(None, obj) == expected


def test_blank_nodes_and_unbound_and_several_types():
    m = Model()
    m.reads(
        f"""_:b1 <{EX}p> _:b2 .
            _:b1 <{EX}q> "x" .
            <{EX}a> <{EX}p> "1"^^<{X}integer> .
            <{EX}a> <{EX}p> <{EX}z> .""",
        format="ntriples",
    )
    rows = bindings(m, "SELECT ?s ?o ?x WHERE { ?s :p ?o OPTIONAL { ?s :q ?x } } ORDER BY ?s ?o")
    kinds = sorted((r["s"]["type"], r["o"]["type"], "x" in r) for r in rows)
    assert kinds == [("bnode", "bnode", True), ("uri", "literal", False), ("uri", "uri", False)]
    blank = next(r for r in rows if r["s"]["type"] == "bnode")
    assert not blank["s"]["value"].startswith("_:") and blank["x"] == {"type": "literal", "value": "x"}
    # The same blank node has the same label within a result.
    rows = bindings(m, "SELECT ?s ?t WHERE { ?s :q ?x BIND(?s AS ?t) }")
    assert rows[0]["s"] == rows[0]["t"] and rows[0]["s"]["type"] == "bnode"


def test_head_and_empty_results_and_values():
    m = Model()
    m.reads(f"<{EX}a> <{EX}p> <{EX}b> .", format="ntriples")
    d = json.loads(m.query(PFX + "SELECT ?x ?y WHERE { :a :p ?x FILTER(false) }", return_json=True))
    assert d["results"]["bindings"] == [] and set(d["head"]["vars"]) <= {"x", "y"}
    rows = bindings(m, 'SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 UNDEF) (UNDEF "b") (:z 2.5) } }')
    assert rows == [
        {"x": {"type": "literal", "value": "1", "datatype": f"{X}integer"}},
        {"y": {"type": "literal", "value": "b"}},
        {"x": {"type": "uri", "value": f"{EX}z"}, "y": {"type": "literal", "value": "2.5", "datatype": f"{X}decimal"}},
    ]


def test_rdflib_reads_it():
    m = Model()
    m.reads(
        f"""<{EX}a> <{EX}p> "q\\"\\n\\u0001"@en .
            <{EX}a> <{EX}p> "7"^^<{X}long> .
            _:b <{EX}p> <{EX}c> .""",
        format="ntriples",
    )
    text = m.query(PFX + "SELECT ?s ?o WHERE { ?s :p ?o }", return_json=True)
    res = rdflib.query.Result.parse(io.StringIO(text), format="json")
    assert len(list(res)) == 3


def test_large_result_keeps_order_across_chunks():
    n = 30_000
    m = Model()
    m.reads(
        "\n".join(
            f'<{EX}s{i:05d}> <{EX}p> "{i}"^^<{X}integer> .'
            + (f'\n<{EX}s{i:05d}> <{EX}q> "q{i}"@en .' if i % 3 == 0 else "")
            for i in range(n)
        ),
        format="ntriples",
    )
    rows = bindings(m, "SELECT ?s ?v ?w WHERE { ?s :p ?v OPTIONAL { ?s :q ?w } } ORDER BY ?v")
    assert len(rows) == n
    assert [int(r["v"]["value"]) for r in rows] == list(range(n))
    assert all(("w" in r) == (i % 3 == 0) for i, r in enumerate(rows))
    assert rows[3000]["w"] == {"type": "literal", "value": "q3000", "xml:lang": "en"}
