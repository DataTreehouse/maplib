"""Zero-length property paths from variables bound by other patterns match every value of the
variable, also values without triples of the path (SPARQL 1.1 section 18.4)."""

import pytest
from maplib import Model

P = "PREFIX ex: <http://ex/> "
DATA = """@prefix ex: <http://ex/> .
ex:a ex:p ex:b . ex:c a ex:Node . ex:a a ex:Node . ex:b a ex:Node .
ex:d ex:q ex:e .
"""


def model():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def pairs(m, query):
    df = m.query(P + query)
    return {(r["x"], r["y"]) for r in df.iter_rows(named=True)}


A, B, C = "<http://ex/a>", "<http://ex/b>", "<http://ex/c>"


@pytest.mark.parametrize("path", ["ex:p*", "ex:p?", "ex:p{0,2}", "ex:p{,1}", "(ex:p|ex:q)*"])
def test_subject_bound(path):
    m = model()
    got = pairs(m, f"SELECT ?x ?y WHERE {{ ?x a ex:Node . ?x {path} ?y }}")
    assert got == {(A, A), (A, B), (B, B), (C, C)}
    # The order of the patterns does not matter.
    got = pairs(m, f"SELECT ?x ?y WHERE {{ ?x {path} ?y . ?x a ex:Node }}")
    assert got == {(A, A), (A, B), (B, B), (C, C)}


@pytest.mark.parametrize("path", ["ex:p*", "ex:p?", "ex:p{0,2}", "ex:p{,1}"])
def test_object_bound(path):
    m = model()
    got = pairs(m, f"SELECT ?x ?y WHERE {{ ?y a ex:Node . ?x {path} ?y }}")
    assert got == {(A, A), (A, B), (B, B), (C, C)}


@pytest.mark.parametrize("path", ["ex:p*", "ex:p?", "ex:p{0,2}"])
def test_values_bound(path):
    m = model()
    got = pairs(m, f"SELECT ?x ?y WHERE {{ VALUES ?x {{ ex:c ex:a }} ?x {path} ?y }}")
    assert got == {(A, A), (A, B), (C, C)}


def test_both_bound_and_same_variable():
    m = model()
    got = pairs(m, "SELECT ?x ?y WHERE { ?x a ex:Node . ?y a ex:Node . ?x ex:p* ?y }")
    assert got == {(A, A), (A, B), (B, B), (C, C)}
    df = m.query(P + "SELECT ?x WHERE { ?x a ex:Node . ?x ex:p* ?x }")
    assert set(df["x"].to_list()) == {A, B, C}


def test_only_nodes_of_the_graph():
    # Zero length paths from a variable match the nodes of the graph (W3C values_and_path).
    m = model()
    assert m.query(P + "SELECT * WHERE { VALUES ?v { 1 } ?v ex:p? ?v }").height == 0
    got = pairs(m, "SELECT ?x ?y WHERE { VALUES ?x { ex:c ex:nowhere } ?x ex:p* ?y }")
    assert got == {(C, C)}
    # A constant matches itself, also outside the graph.
    df = m.query(P + "SELECT ?y WHERE { ex:nowhere ex:p* ?y }")
    assert df["y"].to_list() == ["<http://ex/nowhere>"]


def test_one_or_more_is_unchanged():
    m = model()
    got = pairs(m, "SELECT ?x ?y WHERE { ?x a ex:Node . ?x ex:p+ ?y }")
    assert got == {(A, B)}


def test_island_count():
    # Each node is in the component of itself: nodes without edges are their own islands.
    m = model()
    df = m.query(
        P
        + """SELECT (COUNT(DISTINCT ?root) AS ?n) WHERE {
          ?x a ex:Node .
          { SELECT ?x (MIN(STR(?y)) AS ?root) WHERE { ?x a ex:Node . ?x (ex:p|^ex:p)* ?y } GROUP BY ?x }
        }"""
    )
    assert df["n"][0] == 2
