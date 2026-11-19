"""Zero-length property paths with both ends unbound match every node of the graph: each subject
and each object, literals too, also nodes without triples of the path (SPARQL 1.1 section 18.4,
ZeroLengthPath; Oxigraph and Jena do the same). Predicates are not nodes unless they occur as a
subject or an object."""

import pytest
from maplib import Model

P = "PREFIX ex: <http://ex/> "
DATA = """@prefix ex: <http://ex/> .
ex:a ex:p ex:b .
ex:c a ex:Node .
ex:d ex:q ex:e .
ex:e ex:name "e" .
"""

A, B, C, D, E = (f"<http://ex/{x}>" for x in "abcde")
NODE = "<http://ex/Node>"
LIT = '"e"'
NODES = {A, B, C, D, E, NODE, LIT}


def model():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def pairs(m, query):
    df = m.query(P + query)
    return {(str(r["x"]), str(r["y"])) for r in df.iter_rows(named=True)}


def identity(nodes):
    return {(n, n) for n in nodes}


@pytest.mark.parametrize("path", ["ex:p*", "ex:p?", "ex:p{0,2}", "^ex:p*", "ex:p*/ex:q*"])
def test_all_nodes(path):
    m = model()
    got = pairs(m, f"SELECT ?x ?y WHERE {{ ?x {path} ?y }}")
    forward = {(A, B)} if not path.startswith("^") else {(B, A)}
    if path == "ex:p*/ex:q*":
        forward = {(A, B), (D, E)}
    assert got == identity(NODES) | forward


def test_alternative():
    m = model()
    got = pairs(m, "SELECT ?x ?y WHERE { ?x (ex:p|ex:q)* ?y }")
    assert got == identity(NODES) | {(A, B), (D, E)}


def test_same_variable():
    m = model()
    df = m.query(P + "SELECT ?x WHERE { ?x ex:p* ?x }")
    assert {str(x) for x in df["x"].to_list()} == NODES


def test_predicate_without_triples():
    # Every node also matches a path over a predicate that is not in the graph.
    m = model()
    got = pairs(m, "SELECT ?x ?y WHERE { ?x ex:missing* ?y }")
    assert got == identity(NODES)


def test_blank_node_in_query():
    m = model()
    df = m.query(P + "SELECT ?y WHERE { _:s ex:p* ?y }")
    assert {str(y) for y in df["y"].to_list()} == NODES


def test_count():
    m = model()
    df = m.query(P + "SELECT (COUNT(*) AS ?c) WHERE { ?x ex:p* ?y }")
    assert df["c"][0] == len(NODES) + 1


def test_blank_nodes_of_the_graph():
    m = Model()
    m.reads("_:a <http://ex/q> _:b .\n<http://ex/x> <http://ex/p> <http://ex/y> .", format="ntriples")
    df = m.query(P + "SELECT ?x ?y WHERE { ?x ex:p* ?y }")
    assert df.height == 5
    df = m.query(P + "SELECT ?x WHERE { ?x ex:p* ?x FILTER(ISBLANK(?x)) }")
    assert df.height == 2


def test_one_or_more_is_unchanged():
    m = model()
    assert pairs(m, "SELECT ?x ?y WHERE { ?x ex:p+ ?y }") == {(A, B)}


def test_empty_model():
    m = Model()
    assert m.query(P + "SELECT ?x ?y WHERE { ?x ex:p* ?y }").height == 0
