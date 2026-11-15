"""query(debug=True) explains queries without results; GRAPH patterns used to panic."""

import polars as pl

from maplib import Model

EX = "http://example.net/ns#"
G1 = "urn:graph:one"
G2 = "urn:graph:two"


def _model():
    m = Model()
    m.reads(f"<{EX}a> <{EX}p> <{EX}b> .", format="ntriples", graph=G1)
    m.reads(f"<{EX}b> <{EX}q> <{EX}c> .", format="ntriples", graph=G2)
    return m


def test_debug_graph_variable_with_results():
    m = Model()
    m.reads("<urn:a> <urn:p> <urn:b> .", format="ntriples", graph="urn:g1")
    df = m.query("SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }", debug=True)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 1


def test_debug_graph_variable_no_results():
    m = _model()
    sm = m.query(
        f"SELECT * WHERE {{ GRAPH ?g {{ ?s <{EX}p> <{EX}nothing> }} }}",
        debug=True,
        solution_mappings=True,
    )
    assert sm.mappings.height == 0
    assert sm.debug is not None
    assert "GRAPH ?g" in sm.debug


def test_debug_graph_variable_join_across_graphs():
    # Each pattern matches in some graph, but not both in the same graph.
    m = _model()
    sm = m.query(
        f"SELECT * WHERE {{ GRAPH ?g {{ ?s <{EX}p> ?o . ?o <{EX}q> ?x }} }}",
        debug=True,
        solution_mappings=True,
    )
    assert sm.mappings.height == 0
    assert sm.debug is not None
    assert "GRAPH ?g" in sm.debug


def test_debug_graph_variable_joined_outside():
    m = _model()
    sm = m.query(
        f"SELECT * WHERE {{ GRAPH ?g {{ ?s <{EX}p> ?o }} GRAPH ?h {{ ?o <{EX}nothing> ?x }} }}",
        debug=True,
        solution_mappings=True,
    )
    assert sm.mappings.height == 0
    assert sm.debug is not None
    assert "GRAPH ?h" in sm.debug


def test_debug_named_graph_explains_inner_pattern():
    m = _model()
    sm = m.query(
        f"SELECT * WHERE {{ GRAPH <{G1}> {{ ?s <{EX}q> ?o }} }}",
        debug=True,
        solution_mappings=True,
    )
    assert sm.mappings.height == 0
    assert sm.debug is not None
    assert "triple pattern has no solution mappings" in sm.debug


def test_debug_ask_graph_variable():
    m = _model()
    assert m.query(
        f"ASK {{ GRAPH ?g {{ ?s <{EX}nothing> ?o }} }}", debug=True
    ) is False
