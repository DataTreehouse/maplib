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


OTHER = "http://other.net/ns#"


def _people():
    m = Model()
    m.reads(
        f"""
        <{EX}a> <{EX}knows> <{EX}b> .
        <{EX}a> <{EX}age> 42 .
        <{EX}a> <{EX}name> "Ann" .
        <{EX}b> <{EX}name> "Bob"@en .
        """,
        format="turtle",
    )
    return m


def _debug(m, query):
    sm = m.query(query, debug=True, solution_mappings=True)
    assert sm.mappings.height == 0
    assert sm.debug is not None
    return sm.debug


def test_debug_predicate_of_another_namespace():
    dbg = _debug(_people(), f"SELECT * WHERE {{ ?x <{OTHER}knows> ?y }}")
    assert f"The predicate <{OTHER}knows> is not used in the queried graph" in dbg
    assert f"Predicates with the same local name: <{EX}knows>" in dbg


def test_debug_predicate_of_another_graph():
    dbg = _debug(_model(), f"SELECT * WHERE {{ ?x <{EX}p> ?y }}")
    assert f"It is used in the graph <{G1}>" in dbg


def test_debug_object_with_another_datatype():
    dbg = _debug(_people(), f'SELECT * WHERE {{ ?x <{EX}age> "42" }}')
    assert "written like these objects" in dbg
    assert '"42"^^<http://www.w3.org/2001/XMLSchema#integer>' in dbg


def test_debug_object_without_language_tag():
    dbg = _debug(_people(), f'SELECT * WHERE {{ ?x <{EX}name> "Bob" }}')
    assert '"Bob"@en' in dbg


def test_debug_subject_and_object_change_places():
    dbg = _debug(_people(), f"SELECT * WHERE {{ <{EX}b> <{EX}knows> ?x }}")
    assert "the subject and the object change places" in dbg
    assert f"<{EX}b> has no triples with the predicate" not in dbg


def test_debug_other_objects_of_the_subject():
    dbg = _debug(_people(), f'SELECT * WHERE {{ <{EX}a> <{EX}name> "Anna" }}')
    assert 'are instead, for example: "Ann"' in dbg


def test_debug_unknown_subject():
    dbg = _debug(_people(), f"SELECT * WHERE {{ <{EX}nobody> <{EX}name> ?n }}")
    assert f"<{EX}nobody> is not the subject of any triple" in dbg


def test_debug_join_on_terms_of_different_kinds():
    dbg = _debug(
        _people(), f"SELECT * WHERE {{ ?x <{EX}knows> ?y . ?z <{EX}age> ?y }}"
    )
    assert "?y is never the same term in both" in dbg
    assert "IRI" in dbg
    assert "XMLSchema#integer" in dbg


def test_debug_join_without_common_terms():
    dbg = _debug(
        _people(), f"SELECT * WHERE {{ ?x <{EX}knows> ?y . ?y <{EX}knows> ?z }}"
    )
    assert "They have no term of ?y in common" in dbg


def test_debug_filter_conditions():
    dbg = _debug(
        _people(),
        f"SELECT * WHERE {{ ?x <{EX}age> ?a FILTER(?a > 100 && ?a < 1000) }}",
    )
    assert "The graph pattern has 1 solution mapping, the expression is true for none" in dbg
    assert "?a is <http://www.w3.org/2001/XMLSchema#integer>" in dbg
    never = [l for l in dbg.splitlines() if "Never true on its own" in l]
    assert len(never) == 1
    assert "100" in never[0] and "1000" not in never[0]


def test_debug_filter_conditions_true_apart():
    m = _people()
    m.reads(f"<{EX}b> <{EX}age> 7 .", format="turtle")
    dbg = _debug(
        m, f"SELECT * WHERE {{ ?x <{EX}age> ?a FILTER(?a > 10 && ?a < 10) }}"
    )
    assert "The graph pattern has 2 solution mappings" in dbg
    assert "but not all of them for the same one" in dbg


def test_debug_offset_past_the_solution_mappings():
    dbg = _debug(_people(), "SELECT * WHERE { ?s ?p ?o } OFFSET 100")
    assert "at least one solution mapping" not in dbg
    assert "No solution mappings after the following graph pattern" in dbg


def test_debug_with_results_is_unchanged():
    sm = _people().query(
        "SELECT * WHERE { ?s ?p ?o }", debug=True, solution_mappings=True
    )
    assert sm.mappings.height == 4
    assert "at least one solution mapping" in sm.debug
