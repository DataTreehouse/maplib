import polars as pl
from maplib import Model, TRANSIENT_GRAPH

PREFIX = "PREFIX ex: <http://example.org/>\n"


def count(m: Model, query: str, **kwargs) -> int:
    return m.query(PREFIX + "SELECT (COUNT(*) AS ?c) WHERE { " + query + " }", **kwargs)["c"][0]


def model_with_transient_copy() -> Model:
    m = Model()
    m.reads(
        """
        @prefix ex: <http://example.org/> .
        ex:a ex:name "A" .
        """,
        format="turtle",
    )
    m.insert(
        PREFIX + 'CONSTRUCT { ex:copy ex:name "A" } WHERE { ex:a ex:name "A" }',
        target_graph=TRANSIENT_GRAPH,
    )
    return m


def test_transient_graph_is_included_by_default():
    m = model_with_transient_copy()
    assert count(m, "ex:copy ex:name ?n") == 1
    assert count(m, "?s ex:name ?n") == 2


def test_transient_graph_excluded_with_empty_include_graphs():
    m = model_with_transient_copy()
    assert count(m, "ex:copy ex:name ?n", include_graphs=[]) == 0
    assert count(m, "?s ex:name ?n", include_graphs=[]) == 1
    assert count(m, "ex:copy ex:name ?n", graph=TRANSIENT_GRAPH, include_graphs=[]) == 1


def test_transient_graph_is_not_written():
    m = model_with_transient_copy()
    nt = m.writes(format="ntriples")
    assert "copy" not in nt
    assert "<http://example.org/a>" in nt
    nq = m.writes(format="nquads")
    assert "copy" not in nq
    assert "copy" in m.writes(format="ntriples", graph=TRANSIENT_GRAPH)


def test_triples_in_both_graphs_are_not_duplicated():
    m = model_with_transient_copy()
    # Re-assert, in the transient graph, a triple that already exists in the default graph.
    m.insert(
        PREFIX + "CONSTRUCT { ex:a ex:name ?n } WHERE { ex:a ex:name ?n }",
        target_graph=TRANSIENT_GRAPH,
    )
    assert count(m, "ex:copy ex:name ?n") == 1
    assert count(m, "ex:a ex:name ?n") == 1
    assert count(m, "?s ex:name ?n") == 2


def test_include_other_named_graph():
    m = model_with_transient_copy()
    m.reads(
        """
        @prefix ex: <http://example.org/> .
        ex:b ex:name "B" .
        """,
        format="turtle",
        graph="urn:graph:b",
    )
    assert count(m, "?s ex:name ?n", include_graphs=["urn:graph:b"]) == 2
    assert count(m, "?s ex:name ?n", include_graphs=["urn:graph:b", TRANSIENT_GRAPH]) == 3
    # GRAPH ?g still ranges over the named graphs
    df = m.query(PREFIX + "SELECT ?g WHERE { GRAPH ?g { ?s ex:name ?n } }")
    assert set(df["g"].to_list()) == {"<urn:graph:b>", f"<{TRANSIENT_GRAPH}>"}


def test_read_and_map_into_transient_graph():
    m = Model()
    m.reads(
        '<http://example.org/a> <http://example.org/name> "A" .',
        format="ntriples",
        graph=TRANSIENT_GRAPH,
    )
    m.map_json('{"name": "n0"}', graph=TRANSIENT_GRAPH)
    assert m.size() == 0
    assert m.size(graph=TRANSIENT_GRAPH) > 1
    assert count(m, "?s ex:name ?n") == 1
    assert m.writes(format="ntriples") == ""


def test_get_predicate_include_graphs():
    m = model_with_transient_copy()
    name = "http://example.org/name"
    assert [str(x.iri) for x in m.get_predicate_iris()] == [name]
    sms = m.get_predicate(m.get_predicate_iris()[0])
    assert sum(sm.mappings.height for sm in sms) == 1
    sms = m.get_predicate(m.get_predicate_iris()[0], include_graphs=[TRANSIENT_GRAPH])
    assert sum(sm.mappings.height for sm in sms) == 2
