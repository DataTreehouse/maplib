"""COUNT of all triples is answered from the table heights; check it against the general path."""
from maplib import Model, TRANSIENT_GRAPH

DATA = """
<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .
<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .
<http://ex.org/a> <http://ex.org/p> "x" .
<http://ex.org/a> <http://ex.org/q> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/b> <http://ex.org/q> "y"@en .
_:b1 <http://ex.org/p> <http://ex.org/a> .
"""

COUNT_STAR = "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"


def count(m, q=COUNT_STAR, **kwargs):
    return m.query(q, **kwargs)["c"][0]


def general_count(m, **kwargs):
    # Not covered by the fast path (an extra FILTER), so this takes the general path.
    return count(m, "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o FILTER(true) }", **kwargs)


def test_count_all_triples_matches_general_path():
    m = Model()
    m.reads(DATA, format="ntriples")
    assert count(m) == 5
    assert general_count(m) == 5
    assert count(m, "SELECT (COUNT(?s) AS ?c) WHERE { ?s ?p ?o }") == 5
    assert count(m, "SELECT (COUNT(?o) AS ?c) (COUNT(*) AS ?d) WHERE { ?s ?p ?o }") == 5
    df = m.query("SELECT (COUNT(*) AS ?c) (COUNT(?p) AS ?d) WHERE { ?s ?p ?o }")
    assert df["d"][0] == 5
    assert df["c"].dtype == m.query(
        "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o FILTER(true) }"
    )["c"].dtype


def test_count_all_triples_after_insert_and_delete():
    m = Model()
    m.reads(DATA, format="ntriples")
    m.update(
        """INSERT DATA { <http://ex.org/c> <http://ex.org/p> <http://ex.org/b> .
                         <http://ex.org/a> <http://ex.org/p> <http://ex.org/b> . }"""
    )
    assert count(m) == 6 == general_count(m)
    m.update("DELETE DATA { <http://ex.org/a> <http://ex.org/p> <http://ex.org/b> . }")
    assert count(m) == 5 == general_count(m)


def test_count_all_triples_graphs():
    m = Model()
    m.reads(DATA, format="ntriples")
    m.reads(
        '<http://ex.org/z> <http://ex.org/p> "z" .', format="ntriples", graph="urn:g:2"
    )
    assert count(m) == 5
    assert count(m, graph="urn:g:2") == 1
    assert count(m, "SELECT (COUNT(*) AS ?c) FROM <urn:g:2> WHERE { ?s ?p ?o }") == 1


def test_count_all_triples_with_transient_graph():
    m = Model()
    m.reads(DATA, format="ntriples")
    # A triple both in the default and the transient graph is counted once, as in the merge.
    m.reads(
        """<http://ex.org/a> <http://ex.org/p> "x" .
           <http://ex.org/t> <http://ex.org/p> "t" .""",
        format="ntriples",
        graph=TRANSIENT_GRAPH,
    )
    assert count(m) == 6 == general_count(m)
    assert count(m, include_graphs=[]) == 5 == general_count(m, include_graphs=[])


def test_count_all_triples_not_fast_path_shapes():
    m = Model()
    m.reads(DATA, format="ntriples")
    assert count(m, "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }") == 3
    assert count(m, "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?s }") == 0
    df = m.query("SELECT ?p (COUNT(*) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?p ORDER BY ?p")
    assert df["c"].to_list() == [3, 2]
