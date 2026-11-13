"""DELETE of all triples, or of all triples with a predicate, drops whole tables; check it
against the same deletes done through the general path."""
import polars as pl
from maplib import Model, TRANSIENT_GRAPH

DATA = """
<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .
<http://ex.org/a> <http://ex.org/p> "x" .
<http://ex.org/a> <http://ex.org/q> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/b> <http://ex.org/q> "y"@en .
_:b1 <http://ex.org/p> <http://ex.org/a> .
<http://ex.org/b> <http://ex.org/r> <http://ex.org/a> .
"""

ALL = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o"


def model():
    m = Model()
    m.reads(DATA, format="ntriples")
    return m


def triples(m, **kwargs):
    return m.query(ALL, **kwargs).with_columns(pl.all().cast(pl.String))


def test_delete_predicate_matches_general_path():
    fast, slow = model(), model()
    fast.update("DELETE WHERE { ?s <http://ex.org/p> ?o }")
    # The FILTER keeps this off the fast path.
    slow.update("DELETE { ?s <http://ex.org/p> ?o } WHERE { ?s <http://ex.org/p> ?o FILTER(true) }")
    assert triples(fast).equals(triples(slow))
    assert triples(fast).height == 3


def test_delete_predicate_modify_form():
    m = model()
    m.update("DELETE { ?x <http://ex.org/q> ?y } WHERE { ?x <http://ex.org/q> ?y }")
    assert triples(m).height == 4
    assert m.query("SELECT * WHERE { ?s <http://ex.org/q> ?o }").height == 0


def test_delete_all_then_insert():
    m = model()
    m.update("DELETE WHERE { ?s ?p ?o }")
    assert triples(m).height == 0
    assert m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")["c"][0] == 0
    m.update("INSERT DATA { <http://ex.org/n> <http://ex.org/p> <http://ex.org/m> . }")
    assert triples(m).height == 1


def test_delete_missing_predicate_is_noop():
    m = model()
    m.update("DELETE WHERE { ?s <http://ex.org/nope> ?o }")
    assert triples(m).height == 6


def test_delete_keeps_other_graphs():
    m = model()
    m.reads('<http://ex.org/z> <http://ex.org/p> "z" .', format="ntriples", graph="urn:g:2")
    m.reads('<http://ex.org/t> <http://ex.org/p> "t" .', format="ntriples", graph=TRANSIENT_GRAPH)
    m.update("DELETE WHERE { ?s ?p ?o }")
    assert triples(m, include_graphs=[]).height == 0
    assert triples(m, graph="urn:g:2", include_graphs=[]).height == 1
    assert m.query(ALL, graph=TRANSIENT_GRAPH).height == 1


def test_delete_not_fast_path_shapes():
    m = model()
    # Constant subject: only some of the triples of the predicate.
    m.update("DELETE WHERE { <http://ex.org/a> <http://ex.org/p> ?o }")
    assert triples(m).height == 4
    # A delete in another graph than the update graph.
    m.update("DELETE WHERE { ?s <http://ex.org/q> ?o }", graph="urn:g:other")
    assert triples(m).height == 4
