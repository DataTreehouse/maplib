"""maplib:shortestPath from the solutions of a subquery, on a model whose triples came from
Model.insert. Found by the release gate on 2026-11-20 through validation_portal_core's capability
probe (api/capabilities.py): the query gives no solutions since the optimizations between
3d2402fb (right) and 056247e9 (wrong), while the same path from a constant or from VALUES, and the
same triples read with Model.reads, give the path."""

import pytest

from maplib import Model

P = "PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>\n"
TRIPLES = "<urn:probe:a> <urn:probe:p> <urn:probe:b> . <urn:probe:b> <urn:probe:p> <urn:probe:c> ."
FROM_SUBQUERY = P + """SELECT ?length WHERE {
  { SELECT ?s WHERE { ?s ?p ?o FILTER(isIRI(?o)) } LIMIT 1 }
  ?s maplib:shortestPath ?path .
  ?path maplib:anyPredicate "both" ; maplib:object ?t ; maplib:length ?length .
} LIMIT 1"""
FROM_CONSTANT = P + """SELECT ?length WHERE {
  <urn:probe:a> maplib:shortestPath ?path .
  ?path maplib:anyPredicate "both" ; maplib:object ?t ; maplib:length ?length .
}"""


def inserted() -> Model:
    m = Model()
    m.insert(f"CONSTRUCT {{ {TRIPLES} }} WHERE {{}}")
    return m


def read() -> Model:
    m = Model()
    m.reads(TRIPLES.replace(" . ", " .\n") + "\n", format="ntriples")
    return m


def test_from_a_constant_after_insert():
    assert inserted().query(FROM_CONSTANT).height == 3


def test_from_a_subquery_after_reads():
    assert read().query(FROM_SUBQUERY).height == 1


@pytest.mark.xfail(strict=True, reason="no solutions from the subquery's start on a model built by "
                   "Model.insert since 056247e9 (3d2402fb gave one)")
def test_from_a_subquery_after_insert():
    assert inserted().query(FROM_SUBQUERY).height == 1
