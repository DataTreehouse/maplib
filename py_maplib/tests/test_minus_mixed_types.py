"""MINUS on shared variables with values of several RDF term types.

MINUS ran its own anti join, which compared the key columns as they were: a variable with IRIs and
xsd:date literals on the left and IRIs and blank nodes on the right failed with "datatypes of join
keys don't match". It now uses the join code of inner and left joins, which also compares each
solution only on the variables bound in both sides, as for variables left unbound by OPTIONAL.
Each query is compared with pyoxigraph.
"""

import json
from collections import Counter

import pyoxigraph
import pytest

from maplib import Model

DATA = """
<http://ex.org/s1> <http://ex.org/r> <http://ex.org/s2> .
<http://ex.org/s1> <http://ex.org/r> "2020-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://ex.org/s2> <http://ex.org/p> <http://ex.org/s3> .
_:b <http://ex.org/p> <http://ex.org/s3> .
<http://ex.org/s4> <http://ex.org/r> "2020-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://ex.org/s4> <http://ex.org/r> "hei"@no .
<http://ex.org/s4> <http://ex.org/r> _:c .
<http://ex.org/s5> <http://ex.org/q> "2020-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
<http://ex.org/s5> <http://ex.org/q> <http://ex.org/s2> .
<http://ex.org/s6> <http://ex.org/q> "hei"@no .
<http://ex.org/s6> <http://ex.org/q> "5"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/s3> <http://ex.org/p> "x" .
_:c <http://ex.org/t> <http://ex.org/s1> .
"""

PREFIX = "PREFIX : <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "

QUERIES = {
    # The three queries of the report
    "count, one shared variable": """
        SELECT (COUNT(*) AS ?c) WHERE { ?a :r ?b MINUS { ?b :p ?c } FILTER(true) }""",
    "one shared variable, iri and date against iri and blank node": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?b :p ?c } }""",
    "two shared variables, iri and date against iri and blank node": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?a :p ?b } }""",
    # Several types on one side only
    "single type on the left, several on the right": """
        SELECT ?a WHERE { ?a :p :s3 MINUS { ?x :r ?a } }""",
    "several types on the left, single type on the right": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?b :p :s3 } }""",
    "several types on the left, a single other type on the right": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?x :p ?b FILTER(isLiteral(?b)) } }""",
    # Several types on both sides
    "iri and date on both sides": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?x :q ?b } }""",
    "four types against four types": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?y :q ?b } }""",
    "no common type": """
        SELECT ?a ?b WHERE { ?a :r ?b FILTER(!isIRI(?b)) MINUS { ?b :p ?c } }""",
    "two shared variables, both with several types": """
        SELECT ?a ?b WHERE { { ?a :r ?b } UNION { ?a :q ?b } MINUS { { ?a :q ?b } UNION { ?b :p ?a } } }""",
    "several types on both sides of two shared variables, matching on one": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?a :q ?b } }""",
    "lang strings and integers on the right": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { :s6 :q ?b } }""",
    # Unbound variables from OPTIONAL
    "unbound on the left, one shared variable": """
        SELECT ?a ?b ?c WHERE { ?a :r ?b OPTIONAL { ?b :p ?c } MINUS { ?d :p ?c FILTER(isIRI(?c)) } }""",
    "unbound on the left, two shared variables": """
        SELECT ?a ?b ?c WHERE { ?a :r ?b OPTIONAL { ?b :p ?c } MINUS { ?b :p ?c FILTER(isLiteral(?c)) } }""",
    "unbound on the left, several types": """
        SELECT ?a ?b ?c WHERE { ?a :r ?b OPTIONAL { ?b :t ?c } MINUS { ?b ?p ?c } }""",
    "unbound on the right, one shared variable": """
        SELECT ?a WHERE { ?a ?p ?o MINUS { ?x :r ?y OPTIONAL { ?y :p ?a } } }""",
    "unbound on the right, two shared variables": """
        SELECT ?a ?b WHERE { ?a :r ?b MINUS { ?a :r ?x OPTIONAL { ?x :p ?b } } }""",
    "unbound on the right, two shared variables of several types": """
        SELECT ?a ?b WHERE { { ?a :r ?b } UNION { ?a :q ?b }
          MINUS { ?a ?p ?x FILTER(?p != :r) OPTIONAL { ?a :q ?b FILTER(isLiteral(?b)) } } }""",
    "unbound on both sides": """
        SELECT ?a ?b ?c WHERE { ?a :r ?b OPTIONAL { ?b :p ?c }
          MINUS { ?b2 :p ?c OPTIONAL { ?a :r ?b2 } } }""",
    "unbound on both sides, nothing in common in some rows": """
        SELECT ?a ?c WHERE { ?a ?p ?o OPTIONAL { ?a :p ?c }
          MINUS { ?x :q ?y OPTIONAL { ?x :r ?a } OPTIONAL { ?x :p ?c } } }""",
    "values with undef on the right": """
        SELECT ?a ?b WHERE { ?a :r ?b
          MINUS { VALUES (?a ?b) { (:s1 UNDEF) (UNDEF "hei"@no) } } }""",
}


def _term(t):
    """A term from maplib's SPARQL JSON results or from pyoxigraph, in one comparable form.
    Blank nodes compare equal whatever their labels."""
    if isinstance(t, dict):
        kind = t["type"]
        if kind == "uri":
            return ("uri", t["value"])
        if kind == "bnode":
            return ("bnode",)
        return ("literal", t["value"], t.get("datatype"), t.get("xml:lang"))
    if isinstance(t, pyoxigraph.NamedNode):
        return ("uri", t.value)
    if isinstance(t, pyoxigraph.BlankNode):
        return ("bnode",)
    dt = t.datatype.value
    lang = t.language
    if lang is not None:
        dt = None
    return ("literal", t.value, dt, lang)


def _norm(term):
    if term[0] == "literal" and term[2] == "http://www.w3.org/2001/XMLSchema#string":
        return ("literal", term[1], None, None)
    if term[0] == "literal" and term[3] is not None:
        return ("literal", term[1], None, term[3])
    return term


@pytest.fixture(scope="module")
def stores():
    m = Model()
    m.reads(DATA, format="ntriples")
    store = pyoxigraph.Store()
    store.load(DATA.encode(), pyoxigraph.RdfFormat.N_TRIPLES)
    return m, store


@pytest.mark.parametrize("name", sorted(QUERIES))
def test_like_oxigraph(stores, name):
    m, store = stores
    q = PREFIX + QUERIES[name]
    res = store.query(q)
    names = [v.value for v in res.variables]
    expected = Counter(
        tuple(sorted((n, _norm(_term(sol[n]))) for n in names if sol[n] is not None))
        for sol in res
    )
    j = json.loads(m.query(q, return_format="json"))
    got = Counter(
        tuple(sorted((n, _norm(_term(b))) for n, b in row.items()))
        for row in j["results"]["bindings"]
    )
    assert got == expected
