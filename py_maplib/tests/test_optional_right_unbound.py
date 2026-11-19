"""OPTIONAL where the right side may leave a variable it shares with the left side unbound (a
UNION branch, VALUES with UNDEF, a nested OPTIONAL). Such a right solution is compatible with
every left solution (SPARQL 1.1, 18.5 LeftJoin), and a left solution is kept unmatched only when
no right solution is compatible with it. Compared with Oxigraph as multisets of rows."""

import json
from collections import Counter

import pyoxigraph as ox
import pytest
from maplib import Model

PFX = "PREFIX : <http://ex.org/>\n"

DATA = """<http://ex.org/s1> <http://ex.org/a> <http://ex.org/n3> .
<http://ex.org/s2> <http://ex.org/a> <http://ex.org/n3> .
<http://ex.org/s3> <http://ex.org/a> <http://ex.org/n4> .
<http://ex.org/s1> <http://ex.org/b> <http://ex.org/y1> .
<http://ex.org/s2> <http://ex.org/b> <http://ex.org/y3> .
<http://ex.org/z1> <http://ex.org/h> <http://ex.org/y2> .
<http://ex.org/z2> <http://ex.org/h> <http://ex.org/y1> .
<http://ex.org/y1> <http://ex.org/c> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/y2> <http://ex.org/c> "x" .
"""

REPRO = """<http://ex.org/s1> <http://ex.org/a> <http://ex.org/n3> .
<http://ex.org/s1> <http://ex.org/b> <http://ex.org/y1> .
<http://ex.org/z1> <http://ex.org/h> <http://ex.org/y2> .
"""


def term(t):
    if t is None:
        return None
    if isinstance(t, ox.NamedNode):
        return t.value
    if isinstance(t, ox.BlankNode):
        return "_"
    return (t.value, t.language or t.datatype.value)


def json_term(b):
    if b is None:
        return None
    if b["type"] == "uri":
        return b["value"]
    if b["type"] == "bnode":
        return "_"
    return (
        b["value"],
        b.get("xml:lang") or b.get("datatype", "http://www.w3.org/2001/XMLSchema#string"),
    )


def engines(data):
    store = ox.Store()
    store.load(data.encode(), ox.RdfFormat.N_TRIPLES)
    m = Model()
    m.reads(data, format="ntriples")
    return store, m


@pytest.fixture(scope="module")
def data_engines():
    return engines(DATA)


def check(store, m, q):
    res = store.query(PFX + q)
    ev = [v.value for v in res.variables]
    expected = Counter(tuple(term(r[v]) for v in ev) for r in res)
    d = json.loads(m.query(PFX + q, return_json=True))
    av = d["head"]["vars"]
    assert sorted(av) == sorted(ev)
    actual = Counter(
        tuple(json_term(b.get(v)) for v in ev) for b in d["results"]["bindings"]
    )
    assert actual == expected
    return expected


def test_repro_optional_over_union():
    store, m = engines(REPRO)
    rows = check(
        store, m, "SELECT ?s ?y ?z WHERE { ?s :a :n3 OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } }"
    )
    assert sum(rows.values()) == 2


QUERIES = [
    # OPTIONAL over UNION, the shared variable bound in one branch only.
    "SELECT * WHERE { ?s :a :n3 OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?o :none ?y } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?y :c ?o } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :none ?y } UNION { ?z :none ?y } } }",
    "SELECT * WHERE { ?s :a :n3 OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } ?y :c ?w } }",
    "SELECT * WHERE { ?s :a :n3 OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } FILTER(BOUND(?y)) }",
    "SELECT ?s (COUNT(?y) AS ?n) WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } } GROUP BY ?s",
    # With an expression on the merged solutions.
    "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } FILTER(?o = :n3) } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } FILTER(?y != :y1) } }",
    # VALUES with UNDEF on the right side.
    "SELECT * WHERE { ?s :a ?o OPTIONAL { VALUES ?s { :s1 UNDEF } ?s2 :b ?y } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { VALUES (?s ?y) { (:s1 UNDEF) (UNDEF :y2) (:s3 :y9) } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { VALUES (?s ?o) { (:s1 UNDEF) (UNDEF :n4) } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { ?z :h ?y VALUES ?s { UNDEF } } }",
    # Nested OPTIONALs, the inner one binding a variable of the outer left side.
    "SELECT * WHERE { ?s :a ?o OPTIONAL { ?z :h ?y OPTIONAL { ?s :b ?y } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { ?y :c ?w OPTIONAL { ?s :b ?y } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { ?y :c ?w OPTIONAL { ?s :b ?y OPTIONAL { ?s :a ?o } } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { ?s :b ?y OPTIONAL { ?y :c ?w } } }",
    "SELECT * WHERE { ?s :a ?o OPTIONAL { ?s :b ?y } OPTIONAL { ?z :h ?y OPTIONAL { ?s :b ?y } } }",
    # Left side with unbound variables too.
    "SELECT * WHERE { ?s :a ?o OPTIONAL { ?s :b ?y } OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } }",
    "SELECT * WHERE { { ?s :a ?o } UNION { ?z :h ?y } OPTIONAL { { ?s :b ?y } UNION { ?z :h ?y } } }",
]


@pytest.mark.parametrize("q", QUERIES)
def test_optional_right_unbound(data_engines, q):
    store, m = data_engines
    check(store, m, q)
