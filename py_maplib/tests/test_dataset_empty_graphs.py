"""Queries over a dataset of several graphs (several FROM clauses) where some graphs have no triples
for a pattern.

The default graph of such a dataset is the merge of the FROM graphs, and a triple pattern is read
from each of them. A graph without matching triples gave a result with untyped columns, and merging
it with the other graphs' results typed every variable as "its type, or unbound". maplib EE's
virtualized time series could then not join their external ids ("datatypes of join keys don't
match"). The graphs without matching triples are now left out of the merge. The answers were
right before too (the final results hide the extra type), so these compare with pyoxigraph to
guard the merge; py_chrontext/tests/test_virtualized_several_graphs.py in maplib EE tests the join.
"""

import json
from collections import Counter

import pyoxigraph
import pytest

from maplib import Model

G1 = """
<urn:a> <urn:p> "1" .
<urn:a> <urn:name> "A" .
<urn:b> <urn:p> "2" .
<urn:b> <urn:q> <urn:c> .
"""
G2 = """
<urn:x> <urn:other> <urn:y> .
<urn:a> <urn:kind> <urn:K> .
"""
G3 = """
<urn:z> <urn:other> <urn:w> .
"""

FROM2 = "FROM <urn:g1> FROM <urn:g2>"
FROM3 = "FROM <urn:g1> FROM <urn:g2> FROM <urn:g3>"
QUERIES = {
    "a pattern only one graph has": f"SELECT ?s ?o {FROM2} WHERE {{ ?s <urn:p> ?o }}",
    "three graphs": f"SELECT ?s ?o {FROM3} WHERE {{ ?s <urn:p> ?o }}",
    "the empty graph first": "SELECT ?s ?o FROM <urn:g3> FROM <urn:g1> WHERE { ?s <urn:p> ?o }",
    "no graph has it": f"SELECT ?s ?o {FROM3} WHERE {{ ?s <urn:none> ?o }}",
    "a join across graphs": f"SELECT ?s ?o ?k {FROM2} WHERE {{ ?s <urn:p> ?o . ?s <urn:kind> ?k }}",
    "an optional from the other graph": f"SELECT ?s ?o ?k {FROM2} WHERE {{ ?s <urn:p> ?o OPTIONAL {{ ?s <urn:kind> ?k }} }}",
    "a join on a literal": f'SELECT ?s ?t {FROM2} WHERE {{ ?s <urn:p> ?o . ?t <urn:p> ?o2 FILTER(?o = "1") }}',
    "minus": f"SELECT ?s {FROM2} WHERE {{ ?s <urn:p> ?o MINUS {{ ?s <urn:kind> ?k }} }}",
    "not exists": f"SELECT ?s {FROM2} WHERE {{ ?s <urn:p> ?o FILTER NOT EXISTS {{ ?s <urn:kind> ?k }} }}",
    "values join": f'SELECT ?s {FROM2} WHERE {{ VALUES ?o {{ "1" "2" }} ?s <urn:p> ?o }}',
    "group by": f"SELECT ?s (COUNT(?o) AS ?n) {FROM3} WHERE {{ ?s <urn:p> ?o }} GROUP BY ?s",
    "a variable predicate": f"SELECT ?s ?p ?o {FROM2} WHERE {{ ?s ?p ?o FILTER(?s = <urn:b>) }}",
    "a path": f"SELECT ?s ?c {FROM2} WHERE {{ ?s <urn:q>/<urn:none>? ?c }}",
}


def _term(t):
    if isinstance(t, dict):
        kind, value, dt = t["type"], t["value"], t.get("datatype")
    elif isinstance(t, pyoxigraph.NamedNode):
        kind, value, dt = "uri", t.value, None
    else:
        kind, value, dt = "literal", t.value, t.datatype.value
    if kind == "uri":
        return ("uri", value)
    return ("literal", value, None if dt in (None, "http://www.w3.org/2001/XMLSchema#string") else dt)


@pytest.fixture(scope="module")
def models():
    m = Model()
    store = pyoxigraph.Store()
    for g, data in [("urn:g1", G1), ("urn:g2", G2), ("urn:g3", G3)]:
        m.reads(data, format="ntriples", graph=g)
        store.load(data.encode(), pyoxigraph.RdfFormat.N_TRIPLES, to_graph=pyoxigraph.NamedNode(g))
    return m, store


@pytest.mark.parametrize("name", sorted(QUERIES))
def test_like_oxigraph(models, name):
    m, store = models
    res = store.query(QUERIES[name])
    names = [v.value for v in res.variables]
    expected = Counter(tuple(sorted((n, _term(s[n])) for n in names if s[n] is not None)) for s in res)
    j = json.loads(m.query(QUERIES[name], return_format="json"))
    got = Counter(tuple(sorted((n, _term(b)) for n, b in r.items())) for r in j["results"]["bindings"])
    assert got == expected

