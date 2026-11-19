"""MINUS and FILTER (NOT) EXISTS after a pattern without solutions.

A variable of a pattern without solutions can be typed as never bound, an untyped column, and the
anti and semi joins of MINUS and EXISTS compared it with the typed column of the other side:
"datatypes of join keys don't match - `lim`: bool on left does not match `lim`: u32 on right". The
portal's Grid › Served power asked for the apparent power limits of a grid without any, with
FILTER NOT EXISTS on their limit type. Each query is compared with pyoxigraph.
"""

import json
from collections import Counter

import pyoxigraph
import pytest

from maplib import Model

DATA = """
<urn:l1> <urn:type> <urn:A> .
<urn:l1> <urn:lt> <urn:t1> .
<urn:t1> <urn:dur> "5" .
<urn:l1> <urn:val> "3" .
<urn:l2> <urn:type> <urn:A> .
<urn:l2> <urn:val> "4" .
<urn:s1> <urn:eq> <urn:e1> .
<urn:l1> <urn:set> <urn:s1> .
"""

# urn:B has no instances, so ?lim is bound by nothing on the left
QUERIES = {
    "minus after an empty join with a union": """
        SELECT ?lim WHERE { ?lim <urn:type> <urn:B> . { ?lim <urn:val> ?v } UNION { ?lim <urn:nv> ?v }
          MINUS { ?lim <urn:lt> ?d } }""",
    "not exists with a path after an empty join with a union": """
        SELECT ?lim WHERE { ?lim <urn:type> <urn:B> . { ?lim <urn:val> ?v } UNION { ?lim <urn:nv> ?v }
          FILTER NOT EXISTS { ?lim <urn:lt>/<urn:dur> ?d } }""",
    "exists with a path after an empty join with a union": """
        SELECT ?lim WHERE { ?lim <urn:type> <urn:B> . { ?lim <urn:val> ?v } UNION { ?lim <urn:nv> ?v }
          FILTER EXISTS { ?lim <urn:lt>/<urn:dur> ?d } }""",
    "not exists with a filter inside": """
        SELECT ?lim WHERE { ?lim <urn:type> <urn:B> . { ?lim <urn:val> ?v } UNION { ?lim <urn:nv> ?v }
          FILTER NOT EXISTS { ?lim <urn:lt>/<urn:dur> ?d FILTER(?d != "0") } }""",
    "the served power limits, grouped": """
        SELECT ?eq (MIN(?v) AS ?limit) WHERE {
          ?set <urn:eq> ?eq . ?lim <urn:set> ?set ; <urn:type> <urn:B> .
          { ?lim <urn:val> ?v } UNION { ?lim <urn:nv> ?v }
          FILTER NOT EXISTS { ?lim <urn:lt>/<urn:dur> ?d }
        } GROUP BY ?eq""",
    "the same with solutions on the left": """
        SELECT ?lim WHERE { ?lim <urn:type> <urn:A> . { ?lim <urn:val> ?v } UNION { ?lim <urn:nv> ?v }
          FILTER NOT EXISTS { ?lim <urn:lt>/<urn:dur> ?d } }""",
    "minus with solutions on the left": """
        SELECT ?lim WHERE { ?lim <urn:type> <urn:A> . { ?lim <urn:val> ?v } UNION { ?lim <urn:nv> ?v }
          MINUS { ?lim <urn:lt> ?d } }""",
    "not exists on a join of an empty side": """
        SELECT ?lim ?v WHERE { { ?lim <urn:type> <urn:B> . { ?lim <urn:val> ?x } UNION { ?lim <urn:nv> ?x } }
          ?lim <urn:val> ?v FILTER NOT EXISTS { ?lim <urn:lt> ?d } }""",
}


XSD = "http://www.w3.org/2001/XMLSchema#"


def _term(t):
    """A term from maplib's SPARQL JSON results or from pyoxigraph, in one comparable form."""
    if isinstance(t, dict):
        kind, value, dt = t["type"], t["value"], t.get("datatype")
    elif isinstance(t, pyoxigraph.NamedNode):
        kind, value, dt = "uri", t.value, None
    else:
        kind, value, dt = "literal", t.value, t.datatype.value
    if kind == "uri":
        return ("uri", value)
    return ("literal", value, None if dt in (None, XSD + "string") else dt)


@pytest.mark.parametrize("name", sorted(QUERIES))
def test_like_oxigraph(name):
    m = Model()
    m.reads(DATA, format="ntriples")
    store = pyoxigraph.Store()
    store.load(DATA.encode(), pyoxigraph.RdfFormat.N_TRIPLES)
    res = store.query(QUERIES[name])
    names = [v.value for v in res.variables]
    expected = Counter(
        tuple(sorted((n, _term(sol[n])) for n in names if sol[n] is not None)) for sol in res
    )
    j = json.loads(m.query(QUERIES[name], return_format="json"))
    got = Counter(
        tuple(sorted((n, _term(b)) for n, b in row.items())) for row in j["results"]["bindings"]
    )
    assert got == expected
