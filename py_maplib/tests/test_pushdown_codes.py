"""The values of a small set of solutions are pushed down into the next triple pattern as
dictionary codes (IRIs, blank nodes and string literals), and as terms for the other types and
for the constants of a query. These tests compare with pyoxigraph for patterns whose pushed
down values are IRIs, blank nodes, of several types in one column (IRIs and literals), language
tagged and typed literals, in named graphs, with triples in the write buffer and after deletes,
in each storage mode.

The tables of the patterns the values are pushed into have more than 16,384 triples, so that
they are not simply joined within the query plan."""

import json
from collections import Counter

import pyoxigraph
import pytest

from maplib import Model

from .disk import disk_params, storage_kwargs

EX = "http://example.net/pc#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD = "http://www.w3.org/2001/XMLSchema#"
N = 24_000
G1 = "http://example.net/g1"
G2 = "http://example.net/g2"

PREFIX = f"PREFIX ex: <{EX}>\nPREFIX xsd: <{XSD}>\n"


def s(i):
    return f"<{EX}s{i}>"


def value_of(i):
    """Values of many types in one column of ex:val."""
    k = i % 6
    if k == 0:
        return f"<{EX}o{i % 300}>"
    if k == 1:
        return f'"v{i % 400}"'
    if k == 2:
        return f'"w{i % 200}"@en'
    if k == 3:
        return f'"{i % 500}"^^<{XSD}integer>'
    if k == 4:
        return f'"c{i % 100}"^^<{EX}code>'
    return f"_:vb{i % 50}"


def data():
    lines = []
    for i in range(N):
        lines.append(f"{s(i)} <{EX}val> {value_of(i)} .")
        lines.append(f"{s(i)} <{EX}link> {s((i * 7 + 3) % N)} .")
        if i % 60 == 0:
            lines.append(f"{s(i)} <{RDF_TYPE}> <{EX}Sel> .")
        if i % 24 == 0:
            lines.append(f"{s(i)} <{RDF_TYPE}> <{EX}Medium> .")
        if i % 97 == 0:
            # The values to push: an IRI, a string, a tagged string, an integer, a typed
            # literal or a blank node, from the same column.
            lines.append(f"{s(i)} <{EX}pick> {value_of(i * 5 + 1)} .")
            lines.append(f"{s(i)} <{EX}pick> {value_of(i + 3)} .")
    for b in range(40):
        # Blank node subjects among the selected and the linked.
        lines.append(f"_:sb{b} <{RDF_TYPE}> <{EX}Sel> .")
        lines.append(f"_:sb{b} <{EX}val> {value_of(b * 11)} .")
        lines.append(f"{s(b * 13)} <{EX}link> _:sb{b} .")
    for k in range(300):
        # The IRI values are subjects too.
        lines.append(f"<{EX}o{k}> <{EX}link> {s(k * 17)} .")
    for p in ["val", "link"]:
        lines.append(f"<{EX}{p}> <{RDF_TYPE}> <{EX}Prop> .")
    return "\n".join(lines) + "\n"


def graph_data(g):
    lines = []
    off = 1 if g == G1 else 2
    for i in range(0, N, 3):
        lines.append(f"{s(i + off)} <{EX}gval> {value_of(i + off)} .")
        if i % 90 == 0:
            lines.append(f"{s(i + off)} <{RDF_TYPE}> <{EX}GSel> .")
    return "\n".join(lines) + "\n"


QUERIES = {
    # IRI subjects pushed into a subject lookup, and into an object lookup.
    "iri_subjects": "SELECT ?s ?o WHERE { ?s a ex:Sel . ?s ex:val ?o }",
    "iri_objects": "SELECT ?x ?s WHERE { ?s a ex:Sel . ?x ex:link ?s }",
    # Focus variables of a medium set of solutions (more than 500 rows).
    "medium_subjects": "SELECT ?s ?o WHERE { ?s a ex:Medium . ?s ex:val ?o }",
    "medium_objects": "SELECT (COUNT(*) AS ?n) WHERE { ?s a ex:Medium . ?x ex:link ?s }",
    # Values of all types, from one column, as objects of a column of all types.
    "mixed_objects": "SELECT ?x ?v WHERE { ?s ex:pick ?v . ?x ex:val ?v }",
    "mixed_as_subjects": "SELECT ?v ?y WHERE { ?s ex:pick ?v . ?v ex:link ?y }",
    "lang_and_typed": """SELECT ?x ?v WHERE { ?s ex:pick ?v .
        FILTER(isLiteral(?v) && (lang(?v) != "" || datatype(?v) != xsd:string))
        ?x ex:val ?v }""",
    "blank_objects": "SELECT ?s ?x WHERE { ?x ex:link ?s . ?s a ex:Sel . FILTER(isBlank(?s)) }",
    "blank_values": "SELECT ?x WHERE { ?b a ex:Sel . ?b ex:val ?v . FILTER(isBlank(?v)) ?x ex:val ?v }",
    # Constants of the query together with pushed down codes.
    "values_and_codes": """SELECT ?s ?o WHERE {
        VALUES ?s { ex:s0 ex:s60 ex:s61 ex:nothere } ?s a ex:Sel . ?s ex:val ?o }""",
    "filter_in_and_codes": """SELECT ?s ?o WHERE { ?s a ex:Sel .
        FILTER(?s IN (ex:s120, ex:s180, ex:s7)) ?s ex:val ?o }""",
    "constant_object": """SELECT ?s WHERE { ?s a ex:Sel . ?s ex:val ?o .
        FILTER(?o = "v0" || ?o = ex:o0) }""",
    # A variable predicate with pushed down subjects, and pushed down predicates.
    "variable_predicate": "SELECT ?s ?p ?o WHERE { ?s a ex:Sel . ?s ?p ?o }",
    "pushed_predicates": "SELECT ?p (COUNT(*) AS ?n) WHERE { ?p a ex:Prop . ?s ?p ?o } GROUP BY ?p",
    # A chain, EXISTS and a path from pushed down values.
    "chain": "SELECT ?s ?y ?o WHERE { ?s a ex:Sel . ?s ex:link ?y . ?y ex:val ?o }",
    "exists": """SELECT ?s WHERE { ?s a ex:Sel .
        FILTER EXISTS { ?s ex:link ?y . ?y ex:val ?o . FILTER(isIRI(?o)) } }""",
    "path": "SELECT ?s ?z WHERE { ?s a ex:Sel . FILTER(isIRI(?s)) ?s ex:link/ex:link ?z }",
    "group_count": "SELECT ?o (COUNT(?s) AS ?n) WHERE { ?s a ex:Sel . ?s ex:val ?o } GROUP BY ?o",
    # Named graphs.
    "named_graph": f"""SELECT ?g ?s ?v WHERE {{ GRAPH ?g {{ ?s a ex:GSel . ?s ex:gval ?v }} }}""",
    "graph_to_default": f"""SELECT ?s ?x WHERE {{ GRAPH <{G1}> {{ ?s a ex:GSel . ?s ex:gval ?v }}
        ?x ex:val ?v }}""",
    "from_graphs": f"""SELECT ?s ?v FROM <{G1}> FROM <{G2}> WHERE {{ ?s a ex:GSel . ?s ex:gval ?v }}""",
}

# Small updates go to the write buffer.
INSERTS = f"""PREFIX ex: <{EX}>
INSERT DATA {{
    ex:s1 a ex:Sel . ex:s1 ex:val "v1" .
    ex:new1 a ex:Sel . ex:new1 ex:val ex:o5 . ex:new1 ex:link ex:s60 .
    ex:s5 ex:link ex:new1 .
    ex:s60 ex:val "fresh"@en . ex:s60 ex:val "77"^^<{XSD}integer> .
    _:nb a ex:Sel . _:nb ex:val "v1" .
    ex:s0 ex:pick "fresh"@en . ex:s0 ex:pick ex:o5 .
    GRAPH <{G1}> {{ ex:s1 a ex:GSel . ex:s1 ex:gval "v1" }}
}}"""
DELETES = f"""PREFIX ex: <{EX}>
DELETE DATA {{
    ex:s0 ex:val ex:o0 .
    ex:s120 a ex:Sel .
    ex:s180 ex:link ex:s1263 .
    ex:s7 ex:link ex:s52 .
    GRAPH <{G1}> {{ ex:s91 a ex:GSel }}
}}"""


def _term(t):
    if isinstance(t, dict):
        kind = t["type"]
        if kind == "uri":
            return ("uri", t["value"])
        if kind == "bnode":
            return ("bnode",)
        dt = t.get("datatype")
        lang = t.get("xml:lang")
        if lang is not None or dt == XSD + "string":
            dt = None
        return ("literal", t["value"], dt, lang)
    if isinstance(t, pyoxigraph.NamedNode):
        return ("uri", t.value)
    if isinstance(t, pyoxigraph.BlankNode):
        return ("bnode",)
    dt = t.datatype.value
    lang = t.language
    if lang is not None or dt == XSD + "string":
        dt = None
    return ("literal", t.value, dt, lang)


@pytest.fixture(scope="module", params=disk_params())
def storage(request):
    return request.param


def build(storage, state):
    m = Model(**storage_kwargs(storage))
    m.reads(data(), format="ntriples")
    for g in [G1, G2]:
        m.reads(graph_data(g), format="ntriples", graph=g)
    store = pyoxigraph.Store()
    store.load(data().encode(), pyoxigraph.RdfFormat.N_TRIPLES)
    for g in [G1, G2]:
        store.load(
            graph_data(g).encode(),
            pyoxigraph.RdfFormat.N_TRIPLES,
            to_graph=pyoxigraph.NamedNode(g),
        )
    if state in ("buffer", "delete"):
        m.update(INSERTS)
        store.update(INSERTS)
    if state == "delete":
        m.update(DELETES)
        store.update(DELETES)
    return m, store


@pytest.fixture(scope="module", params=["fresh", "buffer", "delete"])
def stores(request, storage):
    return build(storage, request.param)


@pytest.mark.parametrize("name", list(QUERIES))
def test_like_oxigraph(stores, name):
    m, store = stores
    q = PREFIX + QUERIES[name]
    res = store.query(q)
    names = [v.value for v in res.variables]
    expected = Counter(
        tuple(sorted((n, _term(sol[n])) for n in names if sol[n] is not None))
        for sol in res
    )
    j = json.loads(m.query(q, return_format="json"))
    got = Counter(
        tuple(sorted((n, _term(b)) for n, b in row.items()))
        for row in j["results"]["bindings"]
    )
    assert sum(expected.values()) > 0, "the query should have solutions"
    assert got == expected
