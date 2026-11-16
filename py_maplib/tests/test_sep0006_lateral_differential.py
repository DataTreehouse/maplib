"""
Differential tests of LATERAL (SEP-0006) where no reference suite has expected results.

The vendored Jena and Oxigraph suites (``test_sep0006_lateral.py``) are the only published LATERAL
tests with expected results: SEP-0006 and Jena's documentation give example queries without data
or results, and the W3C SPARQL 1.2 tests have none. These tests cover the rest: the SEP-0006
examples, unbound left variables, OPTIONAL, MINUS, UNION, GRAPH, EXISTS, VALUES and nested
LATERAL inside and around LATERAL, and the rules on rebinding left variables. Every query is
checked
- against Oxigraph (pyoxigraph), an independent implementation of SEP-0006, and
- where it can be written without LATERAL, against that SPARQL 1.1 query evaluated by maplib,
with both evaluation strategies of maplib's LATERAL (set-based, and the right side evaluated once
per left solution with ``MAPLIB_LATERAL_PER_ROW``).
"""

import json
from collections import Counter
from decimal import Decimal

import pytest

from maplib import Model

ox = pytest.importorskip("pyoxigraph")

XSD = "http://www.w3.org/2001/XMLSchema#"
NUMERIC = {XSD + t for t in ("integer", "decimal", "double", "float", "long", "int")}

PREFIX = "PREFIX ex: <http://example.org/>\n"

DATA = """
@prefix ex: <http://example.org/> .
ex:a a ex:T ; ex:label "A" ; ex:v 1, 5, 3 ; ex:min 2 ; ex:knows ex:b .
ex:b a ex:T ; ex:v 10, 2 ; ex:min 8 ; ex:knows ex:c, ex:a .
ex:c a ex:T ; ex:label "C"@en, "Cee" ; ex:v 7 ; ex:min 0 .
ex:d a ex:U ; ex:label "D" ; ex:min 5 .
ex:e ex:v 4 .
ex:g1 { ex:a ex:v 100 . ex:b ex:tag "x" }
"""

# (id, LATERAL query, equivalent SPARQL 1.1 query or None)
CASES = [
    (
        "top_n_per_left_solution",
        """SELECT ?s ?v WHERE { ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v) LIMIT 2 } }""",
        """SELECT ?s ?v WHERE {
            { SELECT ?s ?v (COUNT(?w) AS ?k) WHERE {
                ?s a ex:T ; ex:v ?v OPTIONAL { ?s ex:v ?w FILTER(?w > ?v) } } GROUP BY ?s ?v }
            FILTER(?k < 2) }""",
    ),
    (
        "offset_per_left_solution",
        """SELECT ?s ?v WHERE { ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY ?v LIMIT 1 OFFSET 1 } }""",
        """SELECT ?s ?v WHERE {
            { SELECT ?s ?v (COUNT(?w) AS ?k) WHERE {
                ?s a ex:T ; ex:v ?v OPTIONAL { ?s ex:v ?w FILTER(?w < ?v) } } GROUP BY ?s ?v }
            FILTER(?k = 1) }""",
    ),
    (
        "sep_example_exactly_one_label",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { SELECT * WHERE { ?s ex:label ?label } ORDER BY STR(?label) LIMIT 1 } }""",
        None,
    ),
    (
        "sep_example_zero_or_one_label",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { OPTIONAL { SELECT * WHERE { ?s ex:label ?label } ORDER BY STR(?label) LIMIT 1 } } }""",
        None,
    ),
    (
        "sep_example_hidden_variable_not_substituted",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { SELECT ?label WHERE { ?s ex:label ?label } ORDER BY STR(?label) LIMIT 1 } }""",
        """SELECT * WHERE { ?s a ex:T
            { SELECT ?label WHERE { ?z ex:label ?label } ORDER BY STR(?label) LIMIT 1 } }""",
    ),
    (
        "aggregate_grouped_by_left_variable",
        """SELECT ?s ?n ?sum WHERE { ?s a ex:T
            LATERAL { SELECT ?s (COUNT(?v) AS ?n) (SUM(?v) AS ?sum) WHERE { ?s ex:v ?v } GROUP BY ?s } }""",
        """SELECT ?s ?n ?sum WHERE { ?s a ex:T
            { SELECT ?s (COUNT(?v) AS ?n) (SUM(?v) AS ?sum) WHERE { ?s ex:v ?v } GROUP BY ?s } }""",
    ),
    (
        "aggregate_with_correlated_filter",
        """SELECT ?s ?n WHERE { ?s ex:min ?min
            LATERAL { SELECT ?min (COUNT(?x) AS ?n) WHERE { ?x ex:v ?v FILTER(?v > ?min) } GROUP BY ?min } }""",
        """SELECT ?s ?n WHERE {
            { SELECT ?s (COUNT(?x) AS ?n) WHERE { ?s ex:min ?min . ?x ex:v ?v FILTER(?v > ?min) } GROUP BY ?s } }""",
    ),
    (
        "aggregate_hidden_variable_not_substituted",
        """SELECT ?s ?n WHERE { ?s a ex:T
            LATERAL { SELECT (COUNT(*) AS ?n) WHERE { ?s ex:v ?v } } }""",
        """SELECT ?s ?n WHERE { ?s a ex:T { SELECT (COUNT(*) AS ?n) WHERE { ?z ex:v ?v } } }""",
    ),
    (
        "bgp",
        "SELECT * WHERE { ?s a ex:T LATERAL { ?s ex:knows ?o } }",
        "SELECT * WHERE { ?s a ex:T . ?s ex:knows ?o }",
    ),
    (
        "empty_group",
        "SELECT * WHERE { ?s a ex:T LATERAL { } }",
        "SELECT * WHERE { ?s a ex:T }",
    ),
    (
        "empty_left_side",
        "SELECT * WHERE { ?s ex:nothing ?x LATERAL { ?s ex:v ?v } }",
        "SELECT * WHERE { ?s ex:nothing ?x . ?s ex:v ?v }",
    ),
    (
        "left_solutions_with_duplicates",
        """SELECT ?s ?l WHERE { ?s ex:v ?v
            LATERAL { SELECT ?s ?l WHERE { ?s ex:label ?l } ORDER BY STR(?l) LIMIT 1 } }""",
        None,
    ),
    (
        "optional_inside",
        "SELECT * WHERE { ?s a ex:T LATERAL { OPTIONAL { ?s ex:label ?l } } }",
        "SELECT * WHERE { ?s a ex:T OPTIONAL { ?s ex:label ?l } }",
    ),
    (
        "optional_with_filter_on_left_variable_inside",
        """SELECT * WHERE { ?s ex:min ?min
            LATERAL { OPTIONAL { ?s ex:v ?v FILTER(?v > ?min) } } }""",
        """SELECT * WHERE { ?s ex:min ?min OPTIONAL { ?s ex:v ?v FILTER(?v > ?min) } }""",
    ),
    (
        "union_inside",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { { ?s ex:label ?x } UNION { ?s ex:knows ?x } } }""",
        """SELECT * WHERE { ?s a ex:T { ?s ex:label ?x } UNION { ?s a ex:T . ?s ex:knows ?x } }""",
    ),
    (
        "minus_inside_sharing_variables",
        "SELECT * WHERE { ?s a ex:T LATERAL { ?o a ex:T MINUS { ?s ex:knows ?o } } }",
        "SELECT * WHERE { ?s a ex:T . ?o a ex:T FILTER NOT EXISTS { ?s ex:knows ?o } }",
    ),
    (
        # ?s is substituted in the right side of MINUS too, so MINUS { <s> ex:knows ?x } shares
        # no variable with ?o and removes nothing.
        "minus_inside_left_variable_only_in_minus",
        "SELECT * WHERE { ?s a ex:T LATERAL { ?o a ex:T MINUS { ?s ex:knows ?x } } }",
        None,
    ),
    (
        "filter_not_exists_inside",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { ?s ex:v ?v FILTER NOT EXISTS { ?s ex:min ?m FILTER(?m > ?v) } } }""",
        """SELECT * WHERE { ?s a ex:T ; ex:v ?v FILTER NOT EXISTS { ?s ex:min ?m FILTER(?m > ?v) } }""",
    ),
    (
        "bind_using_left_variable",
        "SELECT * WHERE { ?s ex:min ?min LATERAL { BIND(?min + 1 AS ?m1) } }",
        "SELECT * WHERE { ?s ex:min ?min BIND(?min + 1 AS ?m1) }",
    ),
    (
        "distinct_per_left_solution",
        """SELECT ?s ?t WHERE { ?s a ex:T
            LATERAL { SELECT DISTINCT ?s ?t WHERE { ?s ex:knows ?k . ?k a ?t } } }""",
        """SELECT DISTINCT ?s ?t WHERE { ?s a ex:T ; ex:knows ?k . ?k a ?t }""",
    ),
    (
        "graph_inside",
        "SELECT * WHERE { ?s a ex:T LATERAL { GRAPH ?g { ?s ?p ?o } } }",
        "SELECT * WHERE { ?s a ex:T GRAPH ?g { ?s ?p ?o } }",
    ),
    (
        "values_inside",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { VALUES ?p { ex:label ex:knows } ?s ?p ?o } }""",
        """SELECT * WHERE { ?s a ex:T VALUES ?p { ex:label ex:knows } ?s ?p ?o }""",
    ),
    (
        "values_on_left",
        """SELECT ?s ?n WHERE { VALUES ?s { ex:a ex:d ex:zz }
            LATERAL { SELECT ?s (COUNT(?v) AS ?n) WHERE { ?s ex:v ?v } GROUP BY ?s } }""",
        None,
    ),
    (
        # The unbound ?min of the first row is not substituted, so the FILTER is an error there.
        # The unbound ?s of the second row is not substituted, so it ranges over all subjects.
        "values_with_undef_on_left",
        """SELECT * WHERE { VALUES (?s ?min) { (ex:a UNDEF) (UNDEF 5) }
            LATERAL { SELECT ?s ?min ?v WHERE { ?s ex:v ?v FILTER(?v > ?min) } } }""",
        None,
    ),
    (
        "unbound_left_variable_from_optional",
        """SELECT * WHERE { ?s a ex:T OPTIONAL { ?s ex:knows ?k }
            LATERAL { SELECT ?k ?kv WHERE { ?k ex:v ?kv } ORDER BY DESC(?kv) LIMIT 1 } }""",
        None,
    ),
    (
        "unbound_left_variable_in_bgp",
        """SELECT * WHERE { ?s ex:min ?min OPTIONAL { ?s ex:label ?l }
            LATERAL { ?x ex:label ?l } }""",
        """SELECT * WHERE { ?s ex:min ?min OPTIONAL { ?s ex:label ?l } ?x ex:label ?l }""",
    ),
    (
        "nested_lateral",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { ?s ex:knows ?k
                LATERAL { SELECT ?k ?v WHERE { ?k ex:v ?v } ORDER BY DESC(?v) LIMIT 1 } } }""",
        None,
    ),
    (
        "nested_lateral_using_outer_variable",
        """SELECT * WHERE { ?s ex:min ?min
            LATERAL { ?s ex:knows ?k
                LATERAL { SELECT ?k ?min ?v WHERE { ?k ex:v ?v FILTER(?v > ?min) } ORDER BY ?v LIMIT 1 } } }""",
        None,
    ),
    (
        "chained_laterals",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { SELECT ?s ?k WHERE { ?s ex:knows ?k } ORDER BY ?k LIMIT 1 }
            LATERAL { SELECT ?k ?v WHERE { ?k ex:v ?v } ORDER BY ?v LIMIT 1 } }""",
        None,
    ),
    (
        "lateral_inside_optional",
        """SELECT * WHERE { ?s a ex:T
            OPTIONAL { ?s ex:knows ?k
                LATERAL { SELECT ?k ?v WHERE { ?k ex:v ?v } ORDER BY DESC(?v) LIMIT 1 } } }""",
        None,
    ),
    (
        "lateral_inside_union",
        """SELECT * WHERE {
            { ?s a ex:U } UNION
            { ?s a ex:T LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY ?v LIMIT 1 } } }""",
        None,
    ),
    (
        "lateral_inside_subselect",
        """SELECT ?s ?v WHERE { { SELECT ?s ?v WHERE { ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v) LIMIT 1 } } } }""",
        None,
    ),
    (
        "uncorrelated_with_limit",
        """SELECT * WHERE { ?s a ex:T
            LATERAL { SELECT ?x WHERE { ?x ex:min ?m } ORDER BY ?m LIMIT 1 } }""",
        """SELECT * WHERE { ?s a ex:T { SELECT ?x WHERE { ?x ex:min ?m } ORDER BY ?m LIMIT 1 } }""",
    ),
    (
        "outer_order_and_limit",
        """SELECT ?s ?v WHERE { ?s a ex:T
            LATERAL { SELECT ?s ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v) LIMIT 2 } }
            ORDER BY DESC(?v) LIMIT 3""",
        None,
    ),
]

# Cases that fail for reasons outside of LATERAL, as strict xfails.
KNOWN_FAILURES = {
    "unbound_left_variable_in_bgp": "maplib's joins do not treat unbound variables as compatible "
    "with bound ones, also without LATERAL (as the W3C tests bindings/values7 and "
    "negation/full-minuend)",
}


def cases(with_rewrite=False):
    return [
        pytest.param(
            c,
            id=c[0],
            marks=[pytest.mark.xfail(strict=True, reason=KNOWN_FAILURES[c[0]])]
            if c[0] in KNOWN_FAILURES
            else [],
        )
        for c in CASES
        if c[2] or not with_rewrite
    ]


# Rebinding a variable of the left side at the top level of the right side is illegal.
ILLEGAL = [
    ("sep_example_bind", "SELECT * WHERE { ?s ?p ?o LATERAL { BIND(123 AS ?o) } }"),
    ("values", "SELECT * WHERE { ?s ?p ?o LATERAL { VALUES ?o { 1 } } }"),
    (
        "subselect_expression",
        "SELECT * WHERE { ?s ?p ?o LATERAL { SELECT (1 AS ?o) WHERE { } } }",
    ),
]


def term(b):
    t = b["type"]
    if t == "uri":
        return ("uri", b["value"])
    if t == "bnode":
        return ("bnode", b["value"])
    dt = b.get("datatype")
    lang = b.get("xml:lang")
    if lang:
        return ("literal", b["value"], None, lang.lower())
    if dt in NUMERIC:
        return ("number", Decimal(b["value"]).normalize())
    if dt == XSD + "string":
        dt = None
    return ("literal", b["value"], dt, None)


def ox_term(t):
    if isinstance(t, ox.NamedNode):
        return {"type": "uri", "value": t.value}
    if isinstance(t, ox.BlankNode):
        return {"type": "bnode", "value": t.value}
    b = {"type": "literal", "value": t.value, "datatype": t.datatype.value}
    if t.language:
        b["xml:lang"] = t.language
    return b


def multiset(rows):
    return Counter(tuple(sorted(r.items())) for r in rows)


@pytest.fixture(scope="module")
def maplib_model():
    m = Model()
    m.reads(DATA, format="trig")
    return m


@pytest.fixture(scope="module")
def ox_store():
    s = ox.Store()
    s.load(DATA.encode(), ox.RdfFormat.TRIG)
    return s


@pytest.fixture(params=["set", "per_row"])
def strategy(request, monkeypatch):
    if request.param == "per_row":
        monkeypatch.setenv("MAPLIB_LATERAL_PER_ROW", "1")
    else:
        monkeypatch.delenv("MAPLIB_LATERAL_PER_ROW", raising=False)
    return request.param


def maplib_rows(m, q):
    j = json.loads(m.query(PREFIX + q, return_format="json"))
    return multiset(
        {v: term(b) for v, b in row.items()} for row in j["results"]["bindings"]
    )


def ox_rows(s, q):
    res = s.query(PREFIX + q)
    names = [v.value for v in res.variables]
    rows = []
    for sol in res:
        rows.append(
            {n: term(ox_term(sol[n])) for n in names if sol[n] is not None}
        )
    return multiset(rows)


@pytest.mark.parametrize("case", cases())
def test_same_as_oxigraph(maplib_model, ox_store, strategy, case):
    _, query, _ = case
    expected = ox_rows(ox_store, query)
    assert maplib_rows(maplib_model, query) == expected


@pytest.mark.parametrize("case", cases(with_rewrite=True))
def test_same_as_sparql11_rewrite(maplib_model, ox_store, strategy, case):
    _, query, rewrite = case
    expected = maplib_rows(maplib_model, rewrite)
    # The rewrite itself is checked against Oxigraph, so a wrong rewrite is caught too.
    assert expected == ox_rows(ox_store, rewrite)
    assert maplib_rows(maplib_model, query) == expected


@pytest.mark.parametrize("case", ILLEGAL, ids=[c[0] for c in ILLEGAL])
def test_rebinding_left_variable_is_rejected(maplib_model, ox_store, case):
    _, query = case
    with pytest.raises(SyntaxError):
        ox_store.query(PREFIX + query)
    with pytest.raises(Exception):
        maplib_model.query(PREFIX + query)
