"""Casts of literals whose values are dictionary-encoded (xsd:string and datatypes maplib does
not know, such as BSBM's "5389.96"^^bsbm:USD).

Only xsd:string and durations were decoded before they were cast, so a literal of another such
datatype was cast from its dictionary code: xsd:string("5389.96"^^bsbm:USD) gave "1795", and
BSBM BI Q4 and Q8, which average xsd:float(xsd:string(?price)), averaged codes. A
language-tagged string was cast from its value's code too: xsd:string("x"@en) gave "1". A chain of casts
of such a variable is now evaluated once per distinct value and gathered back by code.

The answers are compared with pyoxigraph.
"""

import json
from collections import Counter
from decimal import Decimal

import pyoxigraph
import pytest

from maplib import Model

EX = "http://example.net/"
USD = EX + "USD"
DATA = f"""
<{EX}o1> <{EX}price> "5389.96"^^<{USD}> .
<{EX}o2> <{EX}price> "+303.88"^^<{USD}> .
<{EX}o3> <{EX}price> "abc"^^<{USD}> .
<{EX}o4> <{EX}price> "12"^^<{USD}> .
<{EX}o5> <{EX}price> "5389.96"^^<{USD}> .
<{EX}o6> <{EX}price> "-1e3"^^<{USD}> .
<{EX}o1> <{EX}name> "12.5" .
<{EX}o2> <{EX}name> "true" .
<{EX}o3> <{EX}name> "x"@en .
<{EX}o4> <{EX}name> "12"@en .
<{EX}o5> <{EX}name> "true"@no .
<{EX}o6> <{EX}name> "2024-01-02"@en .
<{EX}o7> <{EX}name> "P1D"@en .
<{EX}o1> <{EX}group> <{EX}g1> .
<{EX}o2> <{EX}group> <{EX}g1> .
<{EX}o4> <{EX}group> <{EX}g2> .
<{EX}o5> <{EX}group> <{EX}g2> .
"""

QUERIES = {
    "string": "SELECT ?o (xsd:string(?p) AS ?x) WHERE { ?o ex:price ?p }",
    "str": "SELECT ?o (STR(?p) AS ?x) WHERE { ?o ex:price ?p }",
    "float_of_string": "SELECT ?o (xsd:float(xsd:string(?p)) AS ?x) WHERE { ?o ex:price ?p }",
    "double_of_string": "SELECT ?o (xsd:double(xsd:string(?p)) AS ?x) WHERE { ?o ex:price ?p }",
    "decimal_of_string": "SELECT ?o (xsd:decimal(xsd:string(?p)) AS ?x) WHERE { ?o ex:price ?p }",
    "integer_of_string": "SELECT ?o (xsd:integer(xsd:string(?p)) AS ?x) WHERE { ?o ex:price ?p }",
    "boolean_of_string": "SELECT ?o (xsd:boolean(xsd:string(?n)) AS ?x) WHERE { ?o ex:name ?n FILTER(lang(?n) = '') }",
    # A language-tagged string casts to xsd:string, its lexical form, and to nothing else.
    "string_of_lang_string": "SELECT ?o (xsd:string(?n) AS ?x) WHERE { ?o ex:name ?n FILTER(lang(?n) != '') }",
    "string_of_mixed": "SELECT ?o (xsd:string(?n) AS ?x) WHERE { ?o ex:name ?n }",
    "integer_of_lang_string": "SELECT ?o (xsd:integer(?n) AS ?x) WHERE { ?o ex:name ?n FILTER(lang(?n) != '') }",
    "boolean_of_mixed": "SELECT ?o (xsd:boolean(?n) AS ?x) WHERE { ?o ex:name ?n }",
    "double_of_mixed": "SELECT ?o (xsd:double(?n) AS ?x) WHERE { ?o ex:name ?n }",
    "date_of_lang_string": "SELECT ?o (xsd:date(?n) AS ?x) WHERE { ?o ex:name ?n FILTER(lang(?n) != '') }",
    "datetime_of_lang_string": "SELECT ?o (xsd:dateTime(?n) AS ?x) WHERE { ?o ex:name ?n FILTER(lang(?n) != '') }",
    "duration_of_lang_string": "SELECT ?o (xsd:duration(?n) AS ?x) WHERE { ?o ex:name ?n FILTER(lang(?n) != '') }",
    "boolean_of_string_of_lang": "SELECT ?o (xsd:boolean(xsd:string(?n)) AS ?x) WHERE { ?o ex:name ?n }",
    "integer_of_string_of_lang": "SELECT ?o (xsd:integer(xsd:string(?n)) AS ?x) WHERE { ?o ex:name ?n FILTER(lang(?n) != '') }",
    "double_of_plain_string": "SELECT ?o (xsd:double(?n) AS ?x) WHERE { ?o ex:name ?n FILTER(!isLiteral(?n) || lang(?n) = '') }",
    "avg_by_group": "SELECT ?g (AVG(xsd:float(xsd:string(?p))) AS ?x) WHERE { ?o ex:group ?g ; ex:price ?p } GROUP BY ?g",
    "filter_on_cast": "SELECT ?o WHERE { ?o ex:price ?p FILTER(xsd:double(xsd:string(?p)) > 100) }",
}

XSD = "http://www.w3.org/2001/XMLSchema#"
NUMERIC = {XSD + t for t in ("integer", "decimal", "double", "float")}


def _term(t):
    if isinstance(t, dict):
        kind, value, dt, lang = t["type"], t["value"], t.get("datatype"), t.get("xml:lang")
    elif isinstance(t, pyoxigraph.NamedNode):
        kind, value, dt, lang = "uri", t.value, None, None
    else:
        kind, value, dt, lang = "literal", t.value, t.datatype.value, t.language
    if kind == "uri":
        return ("uri", value)
    if lang:
        return ("literal", value, lang.lower())
    if dt in NUMERIC:
        # xsd:float is compared at its own precision
        f = float(value)
        return ("number", round(f, 2) if dt == XSD + "float" else Decimal(value).normalize())
    if dt == XSD + "boolean":
        return ("boolean", value in ("true", "1"))
    return ("literal", value, None if dt in (None, XSD + "string") else dt)


@pytest.mark.parametrize("name", sorted(QUERIES))
def test_cast_matches_oxigraph(name):
    q = f"PREFIX ex: <{EX}> PREFIX xsd: <{XSD}>\n" + QUERIES[name]
    m = Model()
    m.reads(DATA, format="ntriples")
    store = pyoxigraph.Store()
    store.load(DATA.encode(), pyoxigraph.RdfFormat.N_TRIPLES)
    res = store.query(q)
    names = [v.value for v in res.variables]
    expected = Counter(
        tuple(sorted((n, _term(sol[n])) for n in names if sol[n] is not None)) for sol in res
    )
    j = json.loads(m.query(q, return_format="json"))
    got = Counter(
        tuple(sorted((n, _term(b)) for n, b in row.items())) for row in j["results"]["bindings"]
    )
    assert got == expected
