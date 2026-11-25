"""Deeply nested negations, FILTER(!(!(...))), alone and mixed with && and ||.

The parser tried every `!` as the SPARQL 1.2 double negation first, parsed its whole operand,
failed without SPARQL 1.2 and parsed the operand again, so parsing took time exponential in the
nesting depth: 43 levels took more than 20 s. SPARQL 1.2 (which maplib's parser is now built with)
allows `!!` without parentheses, which is checked as well. A client of Model.serve could send such a query.

The answers are compared with pyoxigraph, over values that make the inner test an error (a string
or an IRI compared with a number) and an unbound variable, which three-valued logic must keep.
"""

import json
import time
from collections import Counter

import pyoxigraph
import pytest

from maplib import Model

EX = "http://example.net/"
DATA = f"""
<{EX}a> <{EX}v> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<{EX}b> <{EX}v> "5"^^<http://www.w3.org/2001/XMLSchema#integer> .
<{EX}c> <{EX}v> "x" .
<{EX}d> <{EX}v> <{EX}iri> .
<{EX}a> <{EX}w> "7"^^<http://www.w3.org/2001/XMLSchema#integer> .
<{EX}c> <{EX}w> "0"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""

PATTERN = "?s ex:v ?v OPTIONAL { ?s ex:w ?w }"


def nots(depth, inner):
    return "!(" * depth + inner + ")" * depth


def mixed(depth):
    # Each level negates and combines with a test that is an error, unbound or plain.
    e = "?v > 2"
    for i in range(depth):
        if i % 3 == 0:
            e = f"!({e} && ?w < 3)"
        elif i % 3 == 1:
            e = f"!({e} || BOUND(?w))"
        else:
            e = f"!(!({e}))"
    return e


EXPRESSIONS = {
    "nots_even": nots(120, "?v > 2"),
    "nots_odd": nots(121, "?v > 2"),
    "nots_bound": nots(101, "BOUND(?w)"),
    "nots_unbound_compare": nots(100, "?w > 2"),
    "nots_spaced": "! (" * 100 + "?v > 2" + ")" * 100,
    "mixed": mixed(100),
    "mixed_odd": mixed(101),
}


def _term(t):
    if isinstance(t, dict):
        return (t["type"], t["value"])
    if isinstance(t, pyoxigraph.NamedNode):
        return ("uri", t.value)
    return ("literal", t.value)


def _oxigraph(q):
    store = pyoxigraph.Store()
    store.load(DATA.encode(), pyoxigraph.RdfFormat.N_TRIPLES)
    res = store.query(q)
    names = [v.value for v in res.variables]
    return Counter(
        tuple(sorted((n, _term(sol[n])) for n in names if sol[n] is not None)) for sol in res
    )


def _maplib(m, q):
    j = json.loads(m.query(q, return_format="json"))
    return Counter(
        tuple(sorted((n, _term(b)) for n, b in row.items())) for row in j["results"]["bindings"]
    )


@pytest.mark.parametrize("name", sorted(EXPRESSIONS))
def test_nested_not_fast_and_matches_oxigraph(name):
    m = Model()
    m.reads(DATA, format="ntriples")
    q = f"PREFIX ex: <{EX}> SELECT ?s WHERE {{ {PATTERN} FILTER({EXPRESSIONS[name]}) }}"
    start = time.perf_counter()
    got = _maplib(m, q)
    elapsed = time.perf_counter() - start
    assert elapsed < 5.0, f"{name} took {elapsed:.1f} s"
    assert got == _oxigraph(q)


def test_nested_not_in_bind_fast_and_matches_oxigraph():
    m = Model()
    m.reads(DATA, format="ntriples")
    q = f"PREFIX ex: <{EX}> SELECT ?s ?b WHERE {{ {PATTERN} BIND({mixed(100)} AS ?b) }}"
    start = time.perf_counter()
    got = _maplib(m, q)
    assert time.perf_counter() - start < 5.0
    assert got == _oxigraph(q)


@pytest.mark.parametrize("n", [2, 3, 200, 201])
def test_negations_without_parentheses_fast_and_match_oxigraph(n):
    # SPARQL 1.2 allows `!!` without parentheses (it was a syntax error in SPARQL 1.1). A long
    # run of `!` must still be parsed at once.
    m = Model()
    m.reads(DATA, format="ntriples")
    q = f"PREFIX ex: <{EX}> SELECT ?s WHERE {{ {PATTERN} FILTER({'!' * n}(?v > 2)) }}"
    start = time.perf_counter()
    got = _maplib(m, q)
    assert time.perf_counter() - start < 5.0
    assert got == _oxigraph(q)


@pytest.mark.parametrize("function", ["SUM", "COUNT", "MAX", "SAMPLE", "GROUP_CONCAT"])
def test_nested_aggregate_calls_parse_fast(function):
    # SUM(...) was tried as a window function, failed without OVER, and its argument was parsed
    # again as an aggregate, so nested calls took time exponential in their depth. Nested
    # aggregates are not allowed, but the query must be answered or rejected at once.
    m = Model()
    m.reads(DATA, format="ntriples")
    arg = f"{function}(" * 60 + "?v" + ")" * 60
    q = f"PREFIX ex: <{EX}> SELECT ({arg} AS ?x) WHERE {{ ?s ex:v ?v }}"
    start = time.perf_counter()
    try:
        m.query(q)
    except Exception:
        pass
    assert time.perf_counter() - start < 5.0


def test_aggregate_over_nested_not_matches_oxigraph():
    m = Model()
    m.reads(DATA, format="ntriples")
    q = (
        # COALESCE keeps errors out of the aggregates, which maplib skips but oxigraph propagates.
        f"PREFIX ex: <{EX}> SELECT (COUNT(DISTINCT {nots(100, 'BOUND(?w)')}) AS ?c) "
        f"(SUM(IF(COALESCE({mixed(60)}, false), 1, 0)) AS ?n) WHERE {{ {PATTERN} }}"
    )
    start = time.perf_counter()
    got = _maplib(m, q)
    assert time.perf_counter() - start < 5.0
    assert got == _oxigraph(q)
