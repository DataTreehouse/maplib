"""Counts over one triple pattern are answered from the stored triple tables (distinct subjects,
predicates and objects, filters on the kind of the subject or object, a fixed predicate). Each
query is checked against the general path, which an extra `true` condition makes it take."""
import pytest
from maplib import Model

DATA = """
<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .
<http://ex.org/a> <http://ex.org/q> <http://ex.org/b> .
<http://ex.org/b> <http://ex.org/p> <http://ex.org/a> .
<http://ex.org/a> <http://ex.org/p> "x" .
<http://ex.org/b> <http://ex.org/q> "x" .
<http://ex.org/b> <http://ex.org/q> "x"@en .
<http://ex.org/b> <http://ex.org/q> "x"@nb .
<http://ex.org/c> <http://ex.org/r> "y"@en .
<http://ex.org/a> <http://ex.org/q> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/c> <http://ex.org/q> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/c> <http://ex.org/q> "2"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/c> <http://ex.org/r> "1.5"^^<http://www.w3.org/2001/XMLSchema#double> .
<http://ex.org/a> <http://ex.org/d> "2020-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex.org/b> <http://ex.org/d> "2020-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex.org/b> <http://ex.org/d> "2020-01-01T01:00:00+01:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://ex.org/c> <http://ex.org/d> "2021-06-01"^^<http://www.w3.org/2001/XMLSchema#date> .
_:b1 <http://ex.org/p> <http://ex.org/a> .
_:b1 <http://ex.org/q> _:b2 .
_:b2 <http://ex.org/p> "x" .
<http://ex.org/c> <http://ex.org/p> _:b1 .
"""

AGGS = [
    "COUNT(*)",
    "COUNT(DISTINCT *)",
    "COUNT(?s)",
    "COUNT(?o)",
    "COUNT(DISTINCT ?s)",
    "COUNT(DISTINCT ?o)",
]
FILTERS = [
    None,
    "ISLITERAL(?o)",
    "ISBLANK(?s)",
    "ISIRI(?o)",
    "ISURI(?s)",
    "!ISLITERAL(?o)",
    "ISIRI(?s) && ISLITERAL(?o)",
    "!ISBLANK(?s) && !ISIRI(?o)",
    "ISBLANK(?o)",
]
PREDICATES = ["?p", "<http://ex.org/q>", "<http://ex.org/d>", "<http://ex.org/missing>"]


def query(pred, agg, flt, general):
    conds = [c for c in [flt, "true" if general else None] if c]
    f = f"FILTER({' && '.join(f'({c})' for c in conds)})" if conds else ""
    return f"SELECT ({agg} AS ?c) WHERE {{ ?s {pred} ?o {f} }}"


@pytest.fixture(scope="module")
def m():
    m = Model()
    m.reads(DATA, format="ntriples")
    return m


@pytest.mark.parametrize("pred", PREDICATES)
@pytest.mark.parametrize("agg", AGGS)
@pytest.mark.parametrize("flt", FILTERS)
def test_matches_general_path(m, pred, agg, flt):
    fast = m.query(query(pred, agg, flt, False))
    general = m.query(query(pred, agg, flt, True))
    assert fast["c"].to_list() == general["c"].to_list()
    assert fast["c"].dtype == general["c"].dtype


@pytest.mark.parametrize("flt", FILTERS)
def test_distinct_predicates(m, flt):
    for agg in ["COUNT(DISTINCT ?p)", "COUNT(?p)"]:
        fast = m.query(query("?p", agg, flt, False))["c"].to_list()
        assert fast == m.query(query("?p", agg, flt, True))["c"].to_list()


def test_known_counts(m):
    def c(q):
        return m.query(q)["c"][0]

    assert c("SELECT (COUNT(DISTINCT ?p) AS ?c) WHERE { ?s ?p ?o }") == 4  # p, q, r, d
    # a, b, c and two blank nodes
    assert c("SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }") == 5
    # The same string with different language tags, and the same instant in two time zones,
    # are different terms.
    assert c(
        'SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s <http://ex.org/q> ?o FILTER ISLITERAL(?o) }'
    ) == 5
    assert c("SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s <http://ex.org/d> ?o }") == 3
    assert c("SELECT (COUNT(?s) AS ?c) WHERE { ?s ?p ?o FILTER ISBLANK(?s) }") == 3


def test_several_aggregates(m):
    q = """SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?s) AS ?s_) (COUNT(DISTINCT ?p) AS ?p_)
                  (COUNT(DISTINCT ?o) AS ?o_) WHERE { ?s ?p ?o %s }"""
    assert m.query(q % "").rows() == m.query(q % "FILTER(true)").rows()


def test_not_fast_path_shapes(m):
    # A variable the pattern does not bind, and a pattern with a repeated variable.
    for q in [
        "SELECT (COUNT(?x) AS ?c) WHERE { ?s ?p ?o }",
        "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?s }",
        "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o FILTER(ISLITERAL(?p)) }",
        "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o FILTER(!(ISIRI(?s) && ISLITERAL(?o))) }",
    ]:
        assert m.query(q)["c"].to_list() == m.query(
            q.replace("WHERE {", "WHERE { FILTER(true)")
        )["c"].to_list()


def test_after_insert_and_delete():
    m = Model()
    m.reads(DATA, format="ntriples")
    q = "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o %s }"

    def both():
        return m.query(q % "")["c"][0], m.query(q % "FILTER(true)")["c"][0]

    m.update('INSERT DATA { <http://ex.org/e> <http://ex.org/q> "x"@en , "new" , <http://ex.org/z> . }')
    fast, general = both()
    assert fast == general
    m.update('DELETE DATA { <http://ex.org/e> <http://ex.org/q> "new" , <http://ex.org/z> . }')
    fast2, general2 = both()
    assert fast2 == general2 == fast - 2


def test_named_graph():
    m = Model()
    m.reads(DATA, format="ntriples")
    m.reads('<http://ex.org/z> <http://ex.org/p> "z" .', format="ntriples", graph="urn:g:2")
    q = "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }"
    assert m.query(q, graph="urn:g:2")["c"][0] == 1
    assert m.query(q.replace("WHERE", "FROM <urn:g:2> WHERE"))["c"][0] == 1


def _large_data(seed, n, prefix):
    """N-Triples with many distinct IRIs, blank nodes, strings, language-tagged strings, other
    datatypes and numbers, enough that the distinct counts are computed in parallel."""
    import random

    r = random.Random(seed)
    ex = "http://ex.org/"
    out = []
    for i in range(n):
        s = f"_:{prefix}b{r.randrange(n // 50)}" if i % 7 == 0 else f"<{ex}s{r.randrange(n // 3)}>"
        k = i % 6
        if k == 0:
            p, o = "p", f"<{ex}o{r.randrange(n)}>"
        elif k == 1:
            p, o = "q", f"_:{prefix}c{r.randrange(n // 20)}"
        elif k == 2:
            p, o = "q", f'"v{r.randrange(n // 2)}"'
        elif k == 3:
            p, o = "r", f'"w{r.randrange(n // 4)}"@{r.choice(["en", "nb", "de", "en-GB"])}'
        elif k == 4:
            p, o = "r", f'"t{r.randrange(n // 8)}"^^<{ex}customType>'
        else:
            p, o = "d", f'"{r.randrange(n // 5)}"^^<http://www.w3.org/2001/XMLSchema#integer>'
        out.append(f"{s} <{ex}{p}> {o} .")
    return "\n".join(out)


LARGE_QUERIES = [
    (pred, agg, flt)
    for pred in ["?p", "<http://ex.org/q>", "<http://ex.org/r>"]
    for agg in ["COUNT(DISTINCT ?s)", "COUNT(DISTINCT ?o)"]
    for flt in [None, "ISLITERAL(?o)", "ISBLANK(?s)", "!ISBLANK(?o)"]
]


def _check_large(m, graph=None):
    for pred, agg, flt in LARGE_QUERIES:
        fast = m.query(query(pred, agg, flt, False), graph=graph)["c"].to_list()
        general = m.query(query(pred, agg, flt, True), graph=graph)["c"].to_list()
        assert fast == general, (pred, agg, flt)


def test_large_mixed_types_segments_and_updates():
    """Many tables of several segments each (one per read), then inserts and deletes, and a
    named graph sharing the dictionary codes of the default graph."""
    m = Model()
    for i in range(3):
        m.reads(_large_data(i, 60_000, f"x{i}"), format="ntriples")
    _check_large(m)
    m.update(
        'INSERT DATA { <http://ex.org/new> <http://ex.org/q> "v1" , "brand new" , _:fresh , '
        '"w1"@en , "w1"@fr . }'
    )
    _check_large(m)
    m.update(
        "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o "
        'FILTER(STRSTARTS(STR(?s), "http://ex.org/s1")) }'
    )
    _check_large(m)
    m.reads(_large_data(9, 40_000, "g"), format="ntriples", graph="urn:g:large")
    _check_large(m, graph="urn:g:large")
    _check_large(m)


def test_known_distinct_counts_large():
    """Distinct counts that are known from how the data is built."""
    ex = "http://ex.org/"
    n = 200_000
    lines = [f"<{ex}s{i % 70_000}> <{ex}p> <{ex}o{i % 90_001}> ." for i in range(n)]
    lines += [f'<{ex}s{i}> <{ex}l> "x{i % 1000}"@{["en", "nb"][i % 2]} .' for i in range(4000)]
    m = Model()
    m.reads("\n".join(lines), format="ntriples")

    def c(q):
        return m.query(q)["c"][0]

    assert c(f"SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE {{ ?s <{ex}p> ?o }}") == 70_000
    assert c(f"SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE {{ ?s <{ex}p> ?o }}") == 90_001
    # 1000 values, each with one of the two tags in every other row: (x_k, en) for even k
    # and (x_k, nb) for odd k, since i % 1000 and i % 2 agree in parity.
    assert c(f"SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE {{ ?s <{ex}l> ?o }}") == 1000
    assert c("SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }") == 91_001
    assert c("SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }") == 70_000
