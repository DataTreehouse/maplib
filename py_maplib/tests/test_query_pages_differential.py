"""Differential tests of keyset pagination on seeded random data.

For each query, the keyset pages put together must be exactly the solutions of the query
ordered by the total order of paging (the ORDER BY, then the projected variables), and so
must OFFSET pages of that query. Where the sort keys have one type each, the order is also
compared with rdflib's SPARQL implementation. The data has duplicates, ties, unbound sort
keys, blank nodes and columns of mixed types (IRIs, plain and language-tagged strings,
integers, decimals, doubles, booleans and dateTimes with and without time zones), and the
queries order by several keys, ascending and descending, by expressions and by variables
that are not projected.
"""

import json
import random
from collections import Counter

import pytest
import rdflib

from maplib import Model

from .disk import disk_params, storage_kwargs

EX = "http://example.net/"
XSD = "http://www.w3.org/2001/XMLSchema#"
PREFIX = f"PREFIX : <{EX}>\nPREFIX xsd: <{XSD}>\n"
SEEDS = [1, 2, 3]


def random_value(rnd: random.Random) -> str:
    kind = rnd.randrange(10)
    if kind == 0:
        return f"<{EX}o{rnd.randrange(8)}>"
    if kind == 1:
        return f'"w{rnd.randrange(6)}"'
    if kind == 2:
        return f'"w{rnd.randrange(6)}"@{rnd.choice(["en", "nb", "de"])}'
    if kind == 3:
        return str(rnd.randrange(-5, 6))
    if kind == 4:
        return f"{rnd.randrange(-50, 50) / 10:.1f}"
    if kind == 5:
        return f'"{rnd.randrange(-3, 4)}.5E0"^^<{XSD}double>'
    if kind == 6:
        return rnd.choice(["true", "false"])
    if kind == 7:
        tz = rnd.choice(["", "Z", "+02:00", "-05:00"])
        return f'"2020-01-0{rnd.randrange(1, 4)}T1{rnd.randrange(0, 3)}:00:00{tz}"^^<{XSD}dateTime>'
    if kind == 8:
        return f"_:b{rnd.randrange(5)}"
    return f'"{rnd.choice(["Ångström", "apple", "Apple", "a b", ""])}"'


def dataset(seed: int, n: int = 70) -> str:
    rnd = random.Random(seed)
    lines = []
    for i in range(n):
        s = f"<{EX}s{rnd.randrange(10**6):06d}>"
        lines.append(f"{s} :b {rnd.randrange(5)} .")
        lines.append(f'{s} :c "c{rnd.randrange(4)}" .')
        lines.append(f"{s} :i <{EX}i{rnd.randrange(12):02d}> .")
        # Unbound in about a fifth of the solutions.
        if rnd.random() < 0.8:
            lines.append(f"{s} :a {random_value(rnd)} .")
    return "@prefix : <" + EX + "> .\n" + "\n".join(lines) + "\n"


# (query, the same query in the total order of paging). With ties left only between
# solutions that are equal in every projected variable, which are then indistinguishable.
QUERIES = [
    (
        "SELECT ?s ?a ?b WHERE { ?s :b ?b OPTIONAL { ?s :a ?a } } ORDER BY ?a DESC(?b)",
        "SELECT ?s ?a ?b WHERE { ?s :b ?b OPTIONAL { ?s :a ?a } } ORDER BY ?a DESC(?b) ?s",
    ),
    (
        "SELECT ?a ?b WHERE { ?s :b ?b OPTIONAL { ?s :a ?a } } ORDER BY DESC(?a) ?b",
        "SELECT ?a ?b WHERE { ?s :b ?b OPTIONAL { ?s :a ?a } } ORDER BY DESC(?a) ?b",
    ),
    (
        "SELECT ?b ?c WHERE { ?s :b ?b ; :c ?c } ORDER BY ?c",
        "SELECT ?b ?c WHERE { ?s :b ?b ; :c ?c } ORDER BY ?c ?b",
    ),
    (
        "SELECT DISTINCT ?b ?c WHERE { ?s :b ?b ; :c ?c } ORDER BY DESC(?b)",
        "SELECT DISTINCT ?b ?c WHERE { ?s :b ?b ; :c ?c } ORDER BY DESC(?b) ?c",
    ),
    (
        "SELECT ?s WHERE { ?s :c ?c ; :i ?i } ORDER BY DESC(?i) ?c",
        "SELECT ?s WHERE { ?s :c ?c ; :i ?i } ORDER BY DESC(?i) ?c ?s",
    ),
    (
        "SELECT ?b (COUNT(*) AS ?n) WHERE { ?s :b ?b ; :c ?c } GROUP BY ?b ORDER BY DESC(?n)",
        "SELECT ?b (COUNT(*) AS ?n) WHERE { ?s :b ?b ; :c ?c } GROUP BY ?b ORDER BY DESC(?n) ?b",
    ),
    (
        "SELECT ?a ?c WHERE { ?s :c ?c OPTIONAL { ?s :a ?a } }",
        "SELECT ?a ?c WHERE { ?s :c ?c OPTIONAL { ?s :a ?a } } ORDER BY ?a ?c",
    ),
    (
        'SELECT ?s ?k WHERE { ?s :c ?c ; :b ?b BIND(CONCAT(?c, "-", STR(?b)) AS ?k) } ORDER BY DESC(?k)',
        'SELECT ?s ?k WHERE { ?s :c ?c ; :b ?b BIND(CONCAT(?c, "-", STR(?b)) AS ?k) } ORDER BY DESC(?k) ?s',
    ),
    (
        "SELECT ?s ?b WHERE { ?s :b ?b ; :c ?c } ORDER BY (?b * 2 + STRLEN(?c)) DESC(?s)",
        "SELECT ?s ?b WHERE { ?s :b ?b ; :c ?c } ORDER BY (?b * 2 + STRLEN(?c)) DESC(?s) ?b",
    ),
]


def rows(s: str):
    res = json.loads(s)
    variables = res["head"]["vars"]
    return [
        tuple(
            None if v not in b else (b[v]["type"], b[v]["value"], b[v].get("datatype"), b[v].get("xml:lang"))
            for v in variables
        )
        for b in res["results"]["bindings"]
    ]


def keyset_rows(m, q, page_size):
    out = []
    pages = list(m.query_pages(PREFIX + q, page_size=page_size, return_format="json"))
    for i, p in enumerate(pages):
        r = rows(p)
        if i < len(pages) - 1:
            assert len(r) == page_size
        out.extend(r)
    return out


def offset_rows(m, q, page_size):
    out = []
    while True:
        r = rows(m.query(f"{PREFIX}{q} LIMIT {page_size} OFFSET {len(out)}", return_format="json"))
        out.extend(r)
        if len(r) < page_size:
            return out


@pytest.fixture(scope="module", params=[(s, st) for s in SEEDS for st in disk_params()],
                ids=lambda p: f"seed{p[0]}-{p[1]}")
def model(request):
    seed, storage = request.param
    m = Model(**storage_kwargs(storage))
    m.reads(dataset(seed), format="turtle")
    return m


@pytest.mark.parametrize("q,total", QUERIES, ids=range(len(QUERIES)))
@pytest.mark.parametrize("page_size", [1, 4, 13, 1000])
def test_keyset_and_offset_pages_equal_the_ordered_query(model, q, total, page_size):
    expected = rows(model.query(PREFIX + total, return_format="json"))
    # The paging order is a refinement of the query's own order.
    assert Counter(expected) == Counter(rows(model.query(PREFIX + q, return_format="json")))
    assert keyset_rows(model, q, page_size) == expected
    assert offset_rows(model, total, page_size) == expected


@pytest.mark.parametrize("q,total", QUERIES[:5], ids=range(5))
@pytest.mark.parametrize("offset,limit", [(0, 7), (3, 20), (11, 1), (40, 1000)])
def test_limit_and_offset_of_the_query(model, q, total, offset, limit):
    expected = rows(model.query(PREFIX + total, return_format="json"))[offset : offset + limit]
    for page_size in [1, 5, 50]:
        got = keyset_rows(model, f"{q} OFFSET {offset} LIMIT {limit}", page_size)
        assert got == expected, page_size


def test_tokens_resume_in_another_iterator(model):
    q, total = QUERIES[0]
    expected = rows(model.query(PREFIX + total, return_format="json"))
    got, token = [], None
    while True:
        page, token = model.query_page(PREFIX + q, page_size=6, page_token=token, return_format="json")
        got.extend(rows(page))
        if token is None:
            break
    assert got == expected


# rdflib as a second implementation of SPARQL ORDER BY, for sort keys of one type each, where
# SPARQL defines the order (it leaves the order of some mixed types to implementations).
RDFLIB_QUERIES = [
    "SELECT ?s ?b ?c WHERE { ?s :b ?b ; :c ?c } ORDER BY DESC(?b) ?c ?s",
    "SELECT ?s ?i WHERE { ?s :i ?i } ORDER BY ?i DESC(?s)",
    "SELECT ?i ?c WHERE { ?s :i ?i ; :c ?c } ORDER BY DESC(?c) ?i",
]


def rdflib_rows(g, q):
    out = []
    for r in g.query(PREFIX + q):
        row = []
        for t in r:
            if t is None:
                row.append(None)
            elif isinstance(t, rdflib.URIRef):
                row.append(("uri", str(t), None, None))
            else:
                dt = str(t.datatype) if t.datatype else None
                row.append(("literal", str(t), dt, t.language))
        out.append(tuple(row))
    return out


def normalize(rs):
    # maplib writes xsd:string literals with their datatype in JSON results, rdflib without.
    return [
        tuple(
            (t[0], t[1], None, t[3]) if t and t[2] == XSD + "string" else t
            for t in r
        )
        for r in rs
    ]


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("q", RDFLIB_QUERIES)
def test_order_agrees_with_rdflib(seed, q):
    data = dataset(seed)
    m = Model()
    m.reads(data, format="turtle")
    g = rdflib.Graph()
    g.parse(data=data, format="turtle")
    expected = normalize(rdflib_rows(g, q))
    assert normalize(rows(m.query(PREFIX + q, return_format="json"))) == expected
    for page_size in [1, 7, 100]:
        assert normalize(keyset_rows(m, q, page_size)) == expected
