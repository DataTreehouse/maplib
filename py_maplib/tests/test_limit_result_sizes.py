"""LIMIT queries of several sizes over one large pattern, as SPARQL JSON and as data frames.

Results of at most one chunk of the SPARQL JSON writer (4,096 rows) are decoded and written on the
calling thread, larger ones in parallel chunks. Both must give the same solutions, for variables
of one type and of several types (IRIs, language-tagged strings, dates and integers)."""
import json

import pytest
from maplib import Model

EX = "http://ex.org/"
X = "http://www.w3.org/2001/XMLSchema#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
N = 9_000


def obj(i):
    k = i % 4
    if k == 0:
        return f"<{EX}C{i % 37}>", ("uri", f"{EX}C{i % 37}", None)
    if k == 1:
        return f'"label {i}"@en', ("literal", f"label {i}", "en")
    if k == 2:
        d = f"2020-01-{1 + i % 28:02d}"
        return f'"{d}"^^<{X}date>', ("literal", d, f"{X}date")
    return f'"{i}"^^<{X}integer>', ("literal", str(i), f"{X}integer")


@pytest.fixture(scope="module")
def model():
    lines = []
    for i in range(N):
        lines.append(f"<{EX}s{i}> <{RDF_TYPE}> <{EX}C{i % 37}> .")
        lines.append(f"<{EX}s{i}> <{EX}p> {obj(i)[0]} .")
    m = Model()
    m.reads("\n".join(lines), format="ntriples")
    return m


def key(term):
    if term is None:
        return None
    return (term["type"], term["value"], term.get("xml:lang") or term.get("datatype"))


def rows(m, q):
    res = json.loads(m.query(q, return_json=True))
    return [tuple(key(b.get(v)) for v in res["head"]["vars"]) for b in res["results"]["bindings"]]


TYPE_ROWS = {(("uri", f"{EX}s{i}", None), ("uri", f"{EX}C{i % 37}", None)) for i in range(N)}
P_ROWS = {(("uri", f"{EX}s{i}", None), obj(i)[1]) for i in range(N)}


@pytest.mark.parametrize("limit", [0, 1, 10, 1000, 4096, 4097, 8000, None])
@pytest.mark.parametrize(
    "pattern,expected",
    [(f"?s <{RDF_TYPE}> ?o", TYPE_ROWS), (f"?s <{EX}p> ?o", P_ROWS)],
    ids=["one-type", "several-types"],
)
def test_limit_sizes(model, limit, pattern, expected):
    lim = "" if limit is None else f" LIMIT {limit}"
    q = f"SELECT ?s ?o WHERE {{ {pattern} }}{lim}"
    got = rows(model, q)
    n = N if limit is None else min(limit, N)
    assert len(got) == n
    assert len(set(got)) == n
    assert set(got) <= expected
    assert model.query(q).height == n


@pytest.mark.parametrize("limit", [10, 4096, 4097])
def test_ordered_limit_is_a_prefix(model, limit):
    q = f"SELECT ?s ?o WHERE {{ ?s <{EX}p> ?o }} ORDER BY ?s LIMIT {limit}"
    got = rows(model, q)
    ordered = sorted(P_ROWS, key=lambda r: r[0][1])
    assert got == ordered[:limit]


def test_unbound_values_in_a_small_result(model):
    q = (
        f"SELECT ?s ?o ?x WHERE {{ ?s <{RDF_TYPE}> ?o OPTIONAL {{ ?s <{EX}missing> ?x }} }} "
        f"LIMIT 10"
    )
    res = json.loads(model.query(q, return_json=True))
    assert res["head"]["vars"] == ["s", "o", "x"]
    bs = res["results"]["bindings"]
    assert len(bs) == 10 and all(set(b) == {"s", "o"} for b in bs)


@pytest.mark.parametrize("pattern", [f"?s <{RDF_TYPE}> ?o", f"?s <{EX}p> ?o"])
def test_one_chunk_and_several_chunks_write_the_same_bytes(model, pattern):
    # 4,096 rows are written on the calling thread, 4,097 in parallel chunks: the smaller result
    # is the larger one without its last binding, byte for byte.
    q = f"SELECT ?s ?o WHERE {{ {pattern} }} ORDER BY ?s LIMIT "
    small = model.query(q + "4096", return_json=True)
    large = model.query(q + "4097", return_json=True)
    assert small.endswith("]}}")
    body = small[: -len("]}}")]
    assert large.startswith(body)
    assert large[len(body)] == ","
    assert large.endswith("}]}}")
