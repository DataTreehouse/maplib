"""ORDER BY with LIMIT (and OFFSET): a large LIMIT is collected as a sort followed by a slice
with the in-memory engine, a small one by the engine of the whole query. Either way the results
must be those of the full sort, sliced."""

import os
import random

import pytest

from maplib import Model

N = 200_000
EX = "http://example.net/"


@pytest.fixture(scope="module")
def model():
    rnd = random.Random(7)
    values = [rnd.randrange(1000) for _ in range(N)]
    lines = []
    for i, v in enumerate(values):
        lines.append(f'<{EX}s{i:06d}> <{EX}value> "{v}"^^<http://www.w3.org/2001/XMLSchema#integer> .')
        if i % 3 == 0:
            lines.append(f"<{EX}s{i:06d}> <{EX}tag> <{EX}t{i % 7}> .")
    m = Model()
    m.reads("\n".join(lines) + "\n", format="ntriples")
    return m, values


def expected_desc(values, start, length):
    order = sorted(range(len(values)), key=lambda i: (-values[i], i))
    return [(f"<{EX}s{i:06d}>", values[i]) for i in order[start : start + length]]


@pytest.mark.parametrize("offset", [0, 17, N // 2, N - 3, N + 5])
def test_int_desc_with_ties(model, offset):
    m, values = model
    df = m.query(
        f"""SELECT ?s ?v WHERE {{ ?s <{EX}value> ?v }}
        ORDER BY DESC(?v) ?s LIMIT 10 OFFSET {offset}"""
    )
    assert list(zip(df["s"].to_list(), df["v"].to_list())) == expected_desc(values, offset, 10)


@pytest.mark.parametrize("limit,offset", [(15_000, 0), (12_000, 17), (5_000, 9_000), (20_000, N - 100)])
def test_int_desc_large_limit(model, limit, offset):
    m, values = model
    df = m.query(
        f"""SELECT ?s ?v WHERE {{ ?s <{EX}value> ?v }}
        ORDER BY DESC(?v) ?s LIMIT {limit} OFFSET {offset}"""
    )
    assert list(zip(df["s"].to_list(), df["v"].to_list())) == expected_desc(values, offset, limit)


def test_limit_through_distinct_and_projection(model):
    m, values = model
    df = m.query(
        f"SELECT DISTINCT ?v WHERE {{ ?s <{EX}value> ?v }} ORDER BY DESC(?v) LIMIT 5"
    )
    assert df["v"].to_list() == sorted(set(values), reverse=True)[:5]


def test_unbound_sorts_first_ascending(model):
    m, _ = model
    df = m.query(
        f"""SELECT ?s ?t WHERE {{ ?s <{EX}value> ?v OPTIONAL {{ ?s <{EX}tag> ?t }} }}
        ORDER BY ?t ?s LIMIT 3"""
    )
    assert df["t"].to_list() == [None, None, None]
    assert df["s"].to_list() == [f"<{EX}s000001>", f"<{EX}s000002>", f"<{EX}s000004>"]


def test_sorted_slice_in_subquery_joined_with_outer(model):
    m, values = model
    df = m.query(
        f"""SELECT ?s ?v ?t WHERE {{
            ?s <{EX}tag> ?t .
            {{ SELECT ?s ?v WHERE {{ ?s <{EX}value> ?v }} ORDER BY DESC(?v) ?s LIMIT 1000 }}
        }} ORDER BY ?s"""
    )
    top = expected_desc(values, 0, 1000)
    expected = sorted((s, v) for s, v in top if int(s[len(EX) + 2 : -1]) % 3 == 0)
    assert list(zip(df["s"].to_list(), df["v"].to_list())) == expected


def test_streaming_engine_gives_the_same_result(model):
    m, values = model
    q = f"SELECT ?s ?v WHERE {{ ?s <{EX}value> ?v }} ORDER BY DESC(?v) ?s LIMIT 10 OFFSET 3"
    old = os.environ.get("MAPLIB_POLARS_ENGINE")
    os.environ["MAPLIB_POLARS_ENGINE"] = "streaming"
    try:
        df = m.query(q)
    finally:
        if old is None:
            del os.environ["MAPLIB_POLARS_ENGINE"]
        else:
            os.environ["MAPLIB_POLARS_ENGINE"] = old
    assert list(zip(df["s"].to_list(), df["v"].to_list())) == expected_desc(values, 3, 10)
