"""Window functions and ordered aggregates compared with SQL (SQLite) on random data.

There is no SPARQL standard or test suite for window functions (w3c-cg/sparql-dev#47 proposes the
SQL syntax and semantics), so the reference is SQL: SQLite (from the Python standard library)
evaluates the same window over the same data, and the results must agree. The datasets are
small, seeded and random, with ties in the ORDER BY keys, unbound (NULL) keys, values and
partition keys.

The deliberate differences from SQL, tested on their own at the end of this file:
- Aggregates over a frame without bound values: SPARQL's COUNT and SUM are 0 and AVG is 0, SQL
  gives NULL for SUM and AVG.
- AVG of integers is an xsd:decimal in SPARQL and a float in SQLite, so they are compared as
  numbers.
- Values of several types: SPARQL's SUM and AVG are errors (unbound) when a value is not numeric,
  SQLite converts strings to 0. In ORDER BY, numbers come before strings in both.
- GROUP_CONCAT of no values is "" in SPARQL and NULL in SQL.
Unbound values sort first in ascending and last in descending order, and unbound PARTITION BY
values form one partition, in both.
"""

import math
import random
import sqlite3
from functools import lru_cache

import pytest

from maplib import Model

EX = "http://example.org/"
PREFIX = f"PREFIX : <{EX}>\n"
SEEDS = range(20)

WHERE = """WHERE { ?r :id ?id OPTIONAL { ?r :p ?p } OPTIONAL { ?r :k ?k } OPTIONAL { ?r :v ?v } }"""


def dataset(seed, mixed=False):
    """Rows (id, partition, key, value), with None for unbound."""
    rnd = random.Random(seed)
    rows = []
    for i in range(rnd.randint(0, 25)):
        p = rnd.choice(["a", "b", "c", None])
        k = rnd.choice([0, 1, 1, 2, 3, None])
        if mixed and rnd.random() < 0.3:
            k = rnd.choice(["x", "y", 1.5])
        v = rnd.choice([-2, 0, 1, 3, 5, 8, None])
        rows.append((i, p, k, v))
    return rows


def turtle_term(x):
    if isinstance(x, str):
        return f'"{x}"'
    if isinstance(x, float):
        return f'"{x}"^^<http://www.w3.org/2001/XMLSchema#decimal>'
    return str(x)


@lru_cache(maxsize=None)
def stores(seed, mixed=False):
    rows = dataset(seed, mixed)
    lines = [f"@prefix : <{EX}> ."]
    for i, p, k, v in rows:
        t = f":r{i} :id {i}"
        for name, x in (("p", p), ("k", k), ("v", v)):
            if x is not None:
                t += f" ; :{name} {turtle_term(x)}"
        lines.append(t + " .")
    m = Model()
    m.reads("\n".join(lines), format="turtle")
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE t (id INTEGER, p TEXT, k, v INTEGER)")
    db.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
    return m, db


def number(x):
    if x is None:
        return None
    return float(x)


def sparql_by_id(m, select):
    df = m.query(PREFIX + f"SELECT ?id ({select} AS ?w) {WHERE}")
    return {i: w for i, w in df.iter_rows()}


def sql_by_id(db, select):
    return {i: w for i, w in db.execute(f"SELECT id, {select} FROM t")}


def assert_same(sparql, sql, numeric=False):
    assert sparql.keys() == sql.keys()
    for i in sql:
        a, b = sparql[i], sql[i]
        if numeric:
            a, b = number(a), number(b)
            assert (a is None and b is None) or (
                a is not None and b is not None and math.isclose(a, b, rel_tol=1e-9)
            ), (i, sparql[i], sql[i])
        else:
            assert a == b, (i, sparql[i], sql[i])


def order(direction, ties=True):
    """The window ORDER BY in SPARQL and SQL, with unbound/NULL first ascending and last
    descending in both."""
    tail_sparql = "" if ties else " ?id"
    tail_sql = "" if ties else ", id"
    if direction == "ASC":
        return f"ORDER BY ?k{tail_sparql}", f"ORDER BY k ASC NULLS FIRST{tail_sql}"
    return f"ORDER BY DESC(?k){tail_sparql}", f"ORDER BY k DESC NULLS LAST{tail_sql}"


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("direction", ["ASC", "DESC"])
@pytest.mark.parametrize("partition", [True, False])
@pytest.mark.parametrize("mixed", [False, True])
def test_ranking_functions(seed, direction, partition, mixed):
    m, db = stores(seed, mixed)
    sp, sq = order(direction)
    part_sparql = "PARTITION BY ?p " if partition else ""
    part_sql = "PARTITION BY p " if partition else ""
    for f in ["RANK", "DENSE_RANK"]:
        assert_same(
            sparql_by_id(m, f"{f}() OVER ({part_sparql}{sp})"),
            sql_by_id(db, f"{f}() OVER ({part_sql}{sq})"),
        )
    # ROW_NUMBER needs a total order to be deterministic.
    sp, sq = order(direction, ties=False)
    assert_same(
        sparql_by_id(m, f"ROW_NUMBER() OVER ({part_sparql}{sp})"),
        sql_by_id(db, f"ROW_NUMBER() OVER ({part_sql}{sq})"),
    )


def test_rank_one_filter_matches_sql():
    # FILTER(?rk = 1) on a RANK has its own evaluation (no ranking).
    for seed in SEEDS:
        for direction in ["ASC", "DESC"]:
            m, db = stores(seed)
            sp, sq = order(direction)
            df = m.query(
                PREFIX
                + f"""SELECT ?id WHERE {{
                    {{ ?r :id ?id OPTIONAL {{ ?r :p ?p }} OPTIONAL {{ ?r :k ?k }} }}
                    BIND(RANK() OVER (PARTITION BY ?p {sp}) AS ?rk) FILTER(?rk = 1) }}"""
            )
            expected = db.execute(
                f"SELECT id FROM (SELECT id, RANK() OVER (PARTITION BY p {sq}) AS rk FROM t) "
                "WHERE rk = 1"
            ).fetchall()
            assert sorted(df["id"].to_list()) == sorted(i for (i,) in expected), (seed, direction)


# SPARQL aggregate, SQL aggregate, compared as numbers
AGGREGATES = [
    ("COUNT(*)", "COUNT(*)"),
    ("COUNT(?v)", "COUNT(v)"),
    ("SUM(?v)", "COALESCE(SUM(v), 0)"),
    ("AVG(?v)", "COALESCE(AVG(v), 0)"),
    ("MIN(?v)", "MIN(v)"),
    ("MAX(?v)", "MAX(v)"),
]

FRAMES = [
    # Whole partition, no ORDER BY
    (None, None, None),
    # Running aggregate up to and including the peers of the row (RANGE in SQL)
    ("ASC", True, None),
    ("DESC", True, None),
    # ROWS frames, with a total order
    ("ASC", False, "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"),
    ("ASC", False, "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"),
    ("DESC", False, "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"),
    ("ASC", False, "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING"),
    ("ASC", False, "ROWS UNBOUNDED PRECEDING"),
]


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("frame", FRAMES, ids=lambda f: "-".join(str(x) for x in f))
@pytest.mark.parametrize("partition", [True, False])
def test_aggregate_windows(seed, frame, partition):
    m, db = stores(seed)
    direction, ties, rows = frame
    part_sparql = "PARTITION BY ?p " if partition else ""
    part_sql = "PARTITION BY p " if partition else ""
    if direction is None:
        sp, sq = "", ""
    else:
        sp, sq = order(direction, ties)
    if rows:
        sp, sq = f"{sp} {rows}", f"{sq} {rows}"
    for sparql_agg, sql_agg in AGGREGATES:
        # COALESCE(SUM(v) OVER ..., 0): SPARQL's SUM and AVG of no values are 0.
        if sql_agg.startswith("COALESCE"):
            inner = sql_agg[len("COALESCE(") : -len(", 0)")]
            sql_expr = f"COALESCE({inner} OVER ({part_sql}{sq}), 0)"
        else:
            sql_expr = f"{sql_agg} OVER ({part_sql}{sq})"
        assert_same(
            sparql_by_id(m, f"{sparql_agg} OVER ({part_sparql}{sp})"),
            sql_by_id(db, sql_expr),
            numeric=True,
        )


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("direction", ["ASC", "DESC"])
def test_ordered_group_concat_and_sample(seed, direction):
    m, db = stores(seed)
    sp = "?k ?id" if direction == "ASC" else "DESC(?k) ?id"
    sq = "k ASC NULLS FIRST, id" if direction == "ASC" else "k DESC NULLS LAST, id"
    df = m.query(
        PREFIX
        + f"""SELECT ?p (GROUP_CONCAT(STR(?v); SEPARATOR="," ORDER BY {sp}) AS ?c)
            (SAMPLE(?v ORDER BY {sp}) AS ?first)
            {WHERE} GROUP BY ?p"""
    )
    got = {(p.strip("\"") if p else None): (c, f) for p, c, f in df.iter_rows()}
    expected = {}
    for (p,) in db.execute("SELECT DISTINCT p FROM t"):
        (c,) = db.execute(
            f"SELECT group_concat(v, ',' ORDER BY {sq}) FROM t WHERE p IS ?", (p,)
        ).fetchone()
        first = db.execute(
            f"SELECT v FROM t WHERE p IS ? AND v IS NOT NULL ORDER BY {sq} LIMIT 1", (p,)
        ).fetchone()
        # GROUP_CONCAT of no values is "" in SPARQL, NULL in SQL
        expected[p] = (c if c is not None else "", first[0] if first else None)
    assert got == expected


# Deliberate differences from SQL


def test_sum_and_avg_of_no_values_are_zero_not_null():
    m = Model()
    m.reads(f"@prefix : <{EX}> . :a :id 1 . :b :id 2 ; :v 4 .", format="turtle")
    df = m.query(
        PREFIX
        + f"""SELECT ?id (SUM(?v) OVER (ORDER BY ?id) AS ?sum) (AVG(?v) OVER (ORDER BY ?id) AS ?avg)
            (COUNT(?v) OVER (ORDER BY ?id) AS ?count) (MIN(?v) OVER (ORDER BY ?id) AS ?min)
            {WHERE} ORDER BY ?id"""
    )
    first = df.row(0, named=True)
    # SQL: SUM NULL, AVG NULL, COUNT 0, MIN NULL
    assert (first["sum"], float(first["avg"]), first["count"], first["min"]) == (0, 0.0, 0, None)


def test_sum_of_a_string_is_an_error_not_zero():
    m = Model()
    m.reads(f'@prefix : <{EX}> . :a :id 1 ; :v 2 . :b :id 2 ; :v "x" .', format="turtle")
    df = m.query(PREFIX + f"SELECT ?id (SUM(?v) OVER (ORDER BY ?id) AS ?sum) {WHERE} ORDER BY ?id")
    # SQLite converts "x" to 0, so it would give 2 and 2
    assert df["sum"].to_list() == [2, None]


def test_unbound_sorts_like_sql_nulls_first():
    # Unbound keys are the lowest values, as NULLS FIRST ascending / NULLS LAST descending.
    m, db = stores(3)
    assert_same(
        sparql_by_id(m, "ROW_NUMBER() OVER (ORDER BY ?k ?id)"),
        sql_by_id(db, "ROW_NUMBER() OVER (ORDER BY k ASC NULLS FIRST, id)"),
    )
    assert_same(
        sparql_by_id(m, "ROW_NUMBER() OVER (ORDER BY DESC(?k) ?id)"),
        sql_by_id(db, "ROW_NUMBER() OVER (ORDER BY k DESC NULLS LAST, id)"),
    )
