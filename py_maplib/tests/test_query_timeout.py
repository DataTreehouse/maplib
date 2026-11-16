"""Model.query(timeout=) and Model.update(timeout=): queries that run too long are stopped."""

import time

import polars as pl
import pytest

from maplib import MaplibException, Model, QueryTimeoutError

# Every triple with every triple with every triple: N**3 rows, far too many to count in time.
CROSS_JOIN = "SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i }"
# The same, with a filter on every row: slower, and lighter on memory.
SLOW = (
    "SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i "
    "FILTER(STR(?c) != CONCAT(STR(?f), STR(?i))) }"
)
N = 1000


def _model(n):
    m = Model()
    df = pl.DataFrame(
        {
            "subject": [f"http://example.org/s{i}" for i in range(n)],
            "object": [f"http://example.org/o{i}" for i in range(n)],
        }
    )
    m.add_template(
        """
        @prefix ex: <http://example.org/> .
        ex:T [ ?subject, ?object ] :: { ottr:Triple(?subject, ex:p, ?object) } .
        """
    )
    m.map("http://example.org/T", df)
    return m


@pytest.fixture(scope="module")
def model():
    return _model(N)


def test_cross_join_stops_soon_after_its_timeout(model):
    started = time.monotonic()
    with pytest.raises(QueryTimeoutError, match="timed out after 1 s"):
        model.query(CROSS_JOIN, timeout=1)
    elapsed = time.monotonic() - started
    assert elapsed < 2.5, f"the query stopped {elapsed:.2f} s after it started"


def test_timeout_error_is_a_maplib_exception(model):
    with pytest.raises(MaplibException):
        model.query(SLOW, timeout=0.2)


def test_model_answers_after_a_timeout(model):
    with pytest.raises(QueryTimeoutError):
        model.query(SLOW, timeout=0.2)
    # The stopped query does not go on in the background.
    started = time.monotonic()
    df = model.query(
        "SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c }", timeout=10
    )
    assert df["c"][0] == N
    assert time.monotonic() - started < 1


def test_large_select_stops():
    # 64 million rows: cheap to make, but formatting them takes longer than the timeout. Kept
    # small, as a query that is not stopped formats all of them (a few GB).
    m = _model(400)
    started = time.monotonic()
    with pytest.raises(QueryTimeoutError):
        m.query("SELECT ?a ?c WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i }", timeout=0.3)
    assert time.monotonic() - started < 3


def test_queries_within_their_timeout_answer(model):
    df = model.query(
        "SELECT ?a WHERE { ?a <http://example.org/p> <http://example.org/o7> }", timeout=30
    )
    assert df["a"].to_list() == ["<http://example.org/s7>"]


def test_two_way_cross_join_within_timeout(model):
    df = model.query(
        "SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c . ?d ?e ?f }", timeout=60
    )
    assert df["c"][0] == N * N


def test_property_path_stops(model):
    # Two hops over the cross product of all subjects and objects.
    started = time.monotonic()
    with pytest.raises(QueryTimeoutError):
        model.query(
            """
            SELECT (COUNT(*) AS ?n) WHERE {
                ?a <http://example.org/p>* ?b . ?c ?d ?e . ?f ?g ?h
            }""",
            timeout=1,
        )
    assert time.monotonic() - started < 2.5


def test_update_times_out_and_changes_nothing(model):
    before = model.query("SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c }")["c"][0]
    with pytest.raises(QueryTimeoutError):
        model.update(
            """
            INSERT { ?a <http://example.org/q> ?n }
            WHERE { SELECT ?a (COUNT(*) AS ?n) WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i FILTER(STR(?c) != CONCAT(STR(?f), STR(?i))) } GROUP BY ?a }
            """,
            timeout=0.5,
        )
    after = model.query("SELECT (COUNT(*) AS ?c) WHERE { ?a ?b ?c }")["c"][0]
    assert after == before


def test_update_within_timeout():
    m = Model()
    m.update(
        "INSERT DATA { <http://example.org/a> <http://example.org/p> <http://example.org/b> }",
        timeout=30,
    )
    m.update(
        """
        INSERT { ?b <http://example.org/inv> ?a } WHERE { ?a <http://example.org/p> ?b }
        """,
        timeout=30,
    )
    df = m.query("SELECT ?x WHERE { <http://example.org/b> <http://example.org/inv> ?x }")
    assert df["x"].to_list() == ["<http://example.org/a>"]


def test_zero_timeout_stops_at_once(model):
    with pytest.raises(QueryTimeoutError):
        model.query("SELECT ?a WHERE { ?a ?b ?c }", timeout=0)


@pytest.mark.parametrize("bad", [-1, float("nan"), float("inf")])
def test_invalid_timeouts(model, bad):
    with pytest.raises(Exception, match="timeout"):
        model.query("SELECT ?a WHERE { ?a ?b ?c }", timeout=bad)
