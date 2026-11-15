"""Locking of a Model shared between threads.

Read operations (query, writes, size, ...) take a shared lock, so they run at the same time.
Operations that change the model (update, reads, map, ...) take an exclusive lock, so they wait for
the reads in progress and the reads wait for them.

A user defined function that waits on a threading primitive keeps the lock of the operation that
calls it, which lets the tests hold an operation inside the model for as long as they need.
"""

import threading

import polars as pl
import pytest

from maplib import IRI, Model, xsd

UDF = "urn:maplib:test:gate"
TIMEOUT = 20
# How long a blocked operation must stay blocked before we conclude that it waits for the lock
BLOCKED_FOR = 0.5

DATA = """
<http://example.net/a> <http://example.net/num> 1 .
<http://example.net/b> <http://example.net/num> 2 .
<http://example.net/c> <http://example.net/num> 3 .
"""

GATED_QUERY = f"""
SELECT ?s ?o WHERE {{
    ?s <http://example.net/num> ?n .
    BIND(<{UDF}>(?n) AS ?o)
}}
"""

PLAIN_QUERY = """
SELECT ?s ?n WHERE { ?s <http://example.net/num> ?n }
"""


class Gate:
    """A UDF that blocks every call on non-empty input until the gate is opened.

    `entered` is set when a call is inside the UDF, that is, when the operation calling it holds
    the lock of the model.
    """

    def __init__(self):
        self.entered = threading.Event()
        self.opened = threading.Event()

    def __call__(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.height > 0:
            self.entered.set()
            # No timeout: the tests open the gate in a finally block, and Runner.get guards against
            # hangs. A wall-clock timeout here fails when the process is paused or starved.
            self.opened.wait()
        return df.with_columns(pl.col("0").alias("out"))

    def open(self):
        self.opened.set()


class BarrierUdf:
    """A UDF where `parties` calls must be inside at the same time before any of them returns."""

    def __init__(self, parties: int):
        self.barrier = threading.Barrier(parties, timeout=TIMEOUT)
        self.local = threading.local()

    def __call__(self, df: pl.DataFrame) -> pl.DataFrame:
        # Wait once per thread, the UDF may be called for several batches of one query
        if df.height > 0 and not getattr(self.local, "waited", False):
            self.local.waited = True
            self.barrier.wait()
        return df.with_columns(pl.col("0").alias("out"))


class Runner(threading.Thread):
    """Runs a function in a thread and keeps its result or exception."""

    def __init__(self, f, *args, **kwargs):
        super().__init__(daemon=True)
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.error = None

    def run(self):
        try:
            self.result = self.f(*self.args, **self.kwargs)
        except BaseException as e:
            self.error = e

    def get(self):
        self.join(TIMEOUT)
        assert not self.is_alive(), "The operation did not finish"
        if self.error is not None:
            raise self.error
        return self.result


def start(f, *args, **kwargs) -> Runner:
    r = Runner(f, *args, **kwargs)
    r.start()
    return r


def assert_blocked(r: Runner):
    r.join(BLOCKED_FOR)
    assert r.is_alive(), "The operation should have waited for the lock"


def model_with(udf) -> Model:
    m = Model()
    m.add_udf(UDF, udf, xsd.integer, [xsd.integer])
    m.reads(DATA, format="turtle")
    return m


@pytest.mark.parametrize("n_queries", [2, 4])
def test_queries_run_concurrently(n_queries):
    # Every query waits in the UDF until all of them are there, which only happens if they all
    # hold the lock of the model at the same time. If queries were serialized, the barrier would
    # time out and the queries would fail.
    m = model_with(BarrierUdf(n_queries))
    runners = [start(m.query, GATED_QUERY) for _ in range(n_queries)]
    for r in runners:
        df = r.get()
        assert df.height == 3


def test_read_operations_run_during_a_query():
    gate = Gate()
    m = model_with(gate)
    q = start(m.query, GATED_QUERY)
    assert gate.entered.wait(TIMEOUT)

    # The query holds a read lock in the UDF, the other read operations must not wait for it
    reads = [
        start(m.query, PLAIN_QUERY),
        start(m.size),
        start(m.writes, format="ntriples"),
        start(m.get_predicate_iris),
        start(m.get_predicate, IRI("http://example.net/num")),
        start(m.list_udfs),
        start(m.get_templates),
    ]
    try:
        for r in reads:
            r.join(TIMEOUT)
            assert not r.is_alive(), "A read operation waited for the query"
        assert q.is_alive()
        assert reads[0].get().height == 3
        assert reads[1].get() == 3
        assert reads[2].get().count("\n") == 3
        assert [i.iri for i in reads[3].get()] == ["http://example.net/num"]
        assert reads[4].get()[0].mappings.height == 3
        assert reads[5].get() == [UDF]
    finally:
        gate.open()
    assert q.get().height == 3


def test_write_blocks_query():
    gate = Gate()
    m = model_with(gate)
    w = start(
        m.update,
        f"""
        INSERT {{ ?s <http://example.net/copy> ?o }}
        WHERE {{
            ?s <http://example.net/num> ?n .
            BIND(<{UDF}>(?n) AS ?o)
        }}
        """,
    )
    assert gate.entered.wait(TIMEOUT)

    q = start(m.query, "SELECT ?s ?o WHERE { ?s <http://example.net/copy> ?o }")
    s = start(m.size)
    try:
        assert_blocked(q)
        assert_blocked(s)
    finally:
        gate.open()
    w.get()
    # The query ran after the update, so it sees its triples
    assert q.get().height == 3
    assert s.get() == 6


def test_query_blocks_write():
    gate = Gate()
    m = model_with(gate)
    q = start(m.query, GATED_QUERY)
    assert gate.entered.wait(TIMEOUT)

    w = start(
        m.reads,
        "<http://example.net/d> <http://example.net/num> 4 .",
        format="turtle",
    )
    try:
        assert_blocked(w)
    finally:
        gate.open()
    # The query ran before the write, so it does not see the new triple
    assert q.get().height == 3
    w.get()
    assert m.query(PLAIN_QUERY).height == 4


def test_writes_are_serialized():
    gate = Gate()
    m = model_with(gate)
    w1 = start(
        m.update,
        f"""
        INSERT {{ ?s <http://example.net/copy> ?o }}
        WHERE {{
            ?s <http://example.net/num> ?n .
            BIND(<{UDF}>(?n) AS ?o)
        }}
        """,
    )
    assert gate.entered.wait(TIMEOUT)
    w2 = start(
        m.reads,
        "<http://example.net/d> <http://example.net/num> 4 .",
        format="turtle",
    )
    try:
        assert_blocked(w2)
    finally:
        gate.open()
    w1.get()
    w2.get()
    # The update ran first and did not see the triple added by the second write
    assert m.query("SELECT ?s WHERE { ?s <http://example.net/copy> ?o }").height == 3
    assert m.query(PLAIN_QUERY).height == 4


def test_many_readers_and_a_writer():
    # No lock assertions here: many threads querying while another one writes must neither
    # deadlock nor see a model in the middle of a write.
    m = Model()
    m.reads(DATA, format="turtle")
    stop = threading.Event()
    errors = []
    heights = []

    def reader():
        try:
            while not stop.is_set():
                h = m.query(PLAIN_QUERY).height
                heights.append(h)
        except BaseException as e:
            errors.append(e)

    readers = [threading.Thread(target=reader, daemon=True) for _ in range(4)]
    for r in readers:
        r.start()
    n_writes = 20
    for i in range(n_writes):
        m.reads(
            f"<http://example.net/w{i}> <http://example.net/num> {i} .",
            format="turtle",
        )
    stop.set()
    for r in readers:
        r.join(TIMEOUT)
        assert not r.is_alive()
    assert errors == []
    assert all(3 <= h <= 3 + n_writes for h in heights)
    assert m.query(PLAIN_QUERY).height == 3 + n_writes


def test_add_graph_from_itself_raises():
    m = Model()
    m.reads(DATA, format="turtle")
    with pytest.raises(Exception, match="another Model"):
        m.add_graph(m, target_graph="http://example.net/g")


def test_add_graph_both_ways_does_not_deadlock():
    a = Model()
    a.reads(DATA, format="turtle")
    b = Model()
    b.reads(DATA, format="turtle")
    n = 10

    def add(x, y):
        for i in range(n):
            x.add_graph(y, target_graph=f"http://example.net/g{i}")

    r1 = start(add, a, b)
    r2 = start(add, b, a)
    r1.get()
    r2.get()
    assert a.size("http://example.net/g0") >= 3
    assert b.size("http://example.net/g0") >= 3
