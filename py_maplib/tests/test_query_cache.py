"""The query result cache of Model.query, invalidated per (named graph, predicate)."""

import threading

import polars as pl
import pytest
from maplib import Model, QueryCacheOptions

EX = "http://example.org/"
G1 = "urn:graph:g1"
G2 = "urn:graph:g2"

DATA = f"""
<{EX}a> <{EX}p> "1" .
<{EX}a> <{EX}q> "2" .
<{EX}a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}C> .
"""

Q_P = f"SELECT ?o WHERE {{ ?s <{EX}p> ?o }} ORDER BY ?o"
Q_Q = f"SELECT ?o WHERE {{ ?s <{EX}q> ?o }} ORDER BY ?o"
Q_ANY = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"


def model(**options) -> Model:
    m = Model()
    m.reads(DATA, format="ntriples")
    m.reads(DATA, format="ntriples", graph=G1)
    m.set_query_cache(QueryCacheOptions(**options))
    return m


def hits(m: Model) -> int:
    return m.query_cache_stats()["hits"]


def cached(m: Model, q: str, **kwargs) -> bool:
    """Runs the query and says whether it was answered from the cache. The answer must be
    the same as without the cache."""
    before = hits(m)
    res = m.query(q, **kwargs)
    hit = hits(m) > before
    uncached = m.query(q, cache=False, **kwargs)
    if isinstance(res, pl.DataFrame):
        # Without ORDER BY the row order is not fixed: compare the rows.
        if "ORDER BY" not in q:
            res, uncached = res.sort(res.columns), uncached.sort(uncached.columns)
        assert res.equals(uncached), f"{res}\n!=\n{uncached}"
    else:
        assert res == uncached
    return hit


def g1(q: str) -> str:
    return q.replace("WHERE {", f"WHERE {{ GRAPH <{G1}> {{", 1).replace("} ORDER", "} } ORDER", 1)


def test_disabled_by_default():
    m = Model()
    m.reads(DATA, format="ntriples")
    assert not m.get_query_cache().enabled
    m.query(Q_P)
    m.query(Q_P)
    assert m.query_cache_stats()["hits"] == 0
    assert m.query_cache_stats()["entries"] == 0


def test_options_round_trip_and_validation():
    o = QueryCacheOptions(max_entries=5, max_bytes=1000, max_entry_bytes=100, ttl=2.5, min_run_time=0.1, cache_virtualized=True)
    assert (o.enabled, o.max_entries, o.max_bytes, o.max_entry_bytes) == (True, 5, 1000, 100)
    assert (o.ttl, o.min_run_time, o.cache_virtualized) == (2.5, 0.1, True)
    m = Model()
    m.set_query_cache(o)
    assert m.get_query_cache() == o
    m.set_query_cache(True)
    assert m.get_query_cache() == QueryCacheOptions()
    m.set_query_cache(False)
    assert not m.get_query_cache().enabled
    m.set_query_cache(None)
    assert not m.get_query_cache().enabled
    with pytest.raises(Exception):
        QueryCacheOptions(ttl=-1)
    with pytest.raises(Exception):
        m.set_query_cache("yes")


def test_repeated_queries_hit():
    m = model()
    assert not cached(m, Q_P)
    assert cached(m, Q_P)
    assert cached(m, Q_P)
    s = m.query_cache_stats()
    assert s["entries"] >= 1 and s["inserts"] >= 1 and s["bytes"] > 0
    # Other arguments are other results.
    assert not cached(m, Q_P, graph=G1)
    assert cached(m, Q_P, graph=G1)
    assert not cached(m, Q_P, max_rows=10)
    assert not cached(m, Q_P, include_graphs=[G1])
    q = f"SELECT ?o WHERE {{ ?s ?pred ?o }} ORDER BY ?o"
    assert not cached(m, q, bindings={"pred": f"<{EX}p>"})
    assert cached(m, q, bindings={"pred": f"<{EX}p>"})
    assert not cached(m, q, bindings={"pred": f"<{EX}q>"})
    # The returned formats are made from one cached result.
    assert m.query(Q_P, return_json=True) == m.query(Q_P, return_json=True, cache=False)


def test_a_write_invalidates_only_its_predicate_and_graph():
    m = model()
    for q in [Q_P, Q_Q, g1(Q_P), Q_ANY]:
        m.query(q)
        assert cached(m, q)
    m.update(f'INSERT DATA {{ <{EX}b> <{EX}p> "3" }}')
    assert not cached(m, Q_P)  # the predicate written
    assert cached(m, Q_Q)  # another predicate of the graph
    assert cached(m, g1(Q_P))  # the predicate in another graph
    assert not cached(m, Q_ANY)  # any predicate of the graph
    assert m.query(Q_P)["o"].to_list() == ["1", "3"]


def test_graph_variables_see_writes_to_any_graph():
    m = model()
    q = f"SELECT ?g ?o WHERE {{ GRAPH ?g {{ ?s <{EX}p> ?o }} }} ORDER BY ?g ?o"
    m.query(q)
    m.update(f'INSERT DATA {{ <{EX}b> <{EX}q> "3" }}')
    assert cached(m, q)
    m.update(f'INSERT DATA {{ GRAPH <{G2}> {{ <{EX}b> <{EX}p> "3" }} }}')
    assert not cached(m, q)
    assert m.query(q).height == 2


def test_property_paths_and_types():
    m = model()
    path = f"SELECT * WHERE {{ ?s <{EX}p>|^<{EX}q> ?o }}"
    zero = f"SELECT * WHERE {{ ?s <{EX}p>* ?o }}"
    types = f"SELECT ?s WHERE {{ ?s a <{EX}C> }}"
    for q in [path, zero, types]:
        m.query(q)
    m.update(f'INSERT DATA {{ <{EX}b> <{EX}r> "3" }}')
    assert cached(m, path)
    assert not cached(m, zero)  # zero-length paths match every term of the graph
    assert cached(m, types)
    m.update(f"INSERT DATA {{ <{EX}b> a <{EX}D> }}")
    assert not cached(m, types)
    assert cached(m, path)
    m.update(f'DELETE DATA {{ <{EX}a> <{EX}q> "2" }}')
    assert not cached(m, path)


def write_cases():
    def read(m):
        m.reads(f'<{EX}b> <{EX}p> "3" .', format="ntriples")

    def read_other_graph(m):
        m.reads(f'<{EX}b> <{EX}p> "3" .', format="ntriples", graph=G2)

    def update_other_predicate(m):
        m.update(f'INSERT DATA {{ <{EX}b> <{EX}other> "3" }}')

    def delete_where(m):
        m.update(f"DELETE WHERE {{ ?s <{EX}p> ?o }}")

    def delete_nothing(m):
        m.update(f'DELETE DATA {{ <{EX}zz> <{EX}p> "none" }}')

    def delete_all_predicates(m):
        m.update("DELETE WHERE { ?s ?p ?o }")

    def insert_construct(m):
        m.insert(f'CONSTRUCT {{ ?s <{EX}p> "new" }} WHERE {{ ?s <{EX}q> ?o }}')

    def truncate(m):
        m.truncate_graph()

    def truncate_other(m):
        m.truncate_graph(G1)

    def detach(m):
        m.detach_graph()

    def map_triples(m):
        df = pl.DataFrame({"subject": [f"{EX}x"], "object": [f"{EX}o"]})
        m.map_triples(df, predicate=f"{EX}p")

    def map_triples_other(m):
        df = pl.DataFrame({"subject": [f"{EX}x"], "object": [f"{EX}o"]})
        m.map_triples(df, predicate=f"{EX}other")

    def clear(m):
        m.update("CLEAR DEFAULT")

    def drop_other(m):
        m.update(f"DROP GRAPH <{G1}>")

    def copy_into(m):
        m.update(f"COPY <{G1}> TO DEFAULT")

    def create_index(m):
        from maplib import IndexingOptions

        m.create_index(IndexingOptions(object_sort_all=True))

    # (write, whether Q_P must miss, whether Q_Q must miss)
    return {
        "read": (read, True, False),
        "read_other_graph": (read_other_graph, False, False),
        "update_other_predicate": (update_other_predicate, False, False),
        "delete_where": (delete_where, True, False),
        "delete_nothing": (delete_nothing, False, False),
        "delete_all_predicates": (delete_all_predicates, True, True),
        "insert_construct": (insert_construct, True, False),
        "truncate": (truncate, True, True),
        "truncate_other": (truncate_other, False, False),
        "detach": (detach, True, True),
        "map_triples": (map_triples, True, False),
        "map_triples_other": (map_triples_other, False, False),
        "clear": (clear, True, True),
        "drop_other": (drop_other, False, False),
        "copy_into": (copy_into, True, True),
        "create_index": (create_index, True, True),
    }


@pytest.mark.parametrize("name", list(write_cases()))
def test_writes(name):
    write, p_misses, q_misses = write_cases()[name]
    m = model()
    for q in [Q_P, Q_Q]:
        m.query(q)
        assert cached(m, q)
    write(m)
    # cached() checks that the answer equals the uncached one in every case.
    hit_p = cached(m, Q_P)
    hit_q = cached(m, Q_Q)
    if p_misses is not None:
        assert hit_p == (not p_misses)
        assert hit_q == (not q_misses)


def test_prefixes():
    m = model()
    q = "SELECT ?o WHERE { ?s ex:p ?o }"
    m.add_prefixes({"ex": EX})
    m.query(q)
    m.add_prefixes({"ex": EX})  # unchanged
    assert cached(m, q)
    m.add_prefixes({"ex": "http://example.com/"})
    assert not cached(m, q)
    assert m.query(q).height == 0


def test_volatile_queries_are_not_cached():
    m = model()
    for q in [
        "SELECT (NOW() AS ?n) WHERE {}",
        f"SELECT ?o WHERE {{ ?s <{EX}p> ?o FILTER(RAND() < 2) }}",
        "SELECT (STRUUID() AS ?u) WHERE {}",
        f"SELECT ?b WHERE {{ ?s <{EX}p> ?o BIND(BNODE() AS ?b) }}",
    ]:
        m.query(q)
        before = m.query_cache_stats()
        m.query(q)
        after = m.query_cache_stats()
        assert after["hits"] == before["hits"], q
        assert after["bypassed"] > before["bypassed"], q
    assert m.query_cache_stats()["entries"] == 0


def test_cache_false_and_debug_bypass():
    m = model()
    m.query(Q_P, cache=False)
    m.query(Q_P, cache=False)
    assert m.query_cache_stats()["entries"] == 0


def test_limits():
    m = model(max_entries=2)
    qs = [f"SELECT ?o WHERE {{ ?s <{EX}p> ?o }} LIMIT {i}" for i in range(1, 5)]
    for q in qs:
        m.query(q)
    s = m.query_cache_stats()
    assert s["entries"] == 2 and s["evictions"] >= 2
    assert cached(m, qs[-1])

    m = model(max_entry_bytes=1)
    m.query(Q_P)
    assert m.query_cache_stats()["entries"] == 0

    m = model(max_bytes=1)
    m.query(Q_P)
    assert m.query_cache_stats()["entries"] == 0

    m = model(min_run_time=3600.0)
    m.query(Q_P)
    assert m.query_cache_stats()["entries"] == 0


def test_ttl():
    import time

    m = model(ttl=0.05)
    m.query(Q_P)
    assert cached(m, Q_P)
    time.sleep(0.1)
    assert not cached(m, Q_P)
    assert m.query_cache_stats()["expirations"] >= 1


def test_shrinking_and_disabling_at_runtime():
    m = model()
    for i in range(5):
        m.query(f"SELECT ?o WHERE {{ ?s <{EX}p> ?o }} LIMIT {i + 1}")
    m.set_query_cache(QueryCacheOptions(max_entries=2))
    assert m.query_cache_stats()["entries"] == 2
    m.clear_query_cache()
    assert m.query_cache_stats()["entries"] == 0
    m.query(Q_P)
    m.set_query_cache(False)
    assert m.query_cache_stats()["entries"] == 0
    m.query(Q_P)
    assert m.query_cache_stats()["entries"] == 0


def test_concurrent_readers_see_every_completed_write():
    m = Model()
    m.set_query_cache(True)
    q = f"SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}p> ?o }}"
    other = f"SELECT (COUNT(*) AS ?n) WHERE {{ ?s <{EX}q> ?o }}"
    m.reads(f'<{EX}q0> <{EX}q> "x" .', format="ntriples")
    written = [0]
    errors = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            at_least = written[0]
            n = m.query(q)["n"][0]
            if n < at_least:
                errors.append((n, at_least))
            assert m.query(other)["n"][0] == 1

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    try:
        for i in range(1, 30):
            m.update(f'INSERT DATA {{ <{EX}s{i}> <{EX}p> "{i}" }}')
            written[0] = i
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert not errors, errors[:5]
    assert m.query(q)["n"][0] == 29
    assert m.query_cache_stats()["hits"] > 0
