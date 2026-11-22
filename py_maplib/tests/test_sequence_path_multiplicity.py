"""A sequence path is a join of its steps (SPARQL 1.1, 18.4): it has a solution for each
intermediate node, also when a step is an arbitrary length path (`+`, `*`, `?`), whose own
solutions are distinct. Compared with Oxigraph as multisets of rows, on random graphs."""
import random
from collections import Counter

import pyoxigraph as ox
import pytest
from maplib import Model

EX = "http://ex.org/"
PFX = f"PREFIX : <{EX}>\n"
ALL = " ".join(f":n{i}" for i in range(30))

SHAPES = [
    ":p/:q+",
    ":p/:q*",
    ":p/:q?",
    ":p+/:q",
    ":p?/:q",
    ":p/:q+/:r",
    ":p/(:q/:r)+",
    "^:q+/^:p",
    "^(:p/:q+)",
    ":p/^:q+",
    ":p/:q",  # no closure: unchanged path
    ":p/:q/:r",
    "(:p|:r)/:q+",
    ":p/(:q|:r)+",
    "!(:q)/:q+",
]


def graph(seed, literals):
    rng = random.Random(seed)
    nodes = [f"<{EX}n{i}>" for i in range(30)]
    lines = []
    for pred, n in [("p", 40), ("q", 45), ("r", 25)]:
        for _ in range(n):
            lines.append(f"{rng.choice(nodes)} <{EX}{pred}> {rng.choice(nodes)} .")
    if literals:
        for i in range(10):
            lines.append(f'{rng.choice(nodes)} <{EX}q> "v{i % 4}" .')
    return "\n".join(lines)


def term(t):
    return t.value if hasattr(t, "value") else str(t)


def rows_oxigraph(store, q, variables):
    return Counter(tuple(term(r[v]) for v in variables) for r in store.query(PFX + q))


def rows_maplib(m, q, variables):
    import json

    d = json.loads(m.query(PFX + q, return_json=True))
    return Counter(tuple(b[v]["value"] for v in variables) for b in d["results"]["bindings"])


def engines(seed, literals):
    data = graph(seed, literals)
    store = ox.Store()
    store.load(data.encode(), ox.RdfFormat.N_TRIPLES)
    m = Model()
    m.reads(data, format="ntriples")
    return store, m


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("literals", [False, True])
@pytest.mark.parametrize("path", SHAPES)
def test_unbound_multiset(seed, literals, path):
    store, m = engines(seed, literals)
    q = f"SELECT ?s ?o WHERE {{ ?s {path} ?o }}"
    assert rows_maplib(m, q, ["s", "o"]) == rows_oxigraph(store, q, ["s", "o"])


@pytest.mark.parametrize("seed", [1, 2])
@pytest.mark.parametrize("path", [":p/:q+", ":p/:q*", "^:q+/^:p", ":p+/:q"])
def test_bound_ends_and_same_variable(seed, path):
    store, m = engines(seed, False)
    for q, vs in [
        (f"SELECT ?o WHERE {{ :n3 {path} ?o }}", ["o"]),
        (f"SELECT ?s WHERE {{ ?s {path} :n7 }}", ["s"]),
        (f"SELECT ?x WHERE {{ ?x {path} ?x }}", ["x"]),
        (f"SELECT ?s ?o WHERE {{ VALUES ?s {{ :n1 :n2 :n2 }} ?s {path} ?o }}", ["s", "o"]),
        # Bound to many nodes: the closure over all nodes, filtered to them.
        (f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {ALL} }} ?s {path} ?o }}", ["s", "o"]),
        (f"SELECT ?s ?o WHERE {{ VALUES ?o {{ {ALL} }} ?s {path} ?o }}", ["s", "o"]),
    ]:
        assert rows_maplib(m, q, vs) == rows_oxigraph(store, q, vs), q


@pytest.mark.parametrize("seed", [1, 2])
def test_counts_and_distinct(seed):
    store, m = engines(seed, False)
    for path in [":p/:q+", ":p/:q*", ":p+/:q"]:
        for agg in ["COUNT(*)", "COUNT(DISTINCT ?o)"]:
            q = f"SELECT ({agg} AS ?c) WHERE {{ ?s {path} ?o }}"
            assert rows_maplib(m, q, ["c"]) == rows_oxigraph(store, q, ["c"]), q
        q = f"SELECT DISTINCT ?s ?o WHERE {{ ?s {path} ?o }}"
        assert rows_maplib(m, q, ["s", "o"]) == rows_oxigraph(store, q, ["s", "o"])


def test_sparqloscope_shape():
    # The shape of Sparqloscope's transitive-path-small-join-and-plus: lists that several
    # owners include at different positions, so an end is reached through several nodes.
    lines = []
    for li in range(20):
        nodes = [f"<{EX}l{li}_{k}>" for k in range(8)]
        lines += [f"{a} <{EX}rest> {b} ." for a, b in zip(nodes, nodes[1:])]
        for k in range(0, 8, 3):
            lines.append(f"<{EX}owner{li % 5}> <{EX}includes> {nodes[k]} .")
    data = "\n".join(lines)
    store = ox.Store()
    store.load(data.encode(), ox.RdfFormat.N_TRIPLES)
    m = Model()
    m.reads(data, format="ntriples")
    q = "SELECT (COUNT(*) AS ?c) { ?s :includes/:rest+ ?o }"
    assert rows_maplib(m, q, ["c"]) == rows_oxigraph(store, q, ["c"])


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("literals", [False, True])
@pytest.mark.parametrize("path", SHAPES)
def test_distinct_ends(seed, literals, path):
    """Right under DISTINCT only the distinct ends count, so a sequence with an arbitrary
    length step is evaluated as one path instead of the join of its steps (the fast path
    `path.sequence_distinct_ends`), with the same rows as Oxigraph."""
    import maplib.maplib as native

    store, m = engines(seed, literals)
    counters = hasattr(native, "_fast_path_counts")
    for q, vs in [
        (f"SELECT DISTINCT ?s ?o WHERE {{ ?s {path} ?o }}", ["s", "o"]),
        (f"SELECT DISTINCT * WHERE {{ ?s {path} ?o }}", ["s", "o"]),
        (f"SELECT DISTINCT ?s WHERE {{ ?s {path} ?o }}", ["s"]),
        (f"SELECT DISTINCT ?o WHERE {{ :n3 {path} ?o }}", ["o"]),
        (f"SELECT DISTINCT ?s WHERE {{ ?s {path} :n7 }}", ["s"]),
        (f"SELECT (COUNT(*) AS ?c) WHERE {{ SELECT DISTINCT ?s ?o WHERE {{ ?s {path} ?o }} }}", ["c"]),
    ]:
        if counters:
            native._reset_fast_path_counts()
        got = rows_maplib(m, q, vs)
        hits = native._fast_path_counts().get("path.sequence_distinct_ends", 0) if counters else 0
        assert got == rows_oxigraph(store, q, vs), q
        if counters:
            # Sequences without a closure and length ranges are paths of their own already.
            closure = any(c in path for c in "+*?")
            assert hits == (1 if closure else 0), (q, hits)
    # Not right under DISTINCT: the multiset of the join, also when DISTINCT is further out.
    for q, vs in [
        (f"SELECT DISTINCT ?s ?o ?x WHERE {{ ?s {path} ?o . ?s :p ?x }}", ["s", "o", "x"]),
        (f"SELECT (COUNT(*) AS ?c) WHERE {{ ?s {path} ?o }}", ["c"]),
    ]:
        assert rows_maplib(m, q, vs) == rows_oxigraph(store, q, vs), q
