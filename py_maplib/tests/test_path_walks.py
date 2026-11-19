"""Arbitrary length paths (`+`, `*`, `?`) from a fixed node, a few nodes and many nodes, and
unbound. Few starts are followed on dictionary codes through the sparse indexes of the triple
tables, many starts and unbound paths by a depth first search over keyed tables, and a walk on
codes that runs past its budget falls back to keying. Compared with Oxigraph as multisets of
rows, on random graphs with cycles, inverse steps, named graphs, blank nodes and literals, with
and without the object index, and with tables of several segments."""
import functools
import json
import random
from collections import Counter

import pyoxigraph as ox
import pytest
from maplib import Model, IndexingOptions

EX = "http://ex.org/"
PFX = f"PREFIX : <{EX}>\n"
N_NODES = 40

PATHS = [
    ":p+",
    ":p*",
    ":p?",
    "^:p+",
    "^:p*",
    "(^:p)+",
    "(:p|:q)+",
    "(:p|^:q)*",
    "(:p/:q)+",
    "(:p/:q)*",
    ":p+|:q",
    "^(:p|:q)+",
    ":q+",
    ":missing+",
    ":missing*",
]


def graph_lines(seed, variant):
    rng = random.Random(seed)
    nodes = [f"<{EX}n{i}>" for i in range(N_NODES)]
    if variant == "blanks":
        nodes += [f"_:b{i}" for i in range(10)]
    lines = []
    # Chains with branches and cycles for :p, random :q and a few :r edges.
    for i in range(len(nodes) - 1):
        if rng.random() < 0.8:
            lines.append(f"{nodes[i]} <{EX}p> {nodes[i + 1]}")
    for _ in range(12):
        lines.append(f"{rng.choice(nodes)} <{EX}p> {rng.choice(nodes)}")
    for _ in range(30):
        lines.append(f"{rng.choice(nodes)} <{EX}q> {rng.choice(nodes)}")
    for _ in range(4):
        lines.append(f"{rng.choice(nodes)} <{EX}r> {rng.choice(nodes)}")
    if variant == "literals":
        for i in range(6):
            lines.append(f'{rng.choice(nodes)} <{EX}p> "v{i % 3}"')
            lines.append(f'{rng.choice(nodes)} <{EX}q> "{i}"^^<http://www.w3.org/2001/XMLSchema#integer>')
    return lines


def nquads(lines, graph):
    g = f" <{graph}>" if graph else ""
    return "\n".join(f"{line}{g} ." for line in lines)


@functools.lru_cache(maxsize=None)
def engines(seed, variant, layout):
    """Oxigraph and maplib with the same triples, in the default graph and in two named graphs
    that split them. `layout` is "default", "object_sort" (object index on every table) or
    "segments" (the triples read in two parts, giving tables of several segments)."""
    lines = graph_lines(seed, variant)
    half = len(lines) // 2
    parts = [
        nquads(lines, None),
        nquads(lines[:half], f"{EX}g1"),
        nquads(lines[half:], f"{EX}g2"),
    ]
    store = ox.Store()
    for part in parts:
        store.load(part.encode(), ox.RdfFormat.N_QUADS)
    kwargs = {}
    if layout == "object_sort":
        kwargs["indexing_options"] = IndexingOptions(object_sort_all=True)
    m = Model(**kwargs)
    if layout == "segments":
        first = random.Random(seed).sample(range(len(lines)), half)
        rest = [lines[i] for i in range(len(lines)) if i not in set(first)]
        m.reads(nquads([lines[i] for i in first], None), format="nquads")
        m.reads(nquads(rest, None), format="nquads")
    else:
        m.reads(parts[0], format="nquads")
    for part in parts[1:]:
        m.reads(part, format="nquads")
    return store, m


def term(t):
    if isinstance(t, ox.BlankNode):
        return "_"
    return t.value


def rows_oxigraph(store, q, variables):
    return Counter(
        tuple(term(r[v]) if r[v] is not None else None for v in variables)
        for r in store.query(PFX + q)
    )


def rows_maplib(m, q, variables):
    d = json.loads(m.query(PFX + q, return_json=True))
    out = Counter()
    for b in d["results"]["bindings"]:
        row = []
        for v in variables:
            x = b.get(v)
            row.append(None if x is None else "_" if x["type"] == "bnode" else x["value"])
        out[tuple(row)] += 1
    return out


def starts(k, seed):
    return " ".join(f":n{i}" for i in random.Random(seed).sample(range(N_NODES + 5), k))


QUERIES = {
    "fixed_subject": (lambda p, s: f"SELECT ?o WHERE {{ :n0 {p} ?o }}", ["o"]),
    "fixed_subject_mid": (lambda p, s: f"SELECT ?o WHERE {{ :n17 {p} ?o }}", ["o"]),
    "fixed_object": (lambda p, s: f"SELECT ?s WHERE {{ ?s {p} :n5 }}", ["s"]),
    "both_fixed": (lambda p, s: f"SELECT (COUNT(*) AS ?c) WHERE {{ :n1 {p} :n9 }}", ["c"]),
    "missing_subject": (lambda p, s: f"SELECT ?o WHERE {{ :nope {p} ?o }}", ["o"]),
    "few_values": (
        lambda p, s: f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {starts(5, s)} }} ?s {p} ?o }}",
        ["s", "o"],
    ),
    "few_values_object": (
        lambda p, s: f"SELECT ?s ?o WHERE {{ VALUES ?o {{ {starts(4, s)} }} ?s {p} ?o }}",
        ["s", "o"],
    ),
    "few_join": (lambda p, s: f"SELECT ?s ?x ?o WHERE {{ ?s :r ?x . ?x {p} ?o }}", ["s", "x", "o"]),
    "few_join_object": (
        lambda p, s: f"SELECT ?s ?x ?o WHERE {{ ?x :r ?o . ?s {p} ?x }}",
        ["s", "x", "o"],
    ),
    "many_values": (
        lambda p, s: f"SELECT ?s ?o WHERE {{ VALUES ?s {{ {starts(N_NODES, s)} }} ?s {p} ?o }}",
        ["s", "o"],
    ),
    "many_join": (lambda p, s: f"SELECT ?s ?x ?o WHERE {{ ?s :q ?x . ?x {p} ?o }}", ["s", "x", "o"]),
    "unbound": (lambda p, s: f"SELECT ?s ?o WHERE {{ ?s {p} ?o }}", ["s", "o"]),
    "same_variable": (lambda p, s: f"SELECT ?x WHERE {{ ?x {p} ?x }}", ["x"]),
    "named_graph": (lambda p, s: f"SELECT ?o WHERE {{ GRAPH :g1 {{ :n0 {p} ?o }} }}", ["o"]),
    "graph_variable": (
        lambda p, s: f"SELECT ?g ?o WHERE {{ GRAPH ?g {{ :n2 {p} ?o }} }}",
        ["g", "o"],
    ),
    "from_graphs": (
        lambda p, s: f"SELECT ?o FROM :g1 FROM :g2 WHERE {{ :n0 {p} ?o }}",
        ["o"],
    ),
}


# The paths that match with zero length, and the constant start of the queries whose start may
# not be a node of the graph queried.
ZERO_LENGTH = {p for p in PATHS if p.endswith("*") or p.endswith("?")}
CONSTANT_NOT_IN_GRAPH = {"missing_subject": f"{EX}nope", "named_graph": f"{EX}n0"}


@pytest.mark.parametrize("layout", ["default", "object_sort", "segments"])
@pytest.mark.parametrize("variant", ["iris", "blanks", "literals"])
@pytest.mark.parametrize("kind", list(QUERIES))
def test_paths_match_oxigraph(kind, variant, layout):
    if variant == "blanks" and layout == "segments":
        pytest.skip("blank node labels are scoped to each read, so the two reads differ")
    make, variables = QUERIES[kind]
    failures = []
    for seed in [1, 2, 3]:
        store, m = engines(seed, variant, layout)
        for path in PATHS:
            q = make(path, seed)
            expected = rows_oxigraph(store, q, variables)
            if kind in CONSTANT_NOT_IN_GRAPH and path in ZERO_LENGTH:
                # A zero-length path from a constant term matches the term, also when it is not
                # a node of the (named) graph, as maplib reads SPARQL 1.1 section 18.4; Oxigraph
                # gives no solution then.
                expected[(CONSTANT_NOT_IN_GRAPH[kind],)] = 1
            actual = rows_maplib(m, q, variables)
            if actual != expected:
                failures.append(
                    f"seed {seed} {q}\n  missing {expected - actual}\n  extra {actual - expected}"
                )
    assert not failures, "\n".join(failures[:5])


@pytest.mark.parametrize("layout", ["default", "object_sort"])
@pytest.mark.parametrize("length", [10, 3000])
def test_long_chain_from_one_node(layout, length):
    """A chain longer than the budget of a walk on codes falls back to keying the tables."""
    kwargs = {}
    if layout == "object_sort":
        kwargs["indexing_options"] = IndexingOptions(object_sort_all=True)
    m = Model(**kwargs)
    m.reads(
        "\n".join(f"<{EX}c{i}> <{EX}p> <{EX}c{i + 1}> ." for i in range(length)),
        format="ntriples",
    )

    def count(q):
        d = json.loads(m.query(PFX + q, return_json=True))
        return int(d["results"]["bindings"][0]["c"]["value"])

    assert count("SELECT (COUNT(*) AS ?c) WHERE { :c0 :p+ ?o }") == length
    assert count("SELECT (COUNT(*) AS ?c) WHERE { :c0 :p* ?o }") == length + 1
    assert count(f"SELECT (COUNT(*) AS ?c) WHERE {{ ?s :p+ :c{length} }}") == length
    assert count(f"SELECT (COUNT(*) AS ?c) WHERE {{ :c0 :p+ :c{length} }}") == 1
    assert count("SELECT (COUNT(*) AS ?c) WHERE { ?s :p+ ?o }") == length * (length + 1) // 2


def test_walk_after_insert_and_delete():
    """New segments from an insert and rows removed by a delete are seen by a walk on codes."""
    m = Model()
    m.reads("\n".join(f"<{EX}c{i}> <{EX}p> <{EX}c{i + 1}> ." for i in range(20)), format="ntriples")
    q = PFX + "SELECT ?o WHERE { :c0 :p+ ?o }"

    def objects():
        return {r["o"]["value"] for r in json.loads(m.query(q, return_json=True))["results"]["bindings"]}

    assert len(objects()) == 20
    m.update(PFX + "INSERT DATA { :c20 :p :c21 . :c21 :p :c0 }")
    assert objects() == {f"{EX}c{i}" for i in range(22)}
    m.update(PFX + "DELETE DATA { :c10 :p :c11 }")
    assert objects() == {f"{EX}c{i}" for i in range(1, 11)}
