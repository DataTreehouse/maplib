"""maplib's graph algorithms checked against their definitions on random graphs.

No standard defines connected components, bridges or articulation points for a query language, so
these tests check maplib:component, maplib:bridge and maplib:articulationPoint against the
textbook definitions, computed here by brute force:

* a connected component is a maximal set of nodes joined by paths, the edges taken both ways;
* a strongly connected component is a maximal set of nodes with directed paths between all of them;
* a bridge is an edge whose removal increases the number of connected components (an edge with a
  parallel edge, such as a -> b with b -> a, never is), and its cutOff is the size of the smaller side;
* an articulation point is a node whose removal increases the number of connected components, and
  its cutOff is the number of the other nodes of its component not in the largest remaining part.

The component numbers must follow decreasing size, as documented.
"""

import random
from collections import Counter, defaultdict

import pytest

from maplib import Model

EX = "http://example.org/"
PREFIXES = f"""PREFIX ex: <{EX}>
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
"""


def random_graph(seed: int) -> list[tuple[str, str]]:
    """A random directed graph without self-loops: a few dense clusters joined by sparse edges and
    tails, so that it has bridges, articulation points, cycles and several components."""
    rng = random.Random(seed)
    n = rng.randint(2, 40)
    nodes = [f"n{i}" for i in range(n)]
    edges = set()
    for _ in range(rng.randint(1, 2 * n)):
        a, b = rng.sample(nodes, 2)
        # mostly short edges, so that the graph has long chains and clusters
        if rng.random() < 0.8:
            j = nodes.index(a)
            b = nodes[min(n - 1, j + rng.randint(1, 3))]
            if b == a:
                continue
        edges.add((a, b))
    return sorted(edges)


def model(edges: list[tuple[str, str]]) -> Model:
    m = Model()
    m.reads("\n".join(f"<{EX}{a}> <{EX}e> <{EX}{b}> ." for a, b in edges), format="ntriples")
    return m


def local(iri: str) -> str:
    return iri.strip("<>").removeprefix(EX)


def components(nodes: set[str], edges: list[tuple[str, str]]) -> list[set[str]]:
    adj = defaultdict(set)
    for a, b in edges:
        adj[a].add(b)
        adj[b].add(a)
    seen: set[str] = set()
    out = []
    for n in sorted(nodes):
        if n in seen:
            continue
        part, stack = {n}, [n]
        seen.add(n)
        while stack:
            for y in adj[stack.pop()] - seen:
                seen.add(y)
                part.add(y)
                stack.append(y)
        out.append(part)
    return out


def strong_components(nodes: set[str], edges: list[tuple[str, str]]) -> list[set[str]]:
    """Mutual reachability, by a search from every node: slow, and plainly the definition."""
    adj = defaultdict(set)
    for a, b in edges:
        adj[a].add(b)

    def reach(n: str) -> set[str]:
        seen, stack = {n}, [n]
        while stack:
            for y in adj[stack.pop()] - seen:
                seen.add(y)
                stack.append(y)
        return seen

    reachable = {n: reach(n) for n in nodes}
    out, done = [], set()
    for n in sorted(nodes):
        if n not in done:
            part = {m for m in reachable[n] if n in reachable[m]}
            done |= part
            out.append(part)
    return out


def nodes_of(edges):
    return {n for e in edges for n in e}


SEEDS = range(60)


@pytest.mark.parametrize("seed", SEEDS)
def test_components_follow_the_definition(seed):
    edges = random_graph(seed)
    rows = model(edges).query(PREFIXES + """SELECT ?n ?id ?size WHERE {
        ?n maplib:component [ maplib:predicate ex:e ; maplib:componentId ?id ; maplib:componentSize ?size ]
    }""")
    got = defaultdict(set)
    for r in rows.iter_rows(named=True):
        got[r["id"]].add(local(r["n"]))
    expected = components(nodes_of(edges), edges)
    assert sorted(map(frozenset, got.values()), key=sorted) == sorted(map(frozenset, expected), key=sorted)
    sizes = [len(got[i]) for i in sorted(got)]
    assert sizes == sorted(sizes, reverse=True), "componentId 1 is the largest, then by decreasing size"
    for r in rows.iter_rows(named=True):
        assert r["size"] == len(got[r["id"]])


@pytest.mark.parametrize("seed", SEEDS)
def test_strong_components_follow_the_definition(seed):
    edges = random_graph(seed)
    rows = model(edges).query(PREFIXES + """SELECT ?n ?id WHERE {
        ?n maplib:component [ maplib:predicate ex:e ; maplib:strong true ; maplib:componentId ?id ]
    }""")
    got = defaultdict(set)
    for r in rows.iter_rows(named=True):
        got[r["id"]].add(local(r["n"]))
    expected = strong_components(nodes_of(edges), edges)
    assert sorted(map(frozenset, got.values()), key=sorted) == sorted(map(frozenset, expected), key=sorted)


@pytest.mark.parametrize("seed", SEEDS)
def test_bridges_follow_the_definition(seed):
    edges = random_graph(seed)
    rows = model(edges).query(PREFIXES + """SELECT ?a ?b ?cut WHERE {
        ?a maplib:bridge ?o .
        ?o maplib:object ?b ; maplib:predicate ex:e ; maplib:cutOff ?cut .
    }""")
    got = {frozenset((local(r["a"]), local(r["b"]))): r["cut"] for r in rows.iter_rows(named=True)}
    assert len(got) == len(rows), "one row per bridge"

    nodes = nodes_of(edges)
    base = len(components(nodes, edges))
    # the edges as an undirected multigraph: a -> b and b -> a are parallel
    multiplicity = Counter(frozenset(e) for e in edges)
    expected = {}
    for pair, k in multiplicity.items():
        rest = [e for e in edges if frozenset(e) != pair] if k == 1 else edges
        parts = components(nodes, rest)
        if len(parts) > base:
            a, b = tuple(pair)
            sides = [p for p in parts if a in p or b in p]
            expected[pair] = min(len(s) for s in sides)
    assert got == expected


@pytest.mark.parametrize("seed", SEEDS)
def test_articulation_points_follow_the_definition(seed):
    edges = random_graph(seed)
    rows = model(edges).query(PREFIXES + """SELECT ?n ?cut WHERE {
        ?n maplib:articulationPoint [ maplib:predicate ex:e ; maplib:cutOff ?cut ]
    }""")
    got = {local(r["n"]): r["cut"] for r in rows.iter_rows(named=True)}

    nodes = nodes_of(edges)
    base = len(components(nodes, edges))
    expected = {}
    for n in sorted(nodes):
        rest_nodes = nodes - {n}
        rest = [e for e in edges if n not in e]
        parts = components(rest_nodes, rest)
        # removing a node of a component of its own lowers the count by one: not a split
        if len(parts) > base:
            own = next(p for p in components(nodes, edges) if n in p) - {n}
            inside = [p for p in parts if p <= own]
            expected[n] = len(own) - max(len(p) for p in inside)
    assert got == expected


def test_the_random_graphs_have_what_the_tests_check():
    """The seeds give graphs with bridges, articulation points, cycles and several components."""
    bridges = points = cycles = split = 0
    for seed in SEEDS:
        edges = random_graph(seed)
        nodes = nodes_of(edges)
        parts = components(nodes, edges)
        split += len(parts) > 1
        cycles += any(len(p) > 1 for p in strong_components(nodes, edges))
        base = len(parts)
        bridges += any(len(components(nodes, [e for e in edges if e != x])) > base for x in edges)
        points += any(len(components(nodes - {n}, [e for e in edges if n not in e])) > base for n in nodes)
    assert min(bridges, points, cycles, split) >= 10, (bridges, points, cycles, split)
