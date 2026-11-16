"""The graph algorithm magic predicates maplib:component, maplib:bridge and
maplib:articulationPoint, checked against brute-force references on random graphs."""

import random

import polars as pl
import pytest
from maplib import Model

PREFIXES = """
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
PREFIX ex: <http://example.org/>
"""


def ex(n) -> str:
    return f"http://example.org/{n}"


def model_of(edges, extra: str = "") -> Model:
    """A model with triples ex:a ex:p ex:b for (a, p, b) in edges."""
    ttl = "".join(f"<{ex(a)}> <{ex(p)}> <{ex(b)}> .\n" for a, p, b in edges)
    m = Model()
    m.reads(ttl + extra, format="ntriples")
    return m


def strip(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [pl.col(c).str.strip_chars("<>") for c, t in df.schema.items() if t == pl.String]
    )


def components_of(nodes, edges, removed=()):
    """Undirected components as a set of frozensets."""
    adj = {u: set() for u in nodes if u not in removed}
    for a, b in edges:
        if a in adj and b in adj:
            adj[a].add(b)
            adj[b].add(a)
    seen, out = set(), set()
    for u in adj:
        if u in seen:
            continue
        stack, comp = [u], set()
        seen.add(u)
        while stack:
            x = stack.pop()
            comp.add(x)
            for y in adj[x]:
                if y not in seen:
                    seen.add(y)
                    stack.append(y)
        out.add(frozenset(comp))
    return out


def reach(adj, u):
    seen, stack = {u}, [u]
    while stack:
        x = stack.pop()
        for y in adj.get(x, ()):
            if y not in seen:
                seen.add(y)
                stack.append(y)
    return seen


def random_edges(rng, n, m):
    return [(rng.randrange(n), rng.randrange(n)) for _ in range(m)]


def as_sets(df, node="n", comp="id"):
    groups = {}
    for n, c in df.select(node, comp).iter_rows():
        groups.setdefault(c, set()).add(n)
    return {frozenset(g) for g in groups.values()}


@pytest.mark.parametrize("seed", range(6))
def test_components_match_reference(seed):
    rng = random.Random(seed)
    n = rng.randrange(5, 60)
    edges = random_edges(rng, n, rng.randrange(0, 2 * n))
    m = model_of([(a, "p", b) for a, b in edges])
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?n ?id ?size WHERE {
  ?n maplib:component ?c .
  ?c maplib:predicate ex:p ; maplib:componentId ?id ; maplib:componentSize ?size .
}"""
        )
    )
    nodes = {a for e in edges for a in e}
    expected = components_of(nodes, edges)
    got = {frozenset(int(x.rsplit("/", 1)[1]) for x in g) for g in as_sets(df)}
    assert got == expected
    # numbered from 1 by decreasing size
    sizes = df.unique("id").sort("id")["size"].to_list()
    assert sizes == sorted(sizes, reverse=True)
    assert df.unique("id").sort("id")["id"].to_list() == list(range(1, len(expected) + 1))
    for g in as_sets(df):
        sizes_in = df.filter(pl.col("n").is_in(list(g)))["size"].unique().to_list()
        assert sizes_in == [len(g)]


def test_avoid_splits_and_removes_nodes():
    # 0 - 1 - 2 - 3, avoid 1
    m = model_of([(0, "p", 1), (1, "p", 2), (2, "p", 3)])
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?n ?id ?size WHERE {
  ?n maplib:component ?c .
  ?c maplib:predicate ex:p ; maplib:avoid ex:1 ; maplib:componentId ?id ; maplib:componentSize ?size .
} ORDER BY ?n"""
        )
    )
    assert df["n"].to_list() == [ex(0), ex(2), ex(3)]
    assert df["id"].to_list() == [2, 1, 1]
    assert df["size"].to_list() == [1, 2, 2]


def test_avoid_from_a_subquery_and_a_pattern():
    # switches as nodes: a - s1 - b - s2 - c, s1 is open
    m = model_of(
        [("a", "p", "s1"), ("s1", "p", "b"), ("b", "p", "s2"), ("s2", "p", "c")],
        extra=f'<{ex("s1")}> <{ex("open")}> "true" .\n<{ex("s2")}> <{ex("open")}> "false" .\n',
    )
    for avoid in [
        '{ SELECT ?x WHERE { ?x ex:open ?o FILTER(STR(?o) = "true") } }',
    ]:
        df = strip(
            m.query(
                PREFIXES
                + f"""
SELECT ?n ?id WHERE {{
  {avoid}
  ?n maplib:component ?c .
  ?c maplib:predicate ex:p ; maplib:avoid ?x ; maplib:componentId ?id .
}} ORDER BY ?n"""
            )
        )
        assert dict(df.iter_rows()) == {ex("a"): 2, ex("b"): 1, ex("s2"): 1, ex("c"): 1}
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?n ?id WHERE {
  ?x ex:open "true" .
  ?n maplib:component ?c .
  ?c maplib:predicate ex:p ; maplib:avoid ?x ; maplib:componentId ?id .
} ORDER BY ?n"""
        )
    )
    assert dict(df.iter_rows()) == {ex("a"): 2, ex("b"): 1, ex("s2"): 1, ex("c"): 1}


def test_restriction_keeps_nodes_without_edges():
    m = model_of(
        [(0, "p", 1)],
        extra="".join(
            f"<{ex(i)}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{ex('Node')}> .\n"
            for i in [0, 1, 7, 8]
        ),
    )
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?n ?id ?size WHERE {
  ?n a ex:Node .
  ?n maplib:component ?c .
  ?c maplib:predicate ex:p ; maplib:componentId ?id ; maplib:componentSize ?size .
} ORDER BY ?id ?n"""
        )
    )
    assert df["n"].to_list() == [ex(0), ex(1), ex(7), ex(8)]
    assert df["id"].to_list() == [1, 1, 2, 3]
    assert df["size"].to_list() == [2, 2, 1, 1]


def test_constant_subject():
    m = model_of([(0, "p", 1), (2, "p", 3), (3, "p", 4)])
    df = m.query(
        PREFIXES
        + """
SELECT ?id ?size WHERE {
  ex:3 maplib:component ?c .
  ?c maplib:predicate ex:p ; maplib:componentId ?id ; maplib:componentSize ?size .
}"""
    )
    assert df.rows() == [(1, 3)]


@pytest.mark.parametrize("seed", range(6))
def test_strong_components_match_reference(seed):
    rng = random.Random(100 + seed)
    n = rng.randrange(5, 40)
    edges = random_edges(rng, n, rng.randrange(n, 3 * n))
    m = model_of([(a, "p", b) for a, b in edges])
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?n ?id WHERE {
  ?n maplib:component ?c .
  ?c maplib:predicate ex:p ; maplib:strong true ; maplib:componentId ?id .
}"""
        )
    )
    adj = {}
    for a, b in edges:
        adj.setdefault(a, set()).add(b)
    nodes = {a for e in edges for a in e}
    reached = {u: reach(adj, u) for u in nodes}
    expected = {frozenset(v for v in nodes if v in reached[u] and u in reached[v]) for u in nodes}
    got = {frozenset(int(x.rsplit("/", 1)[1]) for x in g) for g in as_sets(df)}
    assert got == expected


@pytest.mark.parametrize("seed", range(8))
def test_bridges_and_articulation_points_match_reference(seed):
    rng = random.Random(200 + seed)
    n = rng.randrange(5, 30)
    edges = [(a, b) for a, b in random_edges(rng, n, rng.randrange(n // 2, 2 * n)) if a != b]
    if not edges:
        return
    m = model_of([(a, "p", b) for a, b in edges])
    nodes = {a for e in edges for a in e}
    base = len(components_of(nodes, edges))

    expected_bridges = set()
    for i, (a, b) in enumerate(edges):
        rest = edges[:i] + edges[i + 1 :]
        if len(components_of(nodes, rest)) > base:
            expected_bridges.add((a, b))
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?a ?b ?cut WHERE {
  ?a maplib:bridge ?o .
  ?o maplib:object ?b ; maplib:predicate ex:p ; maplib:cutOff ?cut ; maplib:edgePredicate ?p .
}"""
        )
    )
    got = {(int(a.rsplit("/", 1)[1]), int(b.rsplit("/", 1)[1])) for a, b, _ in df.iter_rows()}
    assert got == expected_bridges
    for a, b, cut in df.iter_rows():
        a, b = int(a.rsplit("/", 1)[1]), int(b.rsplit("/", 1)[1])
        i = edges.index((a, b))
        parts = components_of(nodes, edges[:i] + edges[i + 1 :])
        side = [len(p) for p in parts if a in p or b in p]
        assert cut == min(side)

    expected_ap = {}
    comps = components_of(nodes, edges)
    for u in nodes:
        after = components_of(nodes, edges, removed={u})
        comp_u = next(c for c in comps if u in c)
        pieces = [p for p in after if p <= comp_u]
        if len(pieces) > 1:
            expected_ap[u] = len(comp_u) - 1 - max(len(p) for p in pieces)
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?n ?cut WHERE {
  ?n maplib:articulationPoint ?o .
  ?o maplib:predicate ex:p ; maplib:cutOff ?cut .
}"""
        )
    )
    got = {int(n.rsplit("/", 1)[1]): cut for n, cut in df.iter_rows()}
    assert got == expected_ap


def test_parallel_edges_are_not_bridges():
    m = model_of([(0, "p", 1), (0, "q", 1), (1, "p", 2)])
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?a ?b ?p WHERE {
  ?a maplib:bridge ?o .
  ?o maplib:object ?b ; maplib:predicate ex:p, ex:q ; maplib:edgePredicate ?p .
}"""
        )
    )
    assert df.rows() == [(ex(1), ex(2), ex("p"))]


def test_pattern_edges_between_derived_nodes():
    # lines between substations, given through their ends; L2 and L3 are parallel
    triples = [
        ("L1", "from", "A"), ("L1", "to", "B"),
        ("L2", "from", "B"), ("L2", "to", "C"),
        ("L3", "from", "B"), ("L3", "to", "C"),
        ("L4", "from", "C"), ("L4", "to", "D"),
    ]
    m = model_of(triples)
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?line ?a ?b ?cut WHERE {
  { SELECT ?line ?a ?b WHERE { ?line ex:from ?a ; ex:to ?b } }
  ?a maplib:bridge ?o .
  ?o maplib:object ?b ; maplib:edgeSource ?a ; maplib:edgeTarget ?b ; maplib:cutOff ?cut .
} ORDER BY ?line"""
        )
    )
    assert df.rows() == [(ex("L1"), ex("A"), ex("B"), 1), (ex("L4"), ex("C"), ex("D"), 1)]
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?s ?cut WHERE {
  { SELECT ?a ?b WHERE { ?line ex:from ?a ; ex:to ?b } }
  ?s maplib:articulationPoint ?o .
  ?o maplib:edgeSource ?a ; maplib:edgeTarget ?b ; maplib:cutOff ?cut .
} ORDER BY ?s"""
        )
    )
    assert df.rows() == [(ex("B"), 1), (ex("C"), 1)]
    # what B cuts off: the components without B
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?s ?id WHERE {
  { SELECT ?a ?b WHERE { ?line ex:from ?a ; ex:to ?b } }
  ?s maplib:component ?o .
  ?o maplib:edgeSource ?a ; maplib:edgeTarget ?b ; maplib:avoid ex:B ; maplib:componentId ?id .
} ORDER BY ?s"""
        )
    )
    assert df.rows() == [(ex("A"), 2), (ex("C"), 1), (ex("D"), 1)]


def test_errors():
    m = model_of([(0, "p", 1)])
    with pytest.raises(Exception, match="needs its edges"):
        m.query(PREFIXES + "SELECT ?n WHERE { ?n maplib:component ?c . ?c maplib:componentId ?i }")
    with pytest.raises(Exception, match="unknown option maplib:cutOff of maplib:component"):
        m.query(
            PREFIXES
            + "SELECT ?n WHERE { ?n maplib:component ?c . ?c maplib:predicate ex:p ; maplib:cutOff ?x }"
        )
    with pytest.raises(Exception, match="needs maplib:object"):
        m.query(PREFIXES + "SELECT ?n WHERE { ?n maplib:bridge ?c . ?c maplib:predicate ex:p }")
    with pytest.raises(Exception, match="bound together"):
        m.query(
            PREFIXES
            + "SELECT ?n WHERE { ?n maplib:component ?c . ?c maplib:edgeSource ?a ; maplib:edgeTarget ?b }"
        )


def test_count_nodes_weights_sizes_and_cut_off():
    # substations S1 - c1 - S2 - c2 - S3 with corridors c1, c2 as nodes; only substations count
    m = model_of(
        [("S1", "at", "c1"), ("S2", "at", "c1"), ("S2", "at", "c2"), ("S3", "at", "c2")],
        extra="".join(
            f"<{ex(s)}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{ex('Substation')}> .\n"
            for s in ["S1", "S2", "S3"]
        ),
    )
    df = strip(
        m.query(
            PREFIXES
            + """
SELECT ?n ?cut WHERE {
  ?s a ex:Substation .
  ?n maplib:articulationPoint ?o .
  ?o maplib:predicate ex:at ; maplib:countNodes ?s ; maplib:cutOff ?cut .
} ORDER BY ?n"""
        )
    )
    # S2 cuts off one substation; so do the corridors, each the only link of an end substation
    assert df.rows() == [(ex("S2"), 1), (ex("c1"), 1), (ex("c2"), 1)]
    df = m.query(
        PREFIXES
        + """
SELECT DISTINCT ?size WHERE {
  ?s a ex:Substation .
  ?n maplib:component ?o .
  ?o maplib:predicate ex:at ; maplib:countNodes ?s ; maplib:componentSize ?size .
}"""
    )
    assert df.rows() == [(3,)]


@pytest.mark.parametrize("with_data", [False, True])
def test_pattern_edges_with_iris_not_in_the_model(with_data):
    # a - b - c given in VALUES, and the same path made with BIND(IRI(...)): b is the articulation
    # point, both edges are bridges; with and without other data in the model
    m = Model()
    if with_data:
        m.reads(f"<{ex('x')}> <{ex('p')}> <{ex('y')}> .", format="ntriples")
    values = "VALUES (?a ?b) { (<urn:t:a> <urn:t:b>) (<urn:t:b> <urn:t:c>) }"
    bind = """{ SELECT ?a ?b WHERE { VALUES ?s { "a" "b" }
        BIND(IRI(CONCAT("urn:t:", ?s)) AS ?a)
        BIND(IRI(CONCAT("urn:t:", IF(?s = "a", "b", "c"))) AS ?b) } }"""
    for edges in [values, bind]:
        ap = m.query(
            PREFIXES
            + f"""
SELECT ?n ?cut WHERE {{ {edges}
  ?n maplib:articulationPoint ?o .
  ?o maplib:edgeSource ?a ; maplib:edgeTarget ?b ; maplib:cutOff ?cut .
}}"""
        )
        assert strip(ap).rows() == [("urn:t:b", 1)]
        bridges = m.query(
            PREFIXES
            + f"""
SELECT ?a ?b WHERE {{ {edges}
  ?a maplib:bridge ?o .
  ?o maplib:object ?b ; maplib:edgeSource ?a ; maplib:edgeTarget ?b .
}} ORDER BY ?a"""
        )
        assert strip(bridges).rows() == [("urn:t:a", "urn:t:b"), ("urn:t:b", "urn:t:c")]
        comps = m.query(
            PREFIXES
            + f"""
SELECT DISTINCT ?i ?size WHERE {{ {edges}
  ?n maplib:component ?o .
  ?o maplib:edgeSource ?a ; maplib:edgeTarget ?b ; maplib:componentId ?i ; maplib:componentSize ?size .
}}"""
        )
        assert comps.rows() == [(1, 3)]
