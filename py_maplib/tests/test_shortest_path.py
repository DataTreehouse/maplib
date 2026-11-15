"""The maplib:shortestPath magic predicate, checked against a reference breadth first search."""

import json
import random
from collections import deque

import polars as pl
import pytest
from maplib import Model

PREFIXES = """
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
PREFIX ex: <http://example.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


DIRECTIONS = {
    "forward": "maplib:predicate ex:p, ex:q",
    "backward": "maplib:inversePredicate ex:p, ex:q",
    "both": "maplib:predicate ex:p, ex:q ; maplib:inversePredicate ex:p, ex:q",
}


def ex(n) -> str:
    return f"http://example.org/{n}"


def iri(n) -> str:
    return f"<{ex(n)}>"


def model_of(edges, extra: str = "") -> Model:
    """A model with triples ex:a ex:p ex:b for (a, p, b) in edges."""
    ttl = "".join(f"<{ex(a)}> <{ex(p)}> <{ex(b)}> .\n" for a, p, b in edges)
    m = Model()
    m.reads(ttl + extra, format="ntriples")
    return m


def reference_distances(edges, source, direction="forward", avoid=()):
    adj = {}
    for a, _, b in edges:
        if direction in ("forward", "both"):
            adj.setdefault(a, []).append(b)
        if direction in ("backward", "both"):
            adj.setdefault(b, []).append(a)
    dist = {source: 0}
    q = deque([source])
    while q:
        u = q.popleft()
        if u != source and u in avoid:
            continue
        for v in adj.get(u, []):
            if v not in dist:
                dist[v] = dist[u] + 1
                q.append(v)
    return dist


def random_edges(seed, n, m, preds=("p", "q")):
    r = random.Random(seed)
    edges = set()
    while len(edges) < m:
        edges.add((f"n{r.randrange(n)}", r.choice(preds), f"n{r.randrange(n)}"))
    return sorted(edges)


def check_path(nodes, edges, direction):
    """Each step of the path is an edge of the graph in the given direction."""
    es = {(ex(a), ex(b)) for a, _, b in edges}
    for a, b in zip(nodes, nodes[1:]):
        ok = (direction in ("forward", "both") and (a, b) in es) or (
            direction in ("backward", "both") and (b, a) in es
        )
        assert ok, f"{a} -> {b} is not an edge ({direction})"


@pytest.mark.parametrize("seed", range(6))
@pytest.mark.parametrize("direction", ["forward", "backward", "both"])
def test_all_targets_match_reference(seed, direction):
    edges = random_edges(seed, 60, 130)
    m = model_of(edges)
    source = "n0"
    df = m.query(
        PREFIXES
        + f"""
    SELECT ?t ?len ?nodes WHERE {{
      {iri(source)} maplib:shortestPath ?p .
      ?p {DIRECTIONS[direction]} ;
         maplib:object ?t ;
         maplib:minLength 0 ;
         maplib:length ?len ;
         maplib:pathNodes ?nodes .
    }}"""
    )
    got = {row["t"]: row["len"] for row in df.iter_rows(named=True)}
    expected = {
        f"<{ex(k)}>": v for k, v in reference_distances(edges, source, direction).items()
    }
    # The source itself is not in the graph if it has no edges.
    if source not in {x for a, _, b in edges for x in (a, b)}:
        expected = {}
    assert got == expected
    for row in df.iter_rows(named=True):
        nodes = json.loads(row["nodes"])
        assert len(nodes) == row["len"] + 1
        assert f"<{nodes[0]}>" == iri(source)
        assert f"<{nodes[-1]}>" == row["t"]
        check_path(nodes, edges, direction)


@pytest.mark.parametrize("seed", range(4))
def test_pairs_and_target_patterns_match_reference(seed):
    edges = random_edges(seed + 100, 40, 90)
    extra = "".join(
        f"<{ex(f'n{i}')}> <{ex('kind')}> <{ex('Special')}> .\n" for i in range(0, 40, 3)
    )
    m = model_of(edges, extra)
    # Sources from VALUES, targets from a pattern: every source with every target.
    df = m.query(
        PREFIXES
        + """
    SELECT ?s ?t ?len WHERE {
      VALUES ?s { ex:n1 ex:n2 ex:n5 }
      ?s maplib:shortestPath ?p .
      ?p maplib:predicate ex:p, ex:q ;
         maplib:object ?t ;
         maplib:length ?len .
      ?t ex:kind ex:Special .
    }"""
    )
    got = {(r["s"], r["t"]): r["len"] for r in df.iter_rows(named=True)}
    expected = {}
    for s in ["n1", "n2", "n5"]:
        d = reference_distances(edges, s)
        if s not in {x for a, _, b in edges for x in (a, b)}:
            continue
        for i in range(0, 40, 3):
            t = f"n{i}"
            if t in d:
                expected[(iri(s), iri(t))] = d[t]
    assert got == expected

    # The nearest 2 special nodes of each source.
    df = m.query(
        PREFIXES
        + """
    SELECT ?s ?t ?len WHERE {
      VALUES ?s { ex:n1 ex:n2 ex:n5 }
      ?s maplib:shortestPath ?p .
      ?p maplib:predicate ex:p, ex:q ;
         maplib:object ?t ;
         maplib:limit 2 ;
         maplib:length ?len .
      ?t ex:kind ex:Special .
    }"""
    )
    for s in ["n1", "n2", "n5"]:
        lens = sorted(r["len"] for r in df.iter_rows(named=True) if r["s"] == iri(s))
        exp = sorted(v for (ss, _), v in expected.items() if ss == iri(s))[:2]
        assert lens == exp


def test_chain_nodes_and_steps():
    edges = [("a", "p", "b"), ("b", "q", "c"), ("d", "p", "c")]
    m = model_of(edges)
    df = m.query(
        PREFIXES
        + """
    SELECT ?i ?n ?pred ?inv WHERE {
      ex:a maplib:shortestPath ?p .
      ?p maplib:predicate ex:p, ex:q ;
         maplib:inversePredicate ex:p, ex:q ;
         maplib:object ex:d ;
         maplib:nodeIndex ?i ;
         maplib:pathNode ?n ;
         maplib:stepPredicate ?pred ;
         maplib:stepInverse ?inv .
    } ORDER BY ?i"""
    )
    assert df["n"].to_list() == [iri("a"), iri("b"), iri("c"), iri("d")]
    assert df["i"].to_list() == [0, 1, 2, 3]
    assert df["pred"].to_list() == [None, iri("p"), iri("q"), iri("p")]
    assert df["inv"].to_list() == [None, False, False, True]


def test_edge_cases():
    edges = [("a", "p", "b"), ("b", "p", "c"), ("c", "p", "a"), ("x", "p", "y")]
    m = model_of(edges)

    def lengths(body):
        df = m.query(PREFIXES + "SELECT ?t ?len WHERE {" + body + "}")
        return {r["t"]: r["len"] for r in df.iter_rows(named=True)} if "?t" in body else df

    # Source = target.
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:a ; maplib:minLength 0 ;
           maplib:length ?len . }"""
    )
    assert df["len"].to_list() == [0]
    # With the default minimum length 1, back to the source around the cycle.
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:a ; maplib:length ?len . }"""
    )
    assert df["len"].to_list() == [3]
    # No path.
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:x ; maplib:length ?len . }"""
    )
    assert df.height == 0
    # A source that is not in the graph.
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:nothere maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:a ; maplib:length ?len . }"""
    )
    assert df.height == 0
    # Cycle, with a minimum and maximum length.
    got = lengths(
        """ex:a maplib:shortestPath ?p . ?p maplib:predicate ex:p ; maplib:object ?t ;
        maplib:minLength 1 ; maplib:maxLength 1 ; maplib:length ?len ."""
    )
    assert got == {iri("b"): 1}
    # A predicate without triples.
    df = m.query(
        PREFIXES
        + """SELECT ?t WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:none ; maplib:object ?t . }"""
    )
    assert df.height == 0
    # The source must be bound.
    with pytest.raises(Exception, match="must be bound"):
        m.query(
            PREFIXES
            + """SELECT ?t WHERE { ?s maplib:shortestPath ?p .
            ?p maplib:predicate ex:p ; maplib:object ?t . }"""
        )
    # Unknown parameters are errors.
    with pytest.raises(Exception, match="bogus"):
        m.query(
            PREFIXES
            + """SELECT ?t WHERE { ex:a maplib:shortestPath ?p .
            ?p maplib:predicate ex:p ; maplib:bogus 1 . }"""
        )


def test_parallel_edges_all_and_k_shortest():
    # Two predicates between a and b, then b to c; and a longer way a -> d -> e -> c.
    edges = [
        ("a", "p", "b"),
        ("a", "q", "b"),
        ("b", "p", "c"),
        ("a", "p", "d"),
        ("d", "p", "e"),
        ("e", "p", "c"),
    ]
    m = model_of(edges)
    df = m.query(
        PREFIXES
        + """SELECT ?id ?len ?nodes WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p, ex:q ; maplib:object ex:c ; maplib:allShortest true ;
           maplib:pathIndex ?id ; maplib:length ?len ; maplib:pathNodes ?nodes . } ORDER BY ?id"""
    )
    assert df["len"].to_list() == [2, 2]
    assert df["id"].to_list() == [0, 1]
    df = m.query(
        PREFIXES
        + """SELECT ?id ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p, ex:q ; maplib:object ex:c ; maplib:kShortest 5 ;
           maplib:pathIndex ?id ; maplib:length ?len . } ORDER BY ?id"""
    )
    assert df["len"].to_list() == [2, 2, 3]


def test_weights_and_avoid():
    # a -> b -> d is short but b is heavy; a -> c1 -> c2 -> d is light.
    edges = [
        ("a", "p", "b"),
        ("b", "p", "d"),
        ("a", "p", "c1"),
        ("c1", "p", "c2"),
        ("c2", "p", "d"),
    ]
    extra = (
        f'<{ex("b")}> <{ex("w")}> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
        f'<{ex("c1")}> <{ex("w")}> "1.5"^^<http://www.w3.org/2001/XMLSchema#double> .\n'
        f'<{ex("blocker")}> <{ex("blocks")}> <{ex("b")}> .\n'
    )
    m = model_of(edges, extra)
    df = m.query(
        PREFIXES
        + """SELECT ?cost ?len ?nodes WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:d ; maplib:weight ex:w ;
           maplib:defaultWeight 1 ;
           maplib:cost ?cost ; maplib:length ?len ; maplib:pathNodes ?nodes . }"""
    )
    assert df["len"].to_list() == [3]
    assert df["cost"].to_list() == [3.5]
    assert json.loads(df["nodes"][0]) == [ex("a"), ex("c1"), ex("c2"), ex("d")]
    # Unweighted, the way through b is shorter, unless b is avoided.
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:d ; maplib:length ?len . }"""
    )
    assert df["len"].to_list() == [2]
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:d ; maplib:length ?len ;
           maplib:avoid ?x .
        ?y ex:blocks ?x . }"""
    )
    assert df["len"].to_list() == [3]
    # Avoided nodes can still be targets.
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:b ; maplib:avoid ex:b ;
           maplib:length ?len . }"""
    )
    assert df["len"].to_list() == [1]


def test_named_graphs_and_cache():
    m = Model()
    m.reads(
        "<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n",
        format="ntriples",
        graph="http://example.org/g1",
    )
    m.reads(
        "<http://example.org/a> <http://example.org/p> <http://example.org/c> .\n"
        "<http://example.org/c> <http://example.org/p> <http://example.org/b> .\n",
        format="ntriples",
    )
    q = (
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ex:b ; maplib:length ?len . }"""
    )
    assert m.query(q)["len"].to_list() == [2]
    assert m.query(q, graph="http://example.org/g1")["len"].to_list() == [1]
    # The cached graph is rebuilt after an update.
    m.update(PREFIXES + "INSERT DATA { ex:a ex:p ex:b }")
    assert m.query(q)["len"].to_list() == [1]
    m.update(PREFIXES + "DELETE DATA { ex:a ex:p ex:b }")
    assert m.query(q)["len"].to_list() == [2]


def test_unbound_outputs_join_with_other_patterns():
    edges = [("a", "p", "b"), ("b", "p", "c")]
    m = model_of(
        edges,
        f'<{ex("c")}> <{ex("name")}> "C" .\n<{ex("b")}> <{ex("name")}> "B" .\n',
    )
    # The target's name is joined afterwards; the source is a constant.
    df = m.query(
        PREFIXES
        + """SELECT ?t ?name ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ?t ; maplib:length ?len ;
           maplib:limit 1 ; maplib:minLength 1 .
        ?t ex:name ?name . }"""
    )
    assert df.to_dicts() == [{"t": iri("b"), "name": "B", "len": 1}]
    # Through an OPTIONAL and a FILTER outside the group.
    df = m.query(
        PREFIXES
        + """SELECT ?t ?len WHERE { { ex:a maplib:shortestPath ?p .
        ?p maplib:predicate ex:p ; maplib:object ?t ; maplib:length ?len . }
        FILTER(?len > 1) }"""
    )
    assert df["t"].to_list() == [iri("c")]


def test_any_predicate_skips_types_and_literals():
    edges = [("a", "p", "b"), ("c", "q", "b")]
    extra = (
        f"<{ex('a')}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{ex('K')}> .\n"
        f"<{ex('c')}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{ex('K')}> .\n"
        f'<{ex("a")}> <{ex("name")}> "same" .\n<{ex("c")}> <{ex("name")}> "same" .\n'
    )
    m = model_of(edges, extra)
    df = m.query(
        PREFIXES
        + """SELECT ?len ?nodes WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:anyPredicate "both" ; maplib:object ex:c ; maplib:length ?len ;
           maplib:pathNodes ?nodes . }"""
    )
    # not through the class or the shared name, which would be 2 as well, but through b
    assert df["len"].to_list() == [2]
    assert json.loads(df["nodes"][0]) == [ex("a"), ex("b"), ex("c")]
    df = m.query(
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:anyPredicate "forward" ; maplib:object ex:c ; maplib:length ?len . }"""
    )
    assert df.height == 0


def test_any_predicate_with_exclusions():
    edges = [("a", "p", "b"), ("b", "p", "c"), ("a", "region", "r"), ("c", "region", "r")]
    m = model_of(edges)
    q = (
        PREFIXES
        + """SELECT ?len WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:anyPredicate "both" ; %s maplib:object ex:c ; maplib:length ?len . }"""
    )
    assert m.query(q % "")["len"].to_list() == [2]
    assert m.query(q % "maplib:excludePredicate ex:region ;")["len"].to_list() == [2]
    df = m.query(
        PREFIXES
        + """SELECT ?n WHERE { ex:a maplib:shortestPath ?p .
        ?p maplib:anyPredicate "both" ; maplib:excludePredicate ex:region ; maplib:object ex:c ;
           maplib:pathNode ?n ; maplib:nodeIndex ?i . } ORDER BY ?i"""
    )
    assert df["n"].to_list() == [iri("a"), iri("b"), iri("c")]
