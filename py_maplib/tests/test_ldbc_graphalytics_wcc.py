"""maplib:component checked against the LDBC Graphalytics validation graphs for weakly connected
components (testdata/ldbc_graphalytics, Apache License 2.0).

Graphalytics defines WCC as the components of the graph with its edges taken both ways, and accepts
an output that is equivalent to the reference: the same partition of the vertices, whatever the
labels. Every vertex of the graph has a label, also one without edges, so the vertices are bound in
the same block as the predicate, which makes vertices without edges components of their own.
"""

from collections import defaultdict
from pathlib import Path

import pytest

from maplib import Model

DATA = Path(__file__).parent / "testdata" / "ldbc_graphalytics"
EX = "http://example.org/"
QUERY = f"""PREFIX ex: <{EX}>
PREFIX maplib: <https://datatreehouse.github.io/maplib/vocab#>
SELECT ?v ?id ?size WHERE {{
    ?v a ex:Vertex .
    ?v maplib:component [ maplib:predicate ex:edge ; maplib:componentId ?id ; maplib:componentSize ?size ]
}}"""


def adjacency_list(path: Path) -> tuple[set[str], list[tuple[str, str]]]:
    """wcc/*-input: a vertex, then its neighbours."""
    vertices, edges = set(), []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        v, *neighbours = line.split()
        vertices.add(v)
        for w in neighbours:
            vertices.add(w)
            edges.append((v, w))
    return vertices, edges


def vertex_edge_files(stem: Path) -> tuple[set[str], list[tuple[str, str]]]:
    """example/*.v and *.e: the vertices, and the edges with a weight."""
    vertices = {line.strip() for line in stem.with_suffix(".v").read_text().splitlines() if line.strip()}
    edges = [tuple(line.split()[:2]) for line in stem.with_suffix(".e").read_text().splitlines() if line.strip()]
    return vertices, edges


def labels(path: Path) -> dict[str, str]:
    return dict(line.split() for line in path.read_text().splitlines() if line.strip())


def partition(assignment: dict[str, object]) -> set[frozenset[str]]:
    parts = defaultdict(set)
    for v, label in assignment.items():
        parts[label].add(v)
    return {frozenset(p) for p in parts.values()}


def maplib_wcc(vertices: set[str], edges: list[tuple[str, str]]) -> dict[str, tuple[int, int]]:
    m = Model()
    triples = [f"<{EX}{v}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{EX}Vertex> ." for v in vertices]
    triples += [f"<{EX}{a}> <{EX}edge> <{EX}{b}> ." for a, b in edges]
    m.reads("\n".join(triples), format="ntriples")
    out = m.query(QUERY)
    return {r["v"].strip("<>").removeprefix(EX): (r["id"], r["size"]) for r in out.iter_rows(named=True)}


CASES = {
    "wcc/dir": lambda: (adjacency_list(DATA / "wcc" / "dir-input"), labels(DATA / "wcc" / "dir-output")),
    "wcc/undir": lambda: (adjacency_list(DATA / "wcc" / "undir-input"), labels(DATA / "wcc" / "undir-output")),
    "example-directed": lambda: (
        vertex_edge_files(DATA / "example" / "example-directed"),
        labels(DATA / "example" / "example-directed-WCC"),
    ),
    "example-undirected": lambda: (
        vertex_edge_files(DATA / "example" / "example-undirected"),
        labels(DATA / "example" / "example-undirected-WCC"),
    ),
}


@pytest.mark.parametrize("case", CASES)
def test_wcc_matches_the_graphalytics_reference(case):
    (vertices, edges), reference = CASES[case]()
    got = maplib_wcc(vertices, edges)
    # every vertex of the reference has a component, and no other node
    assert set(got) == set(reference)
    assert partition({v: c for v, (c, _) in got.items()}) == partition(reference)
    for v, (c, size) in got.items():
        assert size == sum(1 for c2, _ in got.values() if c2 == c)


def test_the_validation_graphs_are_not_trivial():
    """At least one case has several components, and one has a vertex without edges of its own."""
    several = [c for c in CASES if len(partition(CASES[c]()[1])) > 1]
    assert several, "a case with more than one component"
    (vertices, edges), _ = CASES["wcc/dir"]()
    assert any(not any(v == a for a, _ in edges) for v in vertices), "a vertex with no edges out"
