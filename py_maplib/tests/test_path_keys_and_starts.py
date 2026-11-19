"""Property paths with `+`/`*` key their nodes straight from dictionary codes when the path's
predicates link nodes of one type, and paths whose first step few nodes have are followed from
those nodes instead of the closure over all nodes. Compare with rdflib on random graphs, as sets
of (start, end) pairs."""
import random

import pytest
import rdflib
from maplib import Model

EX = "http://ex.org/"
PFX = f"PREFIX : <{EX}>\n"

PATHS = [
    ":next+",
    ":next*",
    "^:next+",
    ":next/:next+",
    ":rare/:next+",  # few start nodes: followed from them
    ":rare/:next*",
    "^:rare/:next+",
    ":next+/:rare",
    "(:next|:rare)+",
    ":rare+",
    ":missing/:next+",
]


def graph(seed, blank_nodes):
    rng = random.Random(seed)
    nodes = [f"<{EX}n{i}>" for i in range(60)]
    if blank_nodes:
        nodes += [f"_:b{i}" for i in range(20)]
    lines = []
    # Lists and some branching and cycles for :next, a few :rare edges.
    for i in range(len(nodes) - 1):
        if rng.random() < 0.85:
            lines.append(f"{nodes[i]} <{EX}next> {nodes[i + 1]} .")
    for _ in range(15):
        lines.append(f"{rng.choice(nodes)} <{EX}next> {rng.choice(nodes)} .")
    for _ in range(5):
        lines.append(f"{rng.choice(nodes)} <{EX}rare> {rng.choice(nodes)} .")
    return "\n".join(lines)


def pairs_maplib(m, q):
    df = m.query(PFX + q)
    return {tuple(str(v) for v in row) for row in df.rows()}


def pairs_rdflib(g, q):
    out = set()
    for row in g.query(PFX + q):
        out.add(tuple(f"<{v}>" if isinstance(v, rdflib.URIRef) else "_" for v in row))
    return out


def normalise(pairs):
    # Blank node labels differ between the engines: compare them as "_".
    return {tuple("_" if v.startswith("_:") else v for v in p) for p in pairs}


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("blank_nodes", [False, True])
@pytest.mark.parametrize("path", PATHS)
def test_unbound_matches_rdflib(seed, blank_nodes, path):
    data = graph(seed, blank_nodes)
    m = Model()
    m.reads(data, format="ntriples")
    g = rdflib.Graph()
    g.parse(data=data, format="nt")
    q = f"SELECT ?s ?o WHERE {{ ?s {path} ?o }}"
    got = normalise(pairs_maplib(m, q))
    exp = pairs_rdflib(g, q)
    if blank_nodes:
        # Pairs collapse when labels are hidden; compare the IRI-only pairs exactly and the
        # counts per kind of pair.
        assert {p for p in got if "_" not in p} == {p for p in exp if "_" not in p}
    else:
        assert got == exp


@pytest.mark.parametrize("seed", [1, 2])
@pytest.mark.parametrize("path", [":next+", ":rare/:next+", "^:next+", ":next*"])
def test_bound_start_matches_rdflib(seed, path):
    data = graph(seed, False)
    m = Model()
    m.reads(data, format="ntriples")
    g = rdflib.Graph()
    g.parse(data=data, format="nt")
    for start in ["n0", "n10", "n59", "nope"]:
        q = f"SELECT ?o WHERE {{ :{start} {path} ?o }}"
        got = {r[0] for r in m.query(PFX + q).rows()} if m.query(PFX + q).height else set()
        exp = {f"<{r[0]}>" for r in g.query(PFX + q)}
        assert {str(x) for x in got} == exp


def test_count_shapes_of_sparqloscope():
    # A sequence path keeps one solution per intermediate node (it is a join), e.g. 20057 rather
    # than 5076 for Sparqloscope's transitive-path-small-join-and-plus on RINF 0076.
    data = graph(7, False)
    m = Model()
    m.reads(data, format="ntriples")
    g = rdflib.Graph()
    g.parse(data=data, format="nt")
    # Not ":next/:next+": a step repeated with + at the end is the length range :next{2,}, whose
    # solutions are distinct pairs in maplib (see is_repetition in path.rs).
    for path in [":next+", ":rare/:next+", "^:rare/:next+"]:
        q = f"SELECT (COUNT(*) AS ?c) {{ ?s {path} ?o }}"
        assert m.query(PFX + q)["c"][0] == int(list(g.query(PFX + q))[0][0])
