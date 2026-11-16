"""GRAPH blocks in CONSTRUCT templates (https://github.com/w3c/sparql-dev/issues/31)."""

import pathlib

import polars as pl
import pytest
from maplib import Model

PATH_HERE = pathlib.Path(__file__).parent
JENA_PATH = PATH_HERE / "testdata" / "jena_construct_quads"

DATA = """
@prefix ex: <http://example.net/ns#> .
ex:a ex:p ex:b .
ex:b ex:p ex:c .
ex:a ex:g ex:g1 .
ex:b ex:g ex:g2 .
ex:a ex:v 1 .
ex:b ex:v 2 .
"""

PREFIX = "PREFIX ex:<http://example.net/ns#>\n"
EX = "http://example.net/ns#"


@pytest.fixture
def m():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def triples(m: Model, graph: str = None):
    df = m.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", graph=graph, include_graphs=[])
    return sorted(df.rows())


def test_query_graph_iri_and_variable(m):
    res = m.query(
        PREFIX
        + """
    CONSTRUCT {
        GRAPH ex:fixed { ?s ex:q ?o }
        GRAPH ?g { ?s ex:r ?o }
        ?s ex:t ?o
    } WHERE { ?s ex:p ?o . ?s ex:g ?g }
    """
    )
    by_graph = {}
    for df in res:
        g = df.get_column("graph")[0] if "graph" in df.columns else None
        if g is not None:
            assert df.get_column("graph").n_unique() == 1
        by_graph.setdefault(g, []).append(df)
    # The default graph part has no graph column and comes first.
    assert "graph" not in res[0].columns
    assert set(res[0].columns) == {"subject", "predicate", "object"}
    assert set(by_graph.keys()) == {None, f"<{EX}fixed>", f"<{EX}g1>", f"<{EX}g2>"}
    assert sum(df.height for df in by_graph[f"<{EX}fixed>"]) == 2
    assert sum(df.height for df in by_graph[f"<{EX}g1>"]) == 1
    assert by_graph[f"<{EX}g1>"][0].get_column("subject")[0] == f"<{EX}a>"
    assert by_graph[f"<{EX}g2>"][0].get_column("subject")[0] == f"<{EX}b>"


def test_insert_routes_to_graphs(m):
    m.insert(
        PREFIX
        + """
    CONSTRUCT {
        GRAPH ex:fixed { ?s ex:q ?o }
        GRAPH ?g { ?s ex:r ?o . ?o ex:r2 ?s }
        ?s ex:t ?o
    } WHERE { ?s ex:p ?o . ?s ex:g ?g }
    """,
        target_graph="urn:target",
    )
    a, b, c = f"<{EX}a>", f"<{EX}b>", f"<{EX}c>"
    assert triples(m, "urn:target") == [(a, f"<{EX}t>", b), (b, f"<{EX}t>", c)]
    assert triples(m, f"{EX}fixed") == [(a, f"<{EX}q>", b), (b, f"<{EX}q>", c)]
    assert triples(m, f"{EX}g1") == [(a, f"<{EX}r>", b), (b, f"<{EX}r2>", a)]
    assert triples(m, f"{EX}g2") == [(b, f"<{EX}r>", c), (c, f"<{EX}r2>", b)]
    # Nothing was added to the default graph.
    assert len(triples(m)) == 6


def test_insert_default_part_goes_to_default_graph(m):
    m.insert(
        PREFIX
        + """
    CONSTRUCT { ?s ex:t ?o GRAPH ex:other { ?s ex:t ?o } } WHERE { ?s ex:p ?o }
    """
    )
    assert len(triples(m)) == 8
    assert len(triples(m, f"{EX}other")) == 2


def test_insert_only_graph_blocks(m):
    m.insert(
        PREFIX
        + """
    CONSTRUCT { GRAPH ex:x { ?s ex:t ?o } GRAPH ex:y { ?o ex:t ?s } } WHERE { ?s ex:p ?o }
    """
    )
    assert len(triples(m)) == 6
    assert len(triples(m, f"{EX}x")) == 2
    assert len(triples(m, f"{EX}y")) == 2


def test_same_as_separate_inserts(m):
    # One CONSTRUCT with GRAPH blocks gives the same graphs as one CONSTRUCT per graph.
    m2 = Model()
    m2.reads(DATA, format="turtle")
    m.insert(
        PREFIX
        + """
    CONSTRUCT { GRAPH ex:x { ?s ex:t ?v } GRAPH ex:y { ?s ex:u ?o } } WHERE { ?s ex:p ?o . ?s ex:v ?v }
    """
    )
    m2.insert(
        PREFIX + "CONSTRUCT { ?s ex:t ?v } WHERE { ?s ex:p ?o . ?s ex:v ?v }",
        target_graph=f"{EX}x",
    )
    m2.insert(
        PREFIX + "CONSTRUCT { ?s ex:u ?o } WHERE { ?s ex:p ?o . ?s ex:v ?v }",
        target_graph=f"{EX}y",
    )
    for g in [None, f"{EX}x", f"{EX}y"]:
        assert triples(m, g) == triples(m2, g)


def test_blank_nodes_shared_across_graph_blocks(m):
    m.insert(
        PREFIX
        + """
    CONSTRUCT { GRAPH ex:x { ?s ex:node _:n } GRAPH ex:y { _:n ex:of ?s } } WHERE { ?s ex:p ?o }
    """
    )
    x = m.query(
        "SELECT ?s ?n WHERE { ?s <http://example.net/ns#node> ?n }", graph=f"{EX}x"
    )
    y = m.query(
        "SELECT ?s ?n WHERE { ?n <http://example.net/ns#of> ?s }", graph=f"{EX}y"
    )
    assert x.height == 2
    assert x.get_column("n").n_unique() == 2
    assert sorted(x.rows()) == sorted(y.rows())


def test_unbound_or_literal_graph_variable_is_skipped(m):
    res = m.query(
        PREFIX
        + """
    CONSTRUCT { GRAPH ?g { ?s ex:t ?o } } WHERE { ?s ex:p ?o OPTIONAL { ?s ex:v ?g FILTER(?g > 1) } }
    """
    )
    # ?g is unbound for ex:a and a literal for ex:b, so no triples are constructed.
    assert sum(df.height for df in res) == 0


def test_graph_variable_bound_by_where_graph(m):
    m.reads(
        "<urn:s> <urn:p> <urn:o> .", format="ntriples", graph="urn:g:copy_me"
    )
    m.insert(
        "CONSTRUCT { GRAPH ?g { ?s <urn:copied> ?o } } WHERE { GRAPH ?g { ?s ?p ?o } }"
    )
    assert triples(m, "urn:g:copy_me") == [
        ("<urn:s>", "<urn:copied>", "<urn:o>"),
        ("<urn:s>", "<urn:p>", "<urn:o>"),
    ]


def test_construct_where_short_form_with_graph(m):
    m.reads(
        "<urn:s> <urn:p> <urn:o> .", format="ntriples", graph="urn:g:short"
    )
    res = m.query("CONSTRUCT WHERE { GRAPH ?g { ?s <urn:p> ?o } }")
    assert len(res) == 1
    assert res[0].get_column("graph").to_list() == ["<urn:g:short>"]
    assert res[0].get_column("subject").to_list() == ["<urn:s>"]


def test_update_insert_graph_still_works(m):
    m.update(
        PREFIX
        + "INSERT { GRAPH ?g { ?s ex:t ?o } ?s ex:t ?o } WHERE { ?s ex:p ?o . ?s ex:g ?g }"
    )
    assert len(triples(m)) == 8
    assert len(triples(m, f"{EX}g1")) == 1


def test_bindings_replace_graph_variable(m):
    res = m.query(
        PREFIX + "CONSTRUCT { GRAPH ?g { ?s ex:t ?o } } WHERE { ?s ex:p ?o }",
        bindings={"g": "<urn:bound>"},
    )
    assert len(res) == 1
    assert res[0].get_column("graph").to_list() == ["<urn:bound>", "<urn:bound>"]


def test_trig_and_turtle_formats(m):
    q = PREFIX + "CONSTRUCT { ?s ex:t ?o GRAPH ex:x { ?s ex:q ?o } } WHERE { ?s ex:p ?o }"
    trig = m.query(q, return_format="trig")
    assert "ns#x" in trig or "ex:x" in trig
    m2 = Model()
    m2.reads(trig, format="trig")
    assert len(triples(m2)) == 2
    assert len(triples(m2, f"{EX}x")) == 2
    with pytest.raises(Exception, match="trig"):
        m.query(q, return_format="turtle")
    # Turtle still works without GRAPH blocks.
    ttl = m.query(
        PREFIX + "CONSTRUCT { ?s ex:t ?o } WHERE { ?s ex:p ?o }", return_format="turtle"
    )
    assert "ns#t" in ttl or "ex:t" in ttl


def test_solution_mappings_have_graph(m):
    res = m.query(
        PREFIX + "CONSTRUCT { GRAPH ex:x { ?s ex:q ?o } } WHERE { ?s ex:p ?o }",
        solution_mappings=True,
    )
    assert len(res) == 1
    assert "graph" in res[0].mappings.columns


JENA_POSITIVE = [f"syntax-quad-construct-{i:02}.arq" for i in range(1, 13)]
# Jena also allows bare { ... } blocks in templates, which are not supported.
JENA_UNSUPPORTED = {"syntax-quad-construct-09.arq", "syntax-quad-construct-12.arq"}


@pytest.mark.parametrize("name", JENA_POSITIVE)
def test_jena_positive_syntax(name):
    q = (JENA_PATH / name).read_text()
    m = Model()
    m.reads(
        "<http://example/s> <http://example/p> <http://example/o> .",
        format="ntriples",
        graph="http://example/g",
    )
    if name in JENA_UNSUPPORTED:
        with pytest.raises(Exception):
            m.query(q)
    else:
        res = m.query(q)
        assert isinstance(res, list)


def test_jena_negative_syntax():
    q = (JENA_PATH / "syntax-quad-construct-bad-01.arq").read_text()
    with pytest.raises(Exception):
        Model().query(q)


def test_jena_04_results():
    # CONSTRUCT { GRAPH ?g { :s :p :o } ?s ?p ?o } WHERE { GRAPH ?g { ?s ?p ?o } }
    q = (JENA_PATH / "syntax-quad-construct-04.arq").read_text()
    m = Model()
    m.reads(
        "<http://example/a> <http://example/b> <http://example/c> .",
        format="ntriples",
        graph="http://example/g",
    )
    m.insert(q, target_graph="urn:out")
    assert triples(m, "http://example/g") == [
        ("<http://example/a>", "<http://example/b>", "<http://example/c>"),
        ("<http://example/s>", "<http://example/p>", "<http://example/o>"),
    ]
    assert triples(m, "urn:out") == [
        ("<http://example/a>", "<http://example/b>", "<http://example/c>")
    ]
