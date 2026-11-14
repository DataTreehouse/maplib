"""Coverage for keyword options of Model methods that were not exercised elsewhere:
graph / validate_iris on map_triples and map_xml; PVALUES parameters, include_graphs,
max_rows and debug on query, update and insert; graph / prefixes on write; graph on get_predicate
and infer_rdfs."""

import polars as pl
import pytest

from maplib import IRI, Model, RDFType, SolutionMappings, TRANSIENT_GRAPH

EX = "http://example.net/ns#"
G1 = "urn:graph:one"
G2 = "urn:graph:two"

DATA = f"""
<{EX}a> <{EX}p> <{EX}b> .
<{EX}b> <{EX}p> <{EX}c> .
<{EX}c> <{EX}p> <{EX}d> .
"""


def _model(graph=None):
    m = Model()
    m.reads(DATA, format="ntriples", graph=graph)
    return m


# --- mapping into graphs and IRI validation ------------------------------------------


def test_map_triples_into_named_graph():
    m = Model()
    df = pl.DataFrame({"subject": [EX + "a"], "object": [EX + "b"]})
    m.map_triples(df, predicate=EX + "p", graph=G1)
    assert m.size(G1) == 1
    assert m.size() == 0


def test_map_triples_validate_iris():
    df = pl.DataFrame({"subject": ["not an iri"], "object": [EX + "b"]})
    m = Model()
    with pytest.raises(Exception):
        m.map_triples(df, predicate=EX + "p", validate_iris=True)
    m2 = Model()
    m2.map_triples(df, predicate=EX + "p", validate_iris=False)
    assert m2.size() == 1


def test_map_xml_into_named_graph():
    m = Model()
    m.map_xml("<root><child>value</child></root>", graph=G1)
    assert m.size(G1) > 0
    assert m.size() == 0


def test_map_json_into_named_graph():
    m = Model()
    m.map_json('{"key": [1, "abc"]}', graph=G1)
    assert m.size(G1) > 0
    assert m.size() == 0


# --- PVALUES parameters --------------------------------------------------------------


def _pvalues(subjects):
    return {
        "h": SolutionMappings(
            pl.DataFrame({"s": subjects}), {"s": RDFType.IRI}
        )
    }


def test_select_pvalues():
    m = _model()
    df = m.query(
        f"SELECT ?s ?o WHERE {{ ?s <{EX}p> ?o . PVALUES (?s) h }} ORDER BY ?s",
        parameters=_pvalues([EX + "a", EX + "c"]),
    )
    assert df["o"].to_list() == [f"<{EX}b>", f"<{EX}d>"]


def test_update_pvalues():
    m = _model()
    m.update(
        f"""DELETE {{ ?s <{EX}p> ?o }} WHERE {{ ?s <{EX}p> ?o . PVALUES (?s) h }}""",
        parameters=_pvalues([EX + "a"]),
    )
    assert m.size() == 2
    assert not m.query(f"ASK {{ <{EX}a> <{EX}p> ?o }}")


def test_insert_pvalues():
    m = _model()
    res = m.insert(
        f"""CONSTRUCT {{ ?s a <{EX}Selected> }} WHERE {{ ?s <{EX}p> ?o . PVALUES (?s) h }}""",
        parameters=_pvalues([EX + "b"]),
    )
    (df,) = res.values()
    assert df.height == 1
    assert m.query(f"ASK {{ <{EX}b> a <{EX}Selected> }}")


# --- include_graphs ------------------------------------------------------------------


def test_update_include_graphs():
    m = _model()
    m.reads(f"<{EX}d> <{EX}p> <{EX}e> .", format="ntriples", graph=G1)
    m.update(
        f"INSERT {{ ?s <{EX}reaches> ?o }} WHERE {{ ?s <{EX}p> ?x . ?x <{EX}p> ?o }}",
        include_graphs=[G1],
    )
    # c -> d -> e is only found when G1 is included
    assert m.query(f"ASK {{ <{EX}c> <{EX}reaches> <{EX}e> }}")
    assert m.size(G1) == 1


def test_update_include_graphs_empty_excludes_transient():
    m = _model()
    m.reads(f"<{EX}d> <{EX}p> <{EX}e> .", format="ntriples", graph=TRANSIENT_GRAPH)
    m.update(
        f"INSERT {{ ?s <{EX}reaches> ?o }} WHERE {{ ?s <{EX}p> ?x . ?x <{EX}p> ?o }}",
        include_graphs=[],
    )
    assert not m.query(f"ASK {{ <{EX}c> <{EX}reaches> <{EX}e> }}")
    assert m.query(f"ASK {{ <{EX}a> <{EX}reaches> <{EX}c> }}")


def test_insert_include_graphs():
    m = _model()
    m.reads(f"<{EX}d> <{EX}p> <{EX}e> .", format="ntriples", graph=G1)
    m.insert(
        f"CONSTRUCT {{ ?s <{EX}reaches> ?o }} WHERE {{ ?s <{EX}p> ?x . ?x <{EX}p> ?o }}",
        include_graphs=[G1],
    )
    assert m.query(f"ASK {{ <{EX}c> <{EX}reaches> <{EX}e> }}")


def test_get_predicate_iris_include_graphs():
    m = _model()
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1)
    preds = sorted(p.iri for p in m.get_predicate_iris(include_graphs=[G1]))
    assert preds == [EX + "p", EX + "q"]


def test_get_predicate_named_graph():
    m = _model()
    m.reads(f"<{EX}x> <{EX}p> <{EX}y> .", format="ntriples", graph=G1)
    sms = m.get_predicate(IRI(EX + "p"), graph=G1)
    assert sum(sm.mappings.height for sm in sms) == 1


# --- max_rows / debug ----------------------------------------------------------------

CROSS = f"SELECT * WHERE {{ ?a <{EX}p> ?b . ?c <{EX}p> ?d . ?e <{EX}p> ?f }}"


def test_query_max_rows_exceeded_raises():
    m = _model()
    with pytest.raises(Exception, match="max_rows|Maximum estimated rows"):
        m.query(CROSS, max_rows=5)
    assert m.query(CROSS, max_rows=1000).height == 27


def test_update_max_rows_exceeded_raises():
    m = _model()
    with pytest.raises(Exception, match="max_rows|Maximum estimated rows"):
        m.update(
            f"INSERT {{ ?a <{EX}q> ?f }} WHERE {{ ?a <{EX}p> ?b . ?c <{EX}p> ?d . ?e <{EX}p> ?f }}",
            max_rows=5,
        )
    assert m.size() == 3


def test_insert_max_rows_exceeded_raises():
    m = _model()
    with pytest.raises(Exception, match="max_rows|Maximum estimated rows"):
        m.insert(
            f"CONSTRUCT {{ ?a <{EX}q> ?f }} WHERE {{ ?a <{EX}p> ?b . ?c <{EX}p> ?d . ?e <{EX}p> ?f }}",
            max_rows=5,
        )
    assert m.size() == 3


def test_query_debug_solution_mappings():
    m = _model()
    sm = m.query(
        f"SELECT ?s WHERE {{ ?s <{EX}p> <{EX}nothing> }}",
        debug=True,
        solution_mappings=True,
    )
    assert sm.mappings.height == 0
    assert sm.debug is not None
    assert len(sm.debug) > 0


def test_update_and_insert_debug_do_not_change_results():
    m = _model()
    m.update(
        f"INSERT {{ ?s <{EX}q> ?o }} WHERE {{ ?s <{EX}p> ?o }}", debug=True
    )
    res = m.insert(
        f"CONSTRUCT {{ ?s <{EX}r> ?o }} WHERE {{ ?s <{EX}p> ?o }}", debug=True
    )
    assert m.query(f"SELECT ?s WHERE {{ ?s <{EX}q> ?o }}").height == 3
    assert sum(df.height for df in res.values()) == 3


# --- write options -------------------------------------------------------------------


def test_write_named_graph(tmp_path):
    m = _model()
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1)
    path = tmp_path / "g1.nt"
    m.write(str(path), format="ntriples", graph=G1)
    assert path.read_text().strip() == f"<{EX}x> <{EX}q> <{EX}y> ."


def test_write_turtle_prefixes(tmp_path):
    m = _model()
    path = tmp_path / "out.ttl"
    m.write(str(path), format="turtle", prefixes={"ex": EX})
    text = path.read_text()
    assert "ex:a" in text
    m2 = Model()
    m2.read(str(path))
    assert m2.size() == 3


# --- infer_rdfs graph ----------------------------------------------------------------


def test_infer_rdfs_named_graph():
    m = Model()
    m.reads(
        f"""
        @prefix ex: <{EX}> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        ex:Dog rdfs:subClassOf ex:Animal .
        ex:rex a ex:Dog .
        """,
        format="turtle",
        graph=G1,
    )
    n = m.infer_rdfs(graph=G1)
    assert n >= 1
    assert m.query(
        f"ASK {{ <{EX}rex> a <{EX}Animal> }}", graph=G1
    )
    # The inferred triples are put in the transient graph, not in G1 itself
    assert m.size(G1) == 2
    assert not m.query(
        f"ASK {{ <{EX}rex> a <{EX}Animal> }}", graph=G1, include_graphs=[]
    )
    assert m.query(
        f"ASK {{ <{EX}rex> a <{EX}Animal> }}", graph=TRANSIENT_GRAPH, include_graphs=[]
    )
