"""GRAPH ?g on a model without (visible) named graphs has no solutions, but ?g stays in scope."""

import pytest
from maplib import Model

RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
DEFAULT_ONLY = f"<urn:a> {RDF_TYPE} <urn:T> .\n"


@pytest.fixture
def m():
    model = Model()
    model.reads(DEFAULT_ONLY, format="ntriples")
    return model


def test_group_by_graph_variable(m):
    df = m.query(
        "SELECT ?g (COUNT(?x) AS ?n) WHERE { GRAPH ?g { ?x a <urn:T> } } GROUP BY ?g"
    )
    assert df.shape == (0, 2)
    assert df.columns == ["g", "n"]


def test_count_star_group_by_graph_variable(m):
    df = m.query(
        "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?x ?p ?o } } GROUP BY ?g"
    )
    assert df.shape == (0, 2)
    assert df.columns == ["g", "n"]


def test_select_graph_variable(m):
    df = m.query("SELECT ?g ?x WHERE { GRAPH ?g { ?x a <urn:T> } }")
    assert df.shape == (0, 2)
    assert df.columns == ["g", "x"]


def test_select_star(m):
    df = m.query("SELECT * WHERE { GRAPH ?g { ?x a <urn:T> } }")
    assert df.height == 0
    assert set(df.columns) == {"g", "x"}


def test_select_distinct_graph_variable(m):
    df = m.query("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }")
    assert df.shape == (0, 1)


def test_order_by_graph_variable(m):
    df = m.query("SELECT ?g ?x WHERE { GRAPH ?g { ?x a <urn:T> } } ORDER BY ?g")
    assert df.shape == (0, 2)


def test_optional_graph_variable(m):
    df = m.query(
        "SELECT ?x ?g WHERE { ?x a <urn:T> OPTIONAL { GRAPH ?g { ?x a <urn:T> } } }"
    )
    assert df.shape == (1, 2)
    assert df.get_column("x")[0] == "<urn:a>"
    assert df.get_column("g")[0] is None


def test_optional_graph_variable_group_by(m):
    df = m.query(
        "SELECT ?g (COUNT(?x) AS ?n) WHERE { ?x a <urn:T> OPTIONAL { GRAPH ?g { ?x a <urn:T> } } } GROUP BY ?g"
    )
    assert df.shape == (1, 2)
    assert df.get_column("g")[0] is None
    assert df.get_column("n")[0] == 1


def test_union_graph_variable(m):
    df = m.query(
        "SELECT ?x ?g WHERE { { ?x a <urn:T> } UNION { GRAPH ?g { ?x a <urn:T> } } }"
    )
    assert df.shape == (1, 2)
    assert df.get_column("g")[0] is None


def test_minus_graph_variable(m):
    df = m.query("SELECT ?x WHERE { ?x a <urn:T> MINUS { GRAPH ?g { ?x a <urn:T> } } }")
    assert df.shape == (1, 1)


def test_filter_not_exists_graph_variable(m):
    df = m.query(
        "SELECT ?x WHERE { ?x a <urn:T> FILTER NOT EXISTS { GRAPH ?g { ?x a <urn:T> } } }"
    )
    assert df.shape == (1, 1)


def test_filter_exists_graph_variable(m):
    df = m.query(
        "SELECT ?x WHERE { ?x a <urn:T> FILTER EXISTS { GRAPH ?g { ?x a <urn:T> } } }"
    )
    assert df.shape == (0, 1)


def test_graph_variable_bound_outside(m):
    df = m.query(
        "SELECT ?g ?x WHERE { VALUES ?g { <urn:g1> } GRAPH ?g { ?x a <urn:T> } }"
    )
    assert df.shape == (0, 2)


def test_named_graphs_excluded_by_from(m):
    m.reads(f"<urn:b> {RDF_TYPE} <urn:T> .\n", format="ntriples", graph="urn:g1")
    # Sanity check: the named graph is visible without a dataset clause.
    df = m.query(
        "SELECT ?g (COUNT(?x) AS ?n) WHERE { GRAPH ?g { ?x a <urn:T> } } GROUP BY ?g"
    )
    assert df.shape == (1, 2)
    # FROM without FROM NAMED gives a dataset with no named graphs.
    df = m.query(
        "SELECT ?g (COUNT(?x) AS ?n) FROM <urn:g1> WHERE { GRAPH ?g { ?x a <urn:T> } } GROUP BY ?g"
    )
    assert df.shape == (0, 2)
    assert df.columns == ["g", "n"]


def test_named_graphs_excluded_by_from_named(m):
    m.reads(f"<urn:b> {RDF_TYPE} <urn:T> .\n", format="ntriples", graph="urn:g1")
    # FROM NAMED a graph that does not exist: no solutions, ?g still in scope.
    df = m.query(
        "SELECT ?g (COUNT(?x) AS ?n) FROM NAMED <urn:missing> WHERE { GRAPH ?g { ?x a <urn:T> } } GROUP BY ?g"
    )
    assert df.shape == (0, 2)
    assert df.columns == ["g", "n"]
