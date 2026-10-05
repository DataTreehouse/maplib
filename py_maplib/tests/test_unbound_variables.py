import pytest

from maplib import Model

# SPARQL 1.1 18.6: a variable that does not occur in the pattern is unbound in every
# solution. Expressions that use it must evaluate as with any other unbound variable
# instead of failing the query.


@pytest.fixture
def model():
    m = Model()
    m.reads(
        '<http://example.net/a> <http://example.net/p> "b" .\n'
        '<http://example.net/b> <http://example.net/p> "a" .\n',
        format="ntriples",
    )
    return m


def test_order_by_variable_not_in_pattern(model):
    df = model.query(
        "SELECT ?s WHERE { ?s <http://example.net/p> ?o } ORDER BY ?nope ?o"
    )
    assert df["s"].to_list() == ["<http://example.net/b>", "<http://example.net/a>"]


def test_distinct_order_by_variable_not_in_pattern(model):
    df = model.query(
        "SELECT DISTINCT ?s WHERE { ?s <http://example.net/p> ?o } ORDER BY ?nope"
    )
    assert sorted(df["s"].to_list()) == [
        "<http://example.net/a>",
        "<http://example.net/b>",
    ]


def test_bound_variable_not_in_pattern(model):
    df = model.query(
        "SELECT ?s WHERE { ?s <http://example.net/p> ?o FILTER(!BOUND(?nope)) }"
    )
    assert df.height == 2


def test_coalesce_variable_not_in_pattern(model):
    df = model.query(
        "SELECT ?s ?x WHERE { ?s <http://example.net/p> ?o "
        "BIND(COALESCE(?nope, ?o) AS ?x) } ORDER BY ?x"
    )
    assert df["x"].to_list() == ["a", "b"]


def test_filter_on_variable_not_in_pattern(model):
    df = model.query(
        'SELECT ?s WHERE { ?s <http://example.net/p> ?o FILTER(?nope = "a") }'
    )
    assert df.height == 0
