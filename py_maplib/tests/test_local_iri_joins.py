"""Joins of IRIs that are not in the model (from VALUES or BIND).

Such IRIs get local dictionary codes, and the local codes of two patterns may give
different IRIs the same code. A join, OPTIONAL, MINUS or EXISTS must compare the IRIs,
not their codes.
"""

import pytest
from maplib import Model

IN_MODEL = "<urn:m:x> <urn:m:p> <urn:m:y> .\n"


@pytest.fixture(params=["empty", "with_triples"])
def model(request):
    m = Model()
    if request.param == "with_triples":
        m.reads(IN_MODEL, format="ntriples")
    return m


def xs(df, var="x"):
    return sorted(v for v in df.get_column(var).to_list())


def test_join_of_two_singular_values(model):
    df = model.query("SELECT ?x WHERE { VALUES ?x { <urn:n1> } VALUES ?x { <urn:n2> } }")
    assert df.height == 0


def test_join_of_two_binds(model):
    df = model.query(
        "SELECT ?x WHERE { { BIND(<urn:n1> AS ?x) } { BIND(<urn:n2> AS ?x) } }"
    )
    assert df.height == 0


def test_join_keeps_the_shared_iri_only(model):
    df = model.query(
        "SELECT ?x WHERE { VALUES ?x { <urn:n1> <urn:n3> } VALUES ?x { <urn:n2> <urn:n1> } }"
    )
    assert xs(df) == ["<urn:n1>"]


def test_join_with_an_iri_of_the_model(model):
    df = model.query(
        "SELECT ?x WHERE { VALUES ?x { <urn:n1> <urn:m:x> } VALUES ?x { <urn:m:x> <urn:n2> } }"
    )
    assert xs(df) == ["<urn:m:x>"]


def test_join_with_the_triples(model):
    df = model.query(
        "SELECT ?x ?y WHERE { VALUES ?x { <urn:n1> <urn:m:x> } ?x <urn:m:p> ?y }"
    )
    if model.query("ASK { ?s ?p ?o }"):
        assert df.rows() == [("<urn:m:x>", "<urn:m:y>")]
    else:
        assert df.height == 0


def test_optional_does_not_match_another_iri(model):
    df = model.query(
        "SELECT ?x ?y WHERE { VALUES ?x { <urn:n1> } OPTIONAL { VALUES (?x ?y) { (<urn:n2> 1) } } }"
    )
    assert df.height == 1
    assert df.get_column("x").to_list() == ["<urn:n1>"]
    assert df.get_column("y").null_count() == 1


def test_optional_matches_the_same_iri(model):
    df = model.query(
        "SELECT ?x ?y WHERE { VALUES ?x { <urn:n1> <urn:n3> } OPTIONAL { VALUES (?x ?y) { (<urn:n2> 1) (<urn:n1> 2) } } } ORDER BY ?x"
    )
    assert df.get_column("x").to_list() == ["<urn:n1>", "<urn:n3>"]
    assert df.get_column("y").cast(int).to_list() == [2, None]


def test_minus_removes_only_the_same_iri(model):
    df = model.query(
        "SELECT ?x WHERE { VALUES ?x { <urn:n1> <urn:n3> } MINUS { VALUES ?x { <urn:n2> <urn:n3> } } }"
    )
    assert xs(df) == ["<urn:n1>"]


def test_minus_of_another_iri_removes_nothing(model):
    df = model.query(
        "SELECT ?x WHERE { { BIND(<urn:n1> AS ?x) } MINUS { BIND(<urn:n2> AS ?x) } }"
    )
    assert xs(df) == ["<urn:n1>"]


def test_exists_compares_the_iris(model):
    df = model.query(
        "SELECT ?x WHERE { VALUES ?x { <urn:n1> <urn:n3> } FILTER EXISTS { VALUES ?x { <urn:n2> <urn:n3> } } }"
    )
    assert xs(df) == ["<urn:n3>"]


def test_not_exists_compares_the_iris(model):
    df = model.query(
        "SELECT ?x WHERE { VALUES ?x { <urn:n1> <urn:n3> } FILTER NOT EXISTS { VALUES ?x { <urn:n2> <urn:n3> } } }"
    )
    assert xs(df) == ["<urn:n1>"]


def test_join_of_two_bound_literals(model):
    # strings made by BIND are compared by value too
    df = model.query(
        'SELECT ?x WHERE { { BIND("only here" AS ?x) } { BIND("elsewhere" AS ?x) } }'
    )
    assert df.height == 0
