"""Joins with no variables to join on: the cartesian product (no shared variables), OPTIONAL
without shared variables, and shared variables whose types can never be equal."""

import pytest

from maplib import Model

DATA = """
@prefix ex: <http://example.org/> .

ex:a ex:p ex:x .
ex:b ex:p ex:y .
ex:c ex:p ex:z .
ex:k ex:q 1 .
ex:l ex:q 2 .
ex:m ex:s "m" .
"""

PREFIX = "PREFIX ex: <http://example.org/>\n"


@pytest.fixture(scope="module")
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def q(m: Model, query: str):
    return m.query(PREFIX + query)


def test_cartesian_product(model):
    df = q(model, "SELECT ?s ?o ?t ?n WHERE { ?s ex:p ?o . ?t ex:q ?n }")
    assert df.height == 6
    pairs = {(s, t) for s, t in zip(df["s"].to_list(), df["t"].to_list())}
    assert len(pairs) == 6


def test_cartesian_product_with_empty_side(model):
    df = q(model, "SELECT ?s ?t WHERE { ?s ex:p ?o . ?t ex:nothing ?n }")
    assert df.height == 0


def test_optional_without_shared_variables(model):
    df = q(model, "SELECT ?s ?t WHERE { ?s ex:p ?o OPTIONAL { ?t ex:q ?n } }")
    assert df.height == 6


def test_optional_without_shared_variables_empty_right(model):
    # OPTIONAL with no solutions keeps every left solution, with ?t unbound.
    df = q(model, "SELECT ?s ?t WHERE { ?s ex:p ?o OPTIONAL { ?t ex:nothing ?n } }")
    assert df.height == 3
    assert df["t"].null_count() == 3


def test_shared_variable_types_never_equal(model):
    # ?o is an IRI on the left and an integer on the right: no solutions are compatible.
    df = q(model, "SELECT ?s ?o ?t WHERE { ?s ex:p ?o . ?t ex:q ?o }")
    assert df.height == 0


def test_optional_shared_variable_types_never_equal(model):
    df = q(model, "SELECT ?s ?o ?t WHERE { ?s ex:p ?o OPTIONAL { ?t ex:q ?o } }")
    assert df.height == 3
    assert df["t"].null_count() == 3
    assert df["o"].null_count() == 0
