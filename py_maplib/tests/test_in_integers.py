"""?x IN (integer, ...) with only integer literals in the list is evaluated as an is_in lookup when
?x holds only integers of one datatype. These tests check that it gives the same results as the
equivalent ||-chain of = comparisons, which is evaluated the general way.
"""

import polars as pl
import pytest

from maplib import Model

DATA = """
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:a ex:p 1 .
ex:b ex:p 2 .
ex:c ex:p 3 .
ex:d ex:p -4 .
ex:e ex:i "2"^^xsd:int .
ex:f ex:i "5"^^xsd:int .
ex:g ex:u "7"^^xsd:unsignedByte .
ex:h ex:m 2 .
ex:h ex:m "2" .
ex:h ex:m 2.5 .
ex:a ex:q 1 .
ex:b ex:q 7 .
"""

PREFIX = "PREFIX ex: <http://example.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"


@pytest.fixture(scope="module")
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def rows(m: Model, query: str) -> list:
    df = m.query(PREFIX + query)
    df = df.select(sorted(df.columns)).with_columns(pl.all().cast(pl.String))
    return sorted(df.rows(), key=str)


@pytest.mark.parametrize(
    "pred,in_filter,eq_filter",
    [
        ("ex:p", "?v IN (1, 3)", "?v = 1 || ?v = 3"),
        ("ex:p", "?v IN (-4, 100)", "?v = -4 || ?v = 100"),
        ("ex:p", "?v IN (+2, 0003)", "?v = +2 || ?v = 0003"),
        ("ex:p", "?v IN (42)", "?v = 42"),
        ("ex:p", "?v NOT IN (1, 3)", "!(?v = 1 || ?v = 3)"),
        # Integers of other integer datatypes are equal when their values are
        ("ex:p", '?v IN ("2"^^xsd:int, "3"^^xsd:long)', '?v = "2"^^xsd:int || ?v = "3"^^xsd:long'),
        ("ex:i", "?v IN (2, 3)", "?v = 2 || ?v = 3"),
        ("ex:u", '?v IN (7, "8"^^xsd:short)', '?v = 7 || ?v = "8"^^xsd:short'),
        # Lists with other literals and left sides with other types are evaluated the general way
        ("ex:p", "?v IN (2.0, 3)", "?v = 2.0 || ?v = 3"),
        ("ex:p", '?v IN ("2", 3)', '?v = "2" || ?v = 3'),
        ("ex:m", "?v IN (2, 3)", "?v = 2 || ?v = 3"),
    ],
)
def test_in_integers_matches_equality_chain(model, pred, in_filter, eq_filter):
    query = "SELECT ?s ?v WHERE {{ ?s {} ?v FILTER({}) }}"
    assert rows(model, query.format(pred, in_filter)) == rows(
        model, query.format(pred, eq_filter)
    )


@pytest.mark.parametrize("op", ["IN", "NOT IN"])
def test_in_integers_unbound(model, op):
    # ?o is unbound for most subjects: IN must treat these rows as the general evaluation does.
    general = "(1, 2, 2.5)"
    fast = "(1, 2)"
    for form in [
        "SELECT ?s ?in WHERE {{ ?s ex:p ?v . OPTIONAL {{ ?s ex:q ?o }} BIND(?o {} {} AS ?in) }}",
        "SELECT ?s WHERE {{ ?s ex:p ?v . OPTIONAL {{ ?s ex:q ?o }} FILTER(?o {} {}) }}",
    ]:
        assert rows(model, form.format(op, fast)) == rows(model, form.format(op, general))


def test_in_integers_expected_rows(model):
    assert rows(model, "SELECT ?s WHERE { ?s ex:p ?v FILTER(?v IN (1, 3, 99)) }") == [
        ("<http://example.org/a>",),
        ("<http://example.org/c>",),
    ]
