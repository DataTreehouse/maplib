"""?x IN (iri, ...) with only IRIs in the list is evaluated as a lookup of dictionary codes when ?x
holds only IRIs. These tests check that it gives the same results as the equivalent ||-chain of
= comparisons, which is evaluated the general way.
"""

import polars as pl
import pytest

from maplib import Model

DATA = """
@prefix ex: <http://example.org/> .

ex:a ex:p ex:x .
ex:b ex:p ex:y .
ex:c ex:p ex:z .
ex:d ex:p "x" .
ex:e ex:p 1 .
ex:a ex:q ex:a .
ex:f ex:r ex:x .
"""

PREFIX = "PREFIX ex: <http://example.org/>\n"


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
    "in_filter,eq_filter",
    [
        # IRI-valued variable, all list items in the dictionary
        ("?o IN (ex:x, ex:y)", "?o = ex:x || ?o = ex:y"),
        # An IRI that is not in the dictionary matches nothing
        ("?o IN (ex:x, ex:nope)", "?o = ex:x || ?o = ex:nope"),
        ("?o IN (ex:nope)", "?o = ex:nope"),
        # Mixed IRI and literal values on the left side
        ("?v IN (ex:x, ex:z)", "?v = ex:x || ?v = ex:z"),
        # A list with a literal is evaluated the general way
        ('?v IN (ex:x, "x")', '?v = ex:x || ?v = "x"'),
        # The subject: IRIs
        ("?s IN (ex:a, ex:c)", "?s = ex:a || ?s = ex:c"),
        ("?o NOT IN (ex:x, ex:y)", "!(?o = ex:x || ?o = ex:y)"),
    ],
)
def test_in_iris_matches_equality_chain(model, in_filter, eq_filter):
    query = "SELECT ?s ?v ?o WHERE {{ ?s ex:p ?v . OPTIONAL {{ ?s ex:p ?o FILTER(isIRI(?o)) }} FILTER({}) }}"
    assert rows(model, query.format(in_filter)) == rows(model, query.format(eq_filter))


def test_in_iris_bind_unbound(model):
    # ?o is unbound for most subjects: IN must treat these rows as the = chain does.
    query = (
        "SELECT ?s ?in WHERE {{ ?s ex:p ?v . OPTIONAL {{ ?s ex:q ?o }} BIND({} AS ?in) }}"
    )
    assert rows(model, query.format("?o IN (ex:a, ex:b)")) == rows(
        model, query.format("?o = ex:a || ?o = ex:b")
    )


def test_in_iris_expected_rows(model):
    assert rows(model, "SELECT ?s WHERE { ?s ex:p ?o FILTER(?o IN (ex:x, ex:z, ex:nope)) }") == [
        ("<http://example.org/a>",),
        ("<http://example.org/c>",),
    ]
