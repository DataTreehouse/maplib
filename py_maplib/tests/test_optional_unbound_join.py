"""Variables that an OPTIONAL or a UNION branch leaves unbound are compatible with any value
when they are joined with the other patterns of a group, also when the OPTIONAL comes first."""

import pytest

from maplib import Model

DATA = """
@prefix ex: <http://example.org/> .

ex:n0 ex:p ex:n1 .
ex:n1 ex:p ex:n2 .
ex:n0 ex:q ex:a .
ex:a ex:r "A" .
ex:u ex:t ex:v .
"""

PREFIX = "PREFIX ex: <http://example.org/>\n"


def ex(local: str) -> str:
    return f"<http://example.org/{local}>"


@pytest.fixture(scope="module")
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def rows(m: Model, query: str, cols: list[str]) -> set[tuple]:
    df = m.query(PREFIX + query)
    for c in cols:
        if c not in df.columns:
            df = df.with_columns(**{c: None})
    return set(df.select(cols).iter_rows())


def test_optional_first_without_match(model):
    q = "SELECT ?s ?o WHERE { OPTIONAL { ?s ex:none ?x } ?s ex:p ?o . }"
    assert rows(model, q, ["s", "o"]) == {
        (ex("n0"), ex("n1")),
        (ex("n1"), ex("n2")),
    }


def test_optional_last_without_match(model):
    q = "SELECT ?s ?o WHERE { ?s ex:p ?o . OPTIONAL { ?s ex:none ?x } }"
    assert rows(model, q, ["s", "o"]) == {
        (ex("n0"), ex("n1")),
        (ex("n1"), ex("n2")),
    }


def test_optional_first_with_partial_match(model):
    # The OPTIONAL binds ?s to ex:n0 only, which then restricts the triple pattern.
    q = "SELECT ?s ?o ?x WHERE { OPTIONAL { ?s ex:q ?x } ?s ex:p ?o . }"
    assert rows(model, q, ["s", "o", "x"]) == {(ex("n0"), ex("n1"), ex("a"))}


def test_optional_variable_used_after_optional(model):
    # The second row leaves ?x unbound, so it is compatible with every ?x of ex:r.
    q = "SELECT ?s ?o ?x ?y WHERE { ?s ex:p ?o OPTIONAL { ?s ex:q ?x } ?x ex:r ?y }"
    assert rows(model, q, ["s", "o", "x", "y"]) == {
        (ex("n0"), ex("n1"), ex("a"), "A"),
        (ex("n1"), ex("n2"), ex("a"), "A"),
    }


def test_two_optionals_first_without_match(model):
    q = "SELECT ?s ?o WHERE { OPTIONAL { ?s ex:none ?x } OPTIONAL { ?s ex:p ?o } }"
    assert rows(model, q, ["s", "o"]) == {
        (ex("n0"), ex("n1")),
        (ex("n1"), ex("n2")),
    }


def test_union_branch_without_shared_variable(model):
    q = (
        "SELECT ?s ?o ?x ?y WHERE { { ?s ex:q ?x } UNION { ?y ex:t ?z } "
        "?s ex:p ?o }"
    )
    assert rows(model, q, ["s", "o", "x", "y"]) == {
        (ex("n0"), ex("n1"), ex("a"), None),
        (ex("n0"), ex("n1"), None, ex("u")),
        (ex("n1"), ex("n2"), None, ex("u")),
    }


def test_values_undef_with_optional_first(model):
    # ?s is unbound in the OPTIONAL and in the second VALUES row.
    q = (
        "SELECT ?s ?o WHERE { OPTIONAL { ?s ex:none ?x } "
        "VALUES (?s ?o) { (ex:n0 ex:n1) (UNDEF ex:n2) } }"
    )
    assert rows(model, q, ["s", "o"]) == {(ex("n0"), ex("n1")), (None, ex("n2"))}
