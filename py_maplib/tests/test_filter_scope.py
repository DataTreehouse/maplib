"""A FILTER applies to the group it is in (SPARQL 1.1 18.2.2).

A variable that only an enclosing group binds is unbound inside a nested group's FILTER and in
the condition of an OPTIONAL, also when maplib evaluates the nested group with the solutions of
the enclosing one as input. Inside EXISTS and LATERAL the outer solution is substituted into the
pattern, so there the variable stays visible. The cases are the DAWG tests
algebra/filter-nested-2, algebra/filter-scope-1 and
optional-filter/dawg-optional-filter-005-not-simplified.

A FILTER in a group nested inside EXISTS, EXISTS { P . { FILTER(?w = ?outer) } }, loses the
substituted variable; that is a separate, older bug and is not covered here.
"""

import pytest

from maplib import Model


@pytest.fixture(scope="module")
def model():
    m = Model()
    m.reads(
        """
        @prefix : <http://example/> .
        :x :p 1, 2, 3, 4 .
        :x :q 1, 2, 3 .
        :b1 :title "T1" .
        :b2 :title "T2" ; :price 20 .
        :b3 :title "T3" ; :price 30 .
        """,
        format="turtle",
    )
    return m


def rows(df):
    return sorted(tuple(None if v is None else str(v) for v in r) for r in df.iter_rows())


def test_filter_in_nested_group_does_not_see_outer_variable(model):
    df = model.query(
        "PREFIX : <http://example/> SELECT ?v { :x :p ?v . { FILTER(?v = 1) } }"
    )
    assert df.height == 0


def test_filter_in_same_group_sees_variable(model):
    df = model.query("PREFIX : <http://example/> SELECT ?v { :x :p ?v . FILTER(?v = 1) }")
    assert rows(df) == [("1",)]


def test_optional_condition_does_not_see_outer_variable(model):
    df = model.query(
        """PREFIX : <http://example/>
        SELECT ?v ?w ?v2 { :x :p ?v . { :x :q ?w OPTIONAL { :x :p ?v2 FILTER(?v = 1) } } }"""
    )
    # ?v is unbound in the OPTIONAL's condition, so the condition fails and ?v2 is unbound.
    assert df.height == 4 * 3
    assert df["v2"].null_count() == df.height


def test_filter_in_group_inside_optional(model):
    df = model.query(
        """PREFIX : <http://example/>
        SELECT ?title ?price {
            ?book :title ?title .
            OPTIONAL { { ?book :price ?price . FILTER(?title = "T2") } }
        }"""
    )
    assert rows(df) == [("T1", None), ("T2", None), ("T3", None)]


def test_optional_condition_on_left_variable_still_applies(model):
    df = model.query(
        """PREFIX : <http://example/>
        SELECT ?title ?price {
            ?book :title ?title .
            OPTIONAL { ?book :price ?price . FILTER(?title = "T2") }
        }"""
    )
    assert rows(df) == [("T1", None), ("T2", "20"), ("T3", None)]


def test_exists_still_sees_outer_variable(model):
    # The outer solution is substituted into the EXISTS pattern, so its FILTER sees ?v.
    df = model.query(
        """PREFIX : <http://example/>
        SELECT ?v { :x :p ?v . FILTER EXISTS { :x :q ?w . FILTER(?w = ?v) } }"""
    )
    assert rows(df) == [("1",), ("2",), ("3",)]
