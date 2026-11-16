"""Regression tests for FILTER (NOT) EXISTS under variable-predicate patterns.

maplib's filter pushdown rewrite only runs when a pattern has a variable predicate,
and the W3C SPARQL 1.1 EXISTS/negation tests have none, so they do not cover this.
"""

import pytest
from maplib import Model

DATA = """
<urn:S1> <urn:c> <urn:L1> .
<urn:L1> <urn:d> <urn:E1> .
"""


@pytest.fixture
def m():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def iris(df, col):
    return sorted(df.get_column(col).to_list())


def test_not_exists_with_values_and_variable_predicate(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        VALUES ?r { <urn:c> <urn:d> } ?e ?r ?ch .
        FILTER NOT EXISTS { ?p <urn:c> ?e } }"""
    )
    assert iris(df, "e") == ["<urn:S1>"]


def test_minus_matches_not_exists(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        VALUES ?r { <urn:c> <urn:d> } ?e ?r ?ch .
        MINUS { ?p <urn:c> ?e } }"""
    )
    assert iris(df, "e") == ["<urn:S1>"]


def test_not_exists_variable_predicate_without_values(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        ?e ?r ?ch . FILTER NOT EXISTS { ?p <urn:c> ?e } }"""
    )
    assert iris(df, "e") == ["<urn:S1>"]


def test_exists_variable_predicate(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        ?e ?r ?ch . FILTER EXISTS { ?p <urn:c> ?e } }"""
    )
    assert iris(df, "e") == ["<urn:L1>"]


def test_not_exists_with_union(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        { ?e <urn:c> ?ch } UNION { ?e <urn:d> ?ch }
        FILTER NOT EXISTS { ?p <urn:c> ?e } }"""
    )
    assert iris(df, "e") == ["<urn:S1>"]


def test_not_exists_with_union_variable_predicate(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        { ?e ?r ?ch } UNION { ?ch ?r ?e }
        FILTER NOT EXISTS { ?p <urn:c> ?e } }"""
    )
    assert iris(df, "e") == ["<urn:E1>", "<urn:S1>"]


def test_not_exists_with_in_filter(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        ?e ?r ?ch . FILTER(?r IN (<urn:c>, <urn:d>))
        FILTER NOT EXISTS { ?p <urn:c> ?e } }"""
    )
    assert iris(df, "e") == ["<urn:S1>"]


def test_not_exists_with_values_and_in_inside(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        VALUES ?r { <urn:c> <urn:d> } ?e ?r ?ch .
        FILTER NOT EXISTS { ?p ?q ?e . FILTER(?q IN (<urn:c>)) } }"""
    )
    assert iris(df, "e") == ["<urn:S1>"]


def test_not_exists_with_values_inside(m):
    df = m.query(
        """SELECT DISTINCT ?e WHERE {
        ?e ?r ?ch .
        FILTER NOT EXISTS { VALUES ?q { <urn:c> } ?p ?q ?e } }"""
    )
    assert iris(df, "e") == ["<urn:S1>"]
