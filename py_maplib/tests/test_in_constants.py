"""IN and NOT IN over mixed term types, and a long IN list of IRIs."""

import time

import pytest
from maplib import Model

DATA = """
<http://a> <http://p> <http://x> .
<http://b> <http://p> <http://y> .
<http://c> <http://p> "x" .
<http://d> <http://p> "y"@en .
<http://e> <http://p> 1 .
<http://f> <http://p> 2.0 .
<http://g> <http://p> "x"^^<http://example.org/dt> .
"""


@pytest.fixture(scope="module")
def m():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def subjects(m, filter_expr):
    df = m.query(f"SELECT ?s WHERE {{ ?s <http://p> ?o FILTER({filter_expr}) }}")
    return sorted(df.get_column("s").to_list())


@pytest.mark.parametrize(
    "filter_expr,expected",
    [
        ("?o IN (<http://x>)", ["<http://a>"]),
        ("?o IN (<http://x>, <http://y>, <http://nothere>)", ["<http://a>", "<http://b>"]),
        ('?o IN ("x")', ["<http://c>"]),
        ('?o IN ("y")', []),
        ('?o IN ("y"@en)', ["<http://d>"]),
        ('?o IN ("x", "y"@en, <http://y>)', ["<http://b>", "<http://c>", "<http://d>"]),
        # Numbers are compared by value
        ("?o IN (1.0, 2)", ["<http://e>", "<http://f>"]),
        ('?o IN ("x"^^<http://example.org/dt>)', ["<http://g>"]),
        ("?o IN ()", []),
        ("?o NOT IN (<http://x>, \"x\", 1)", ["<http://b>", "<http://d>", "<http://f>", "<http://g>"]),
        ("?s IN (<http://a>, <http://e>)", ["<http://a>", "<http://e>"]),
        # An IRI is not equal to the string with the same characters
        ('?o IN ("http://x")', []),
        ("?o IN (?s, <http://x>)", ["<http://a>"]),
    ],
)
def test_in(m, filter_expr, expected):
    assert subjects(m, filter_expr) == expected


def test_in_values_not_in_store():
    # Values from VALUES are not encoded in the store
    m = Model()
    df = m.query(
        'SELECT ?v WHERE { VALUES ?v { <http://new1> <http://new2> "s1" "s2" } FILTER(?v IN (<http://new2>, "s1")) }'
    )
    assert sorted(df.get_column("v").to_list()) == ['"s1"', "<http://new2>"]


def test_in_bind(m):
    df = m.query("SELECT ?s ?b WHERE { ?s <http://p> ?o BIND(?o IN (<http://y>, \"x\") AS ?b) }")
    got = dict(zip(df.get_column("s").to_list(), df.get_column("b").to_list()))
    assert got["<http://b>"] is True
    assert got["<http://c>"] is True
    assert got["<http://a>"] is False
    assert got["<http://e>"] is False


def test_long_in_list_is_fast():
    m = Model()
    n = 20_000
    m.reads("".join(f"<http://s{i}> <http://p> <http://o{i}> .\n" for i in range(n)), format="ntriples")
    in_list = ", ".join(f"<http://o{i}>" for i in range(0, 2 * n, 2))
    start = time.time()
    df = m.query(f"SELECT ?s WHERE {{ ?s <http://p> ?o FILTER(?o IN ({in_list})) }}")
    assert time.time() - start < 30
    assert df.height == n // 2
    df = m.query(f"SELECT ?s WHERE {{ ?s <http://p> ?o FILTER(?o NOT IN ({in_list})) }}")
    assert df.height == n // 2
