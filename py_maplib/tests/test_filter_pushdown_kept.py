"""A FILTER must hold for every row of the result, wherever the filter pushdown rewrite moves it.

The rewrite moves the conjuncts of a FILTER over a group with a variable predicate (``?a ?p ?b``)
down to the patterns that bind their variables. A filter that reached a VALUES block, a property
path or a basic graph pattern without a variable predicate was dropped, so in

    SELECT ?n WHERE { ?a ?p ?b . VALUES ?p { <urn:p> } ?a <urn:name> ?n FILTER(?n = "A") }

FILTER(?n = "A") was ignored and the query also returned "C". A filter moved next to the
variables of a sibling pattern of a join could also run before the sibling had bound them, and
remove every row.
"""

from collections import Counter

import pytest

from maplib import Model

DATA = """
<urn:a> <urn:p> <urn:b> .
<urn:a> <urn:name> "A" .
<urn:a> <urn:q> "A" .
<urn:c> <urn:p> <urn:d> .
<urn:c> <urn:name> "C" .
"""


@pytest.fixture(scope="module")
def model():
    m = Model()
    m.reads(DATA, format="ntriples")
    return m


def rows(model, query, var):
    return Counter(model.query(query)[var].to_list())


CASES = [
    # The query from the portal review: VALUES between the triple patterns of a group.
    (
        'SELECT ?n WHERE { ?a ?p ?b . VALUES ?p { <urn:p> } ?a <urn:name> ?n FILTER(?n = "A") }',
        "n",
        {"A": 1},
    ),
    (
        'SELECT ?n WHERE { ?a ?p ?b . VALUES ?p { <urn:p> } ?a <urn:name> ?n . FILTER(?n = "A") }',
        "n",
        {"A": 1},
    ),
    # The same group without VALUES, and with VALUES first, were already right.
    (
        'SELECT ?n WHERE { ?a ?p ?b . ?a <urn:name> ?n FILTER(?n = "A" && ?p = <urn:p>) }',
        "n",
        {"A": 1},
    ),
    (
        'SELECT ?n WHERE { VALUES ?p { <urn:p> } ?a ?p ?b . ?a <urn:name> ?n FILTER(?n = "A") }',
        "n",
        {"A": 1},
    ),
    # A VALUES block that shares no variable with the patterns.
    (
        'SELECT ?n WHERE { ?a ?p ?b . VALUES ?z { 1 } ?a <urn:name> ?n FILTER(?n = "A") }',
        "n",
        {"A": 3},
    ),
    # A filter on the variable of the VALUES block.
    (
        "SELECT ?x WHERE { ?a ?p ?b . VALUES ?x { 1 2 } FILTER(?x = 1) }",
        "x",
        {1: 5},
    ),
    # A filter over the variables of both the VALUES block and the triple patterns.
    (
        'SELECT ?n WHERE { ?a <urn:name> ?n . ?a ?p ?b . VALUES ?q { "A" } FILTER(?n = ?q) }',
        "n",
        {"A": 3},
    ),
    # A filter on the end of a property path.
    (
        "SELECT ?z WHERE { ?a ?p ?b . ?a <urn:p>/<urn:p>? ?z FILTER(?z = <urn:b>) }",
        "z",
        {"<urn:b>": 3},
    ),
    # A filter between a sibling's variable (?q, from VALUES) and a triple pattern's (?n) was
    # moved next to the triple pattern and run before ?q was bound, which removed every row.
    (
        'SELECT ?n WHERE { VALUES ?q { "A" } ?a <urn:name> ?n . ?a ?p ?b FILTER(?n = ?q) }',
        "n",
        {"A": 3},
    ),
    # A variable bound only in some rows (OPTIONAL) on one side of a join must not have the
    # filter moved into that side: the other side binds it in the remaining rows.
    (
        "SELECT ?v WHERE { { ?a <urn:p> ?b OPTIONAL { ?a <urn:q> ?v } } "
        '?a ?p ?x . ?a <urn:name> ?v FILTER(?v = "C") }',
        "v",
        {"C": 2},
    ),
]


@pytest.mark.parametrize("query,var,expected", CASES)
def test_filter_holds_after_pushdown(model, query, var, expected):
    assert rows(model, query, var) == Counter(expected)
