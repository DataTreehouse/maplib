import json

import pytest
from maplib import Model

# DISTINCT over a UNION used to return its columns in hash order. The union gives ?o and ?x
# multi-type columns, and the DISTINCT (which runs after the projection) appended them at the
# end in a random order. Fresh models are used each round since the order varied between runs.

DATA = "<urn:a> <urn:p> <urn:b> .\n<urn:a> <urn:q> <urn:c> .\n<urn:d> <urn:p> \"lit\" ."
P = "?s <urn:p> ?o OPTIONAL { ?s <urn:q> ?x }"
WHERE = f"{{ {{ {P} }} UNION {{ GRAPH ?g {{ {P} }} }} }}"


def _model():
    m = Model()
    m.reads(DATA, format="ntriples")
    m.reads(DATA, format="ntriples", graph="urn:g")
    return m


@pytest.mark.parametrize(
    "query, expected",
    [
        (f"SELECT DISTINCT ?s ?o ?x WHERE {WHERE}", ["s", "o", "x"]),
        (f"SELECT DISTINCT ?x ?s ?o WHERE {WHERE}", ["x", "s", "o"]),
        (f"SELECT DISTINCT ?o ?x ?s WHERE {WHERE} ORDER BY ?s ?o", ["o", "x", "s"]),
        (f"SELECT DISTINCT ?s ?o ?x WHERE {WHERE} LIMIT 10", ["s", "o", "x"]),
        (
            f"SELECT DISTINCT ?x ?o ?s WHERE {WHERE} ORDER BY DESC(?o) LIMIT 2 OFFSET 1",
            ["x", "o", "s"],
        ),
    ],
)
def test_distinct_union_keeps_projection_order(query, expected):
    for _ in range(20):
        df = _model().query(query)
        assert df.columns == expected


def test_distinct_union_json_head_order():
    for _ in range(20):
        res = json.loads(
            _model().query(f"SELECT DISTINCT ?s ?o ?x WHERE {WHERE}", return_format="json")
        )
        assert res["head"]["vars"] == ["s", "o", "x"]
