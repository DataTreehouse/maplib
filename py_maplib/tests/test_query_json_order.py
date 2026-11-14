"""JSON query results list the variables, and give the solutions, in the order of the projection."""

import json

import pytest

from maplib import Model


@pytest.mark.parametrize("vars", [["name", "age", "p"], ["p", "name", "age"], ["age", "p", "name"]])
def test_json_results_keep_the_projection_order(vars):
    m = Model()
    m.reads('<http://example.org/a> <http://example.org/name> "A" ; <http://example.org/age> 1 .', format="turtle")
    q = f"SELECT ?{vars[0]} ?{vars[1]} ?{vars[2]} WHERE {{ ?p <http://example.org/name> ?name ; <http://example.org/age> ?age }}"
    res = json.loads(m.query(q, return_format="json"))
    assert res["head"]["vars"] == vars
    assert res["results"]["bindings"][0]["age"]["value"] == "1"
    empty = json.loads(m.query(q.replace("?age }", "?age . FILTER(false) }"), return_format="json"))
    assert empty["head"]["vars"] == vars
