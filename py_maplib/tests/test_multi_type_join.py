"""Joins on a variable that holds several kinds of terms on both sides.

When the two sides share only one kind of term for a variable (here IRIs, with literals on the
left and blank nodes on the right), the join used to keep the rows of the other kinds with a null
key, and nulls matched each other: "v" joined _:b. Found through owl:sameAs reasoning, where the
Datalog engine joins the new triples of a round with the stored ones.
"""
import json

from maplib import Model

DATA = """
@prefix ex: <http://ex.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:p "v" .
ex:a ex:p ex:c .
ex:a ex:p "1"^^xsd:integer .
_:b ex:q ex:z .
ex:c ex:q ex:w .
ex:d ex:q "2"^^xsd:decimal .
"""


def rows(m: Model, query: str):
    res = json.loads(m.query("PREFIX ex: <http://ex.org/>\n" + query, return_format="json"))
    out = set()
    for b in res["results"]["bindings"]:
        out.add(tuple(sorted((k, v["value"]) for k, v in b.items())))
    return out


def test_optional_on_variable_with_different_kinds_of_terms():
    m = Model()
    m.reads(DATA, format="turtle")
    got = rows(
        m,
        """SELECT ?x ?y ?z WHERE {
            { SELECT ?x ?y WHERE { ?x ex:p ?y } }
            OPTIONAL { SELECT ?y ?z WHERE { ?y ex:q ?z } }
        }""",
    )
    assert got == {
        (("x", "http://ex.org/a"), ("y", "http://ex.org/c"), ("z", "http://ex.org/w")),
        (("x", "http://ex.org/a"), ("y", "v")),
        (("x", "http://ex.org/a"), ("y", "1")),
    }


def test_join_on_variable_with_different_kinds_of_terms():
    m = Model()
    m.reads(DATA, format="turtle")
    got = rows(
        m,
        """SELECT ?x ?y ?z WHERE {
            { SELECT ?x ?y WHERE { ?x ex:p ?y } }
            { SELECT ?y ?z WHERE { ?y ex:q ?z } }
        }""",
    )
    assert got == {
        (("x", "http://ex.org/a"), ("y", "http://ex.org/c"), ("z", "http://ex.org/w")),
    }
