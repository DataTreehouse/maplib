"""A variable used twice in a triple pattern (e.g. `?s <p> ?s`) when the predicate has triples
with several subject and object types."""
from maplib import Model

P = "<http://ex.org/p>"
DATA = f"""
<http://ex.org/c> {P} <http://ex.org/c> .
<http://ex.org/a> {P} <http://ex.org/b> .
<http://ex.org/a> {P} "x" .
_:b1 {P} <http://ex.org/a> .
"""
# Also a blank node that refers to itself, so the variable really has two types.
DATA_BLANK_LOOP = DATA + f"_:x {P} _:x .\n"


def model(data):
    m = Model()
    m.reads(data, format="ntriples")
    return m


def count_p(m):
    return m.query(f"SELECT (COUNT(*) AS ?c) WHERE {{ ?s {P} ?o FILTER(true) }}")["c"][0]


def test_query_same_variable_subject_object():
    m = model(DATA)
    df = m.query(f"SELECT ?s WHERE {{ ?s {P} ?s }}")
    assert df["s"].to_list() == ["<http://ex.org/c>"]


def test_query_same_variable_two_types():
    m = model(DATA_BLANK_LOOP)
    df = m.query(f"SELECT ?s WHERE {{ ?s {P} ?s }}")
    assert df.height == 2


def test_delete_same_variable_subject_object():
    m = model(DATA)
    m.update(f"DELETE WHERE {{ ?s {P} ?s }}")
    assert count_p(m) == 3
    assert m.query(f"SELECT ?s WHERE {{ ?s {P} ?s }}").height == 0


def test_delete_same_variable_two_types():
    m = model(DATA_BLANK_LOOP)
    m.update(f"DELETE WHERE {{ ?s {P} ?s }}")
    assert count_p(m) == 3
    assert m.query(f"SELECT ?s WHERE {{ ?s {P} ?s }}").height == 0


def test_same_variable_subject_predicate():
    m = model(DATA + f"{P} {P} <http://ex.org/z> .\n")
    df = m.query("SELECT ?x ?o WHERE { ?x ?x ?o }")
    assert df.height == 1
    assert df["x"][0] == P
