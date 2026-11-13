import pytest
from maplib import Model

DATA = """
<http://ex/s> <http://ex/p> _:e .
_:e <http://ex/p> _:e .
<http://ex/s> <http://ex/q> "lit" .
"""


@pytest.fixture
def m():
    m = Model()
    m.reads(DATA, format="ntriples")
    return m


def test_same_variable_subject_object_only_blank_matches(m):
    # The subject column holds IRIs and blank nodes, but only the blank self-loop matches.
    df = m.query("SELECT ?a WHERE {?a ?p ?a}")
    assert len(df) == 1
    assert df["a"][0].startswith("_:")
    df = m.query("SELECT ?a WHERE {?a <http://ex/p> ?a}")
    assert len(df) == 1
    assert df["a"][0].startswith("_:")


def test_same_variable_no_type_compatible_slices(m):
    # The only object is a literal, which can never equal an IRI subject.
    df = m.query("SELECT ?a WHERE {?a <http://ex/q> ?a}")
    assert len(df) == 0
    # No subject is also used as a predicate.
    df = m.query("SELECT ?a ?o WHERE {?a ?a ?o}")
    assert len(df) == 0
