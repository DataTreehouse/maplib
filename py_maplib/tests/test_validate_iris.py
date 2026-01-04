import pytest
from maplib import Model, Triple, Variable, Template, IRI, Parameter, RDFType
import polars as pl


def test_many_validation_errors_subject():
    n = 10_000
    df = pl.DataFrame(
        {
            "subject": [f"MyBadIRI//{i}" for i in range(n)],
            "predicate": ["https://example.net/hasObject"] * n,
            "object": [i for i in range(n)],
        }
    )
    m = Model()
    subj = Variable("subject")
    pred = Variable("predicate")
    obj = Variable("object")
    t = Template(
        IRI("https://github.com/DataTreehouse/maplib/my_template"),
        [subj, pred, obj],
        [Triple(subj, pred, obj)],
    )
    with pytest.raises(Exception) as e:
        m.map(t, df, validate_iris=True)
    assert "invalid IRI" in str(e)


def test_many_validation_errors_predicate():
    n = 10_000
    df = pl.DataFrame(
        {
            "subject": [f"urn:dth:goodiri_{i}" for i in range(n)],
            "predicate": ["!https://example.net/hasObject"] * n,
            "object": [i for i in range(n)],
        }
    )
    m = Model()
    subj = Variable("subject")
    pred = Variable("predicate")
    obj = Variable("object")
    t = Template(
        IRI("https://github.com/DataTreehouse/maplib/my_template"),
        [subj, pred, obj],
        [Triple(subj, pred, obj)],
    )
    with pytest.raises(Exception) as e:
        m.map(t, df, validate_iris=True)
    assert "invalid IRI" in str(e)


def test_many_validation_errors_nested_object():
    n = 1_000
    df = pl.DataFrame(
        {
            "subject": [f"urn:dth:goodiri_{i}" for i in range(n)],
            "predicate": ["https://example.net/hasObject"] * n,
            "object": [
                [f"aTerribleIRI!!{i}", f"anotherTerribleIRI!!{i}"] for i in range(n)
            ],
        }
    )
    m = Model()
    subj = Variable("subject")
    pred = Variable("predicate")
    obj = Variable("object")
    t = Template(
        IRI("https://github.com/DataTreehouse/maplib/my_template"),
        [subj, pred, Parameter(obj, rdf_type=RDFType.Nested(RDFType.IRI))],
        [Triple(subj, pred, obj)],
    )
    with pytest.raises(Exception) as e:
        m.map(t, df, validate_iris=True)
    assert "invalid IRI" in str(e)
