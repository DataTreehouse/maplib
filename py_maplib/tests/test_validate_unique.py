import pytest
from maplib import Mapping, Triple, Variable, Template, IRI, Parameter, RDFType
import polars as pl


def test_many_validation_errors_non_unique():
    n = 10_000
    df = pl.DataFrame(
        {
            "subject": [f"urn:dth:goodiri{i%100}" for i in range(n)],
            "predicate": ["https://example.net/hasObject"] * n,
            "object": [i for i in range(n)],
        }
    )
    m = Mapping()
    subj = Variable("subject")
    pred = Variable("predicate")
    obj = Variable("object")
    t = Template(
        IRI("https://github.com/DataTreehouse/maplib/my_template"),
        [subj, pred, obj],
        [Triple(subj, pred, obj)],
    )
    with pytest.raises(Exception) as e:
        m.expand(t, df, unique_subset=["subject"], validate_unique_subset=True)
    assert "duplicated" in str(e)
