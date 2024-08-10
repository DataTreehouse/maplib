import polars as pl
import pytest

from maplib import (
    Mapping,
    Template,
    IRI,
    Prefix,
    Triple,
    Variable,
    Parameter,
    XSD,
    RDFType,
)
from polars.testing import assert_frame_equal


def test_want_float_got_int64():
    xsd = XSD()
    df = pl.DataFrame({"MyValue": [1]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal(xsd.float))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    with pytest.raises(Exception):
        mapping.expand(template, df)
