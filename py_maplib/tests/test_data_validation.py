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

def test_want_rdfs_literal_got_int64():
    df = pl.DataFrame({"MyValue": [1]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal("http://www.w3.org/2000/01/rdf-schema#Literal"))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    mapping.expand(template, df)
    r = mapping.query("""
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """, include_datatypes=True)
    assert r.rdf_types["c"] == RDFType.Literal(XSD().long)

def test_want_rdfs_resource_got_int64():
    df = pl.DataFrame({"MyValue": [1]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal("http://www.w3.org/2000/01/rdf-schema#Resource"))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    mapping.expand(template, df)
    r = mapping.query("""
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """, include_datatypes=True)
    assert r.rdf_types["c"] == RDFType.Literal(XSD().long)

def test_want_xsd_int_got_xsd_boolean():
    xsd = XSD()
    df = pl.DataFrame({"MyValue": [True]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal(xsd.int_))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    with pytest.raises(Exception):
        mapping.expand(template, df)

def test_want_xsd_long_got_xsd_short():
    xsd = XSD()
    df = pl.DataFrame({"MyValue": [1]})
    df = df.with_columns(
        pl.col("MyValue").cast(pl.Int16)
    )
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal(xsd.long))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    mapping.expand(template, df)
    r = mapping.query("""
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """, include_datatypes=True)
    assert r.rdf_types["c"] == RDFType.Literal(XSD().short)
