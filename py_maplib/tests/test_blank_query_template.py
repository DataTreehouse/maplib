import pathlib

import polars as pl
import pytest

from maplib import (
    Model,
    Template,
    IRI,
    Prefix,
    Triple,
    Variable,
    Parameter,
    Literal,
    xsd,
    RDFType, rdf,
)
from polars.testing import assert_frame_equal
PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

def test_construct_with_blank():
    m = Model()
    m.read(TESTDATA_PATH / "read_ntriples.nt")
    res = m.query("""
    PREFIX :<urn:maplib:ex> 
    CONSTRUCT {
        ?a :pred [a :bdc ].
    } WHERE {
        ?a a ?b .
    }
    """)
    assert len(res) == 2
    for r in res:
        assert r.height == 2

def test_insert_with_blank():
    m = Model()
    m.read(TESTDATA_PATH / "read_ntriples.nt")
    m.update("""
    PREFIX :<urn:maplib:ex> 
    INSERT {
        ?a :pred [a :bdc ].
    } WHERE {
        ?a a ?b .
    }
    """)

    df = m.query("""
    SELECT ?a ?b ?c WHERE {?a ?b ?c}
    """)
    assert df.height == 12
