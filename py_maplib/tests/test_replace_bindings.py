import polars as pl
import pytest

import maplib
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
    SolutionMappings,
)
from polars.testing import assert_frame_equal

def test_bindings_literal_class():
    q = """
    SELECT * WHERE {
        VALUES ?a {"a" "b"}
        FILTER(?a = ?b)
    }
    """
    m = Model()
    df = m.query(q, bindings={"b": Literal("b", xsd.string)})
    assert df.height == 1

def test_bindings_literal_int():
    q = """
    SELECT * WHERE {
        VALUES ?a {"a" "1"^^<http://www.w3.org/2001/XMLSchema#int>}
        FILTER(?a = ?b)
    }
    """
    m = Model()
    df = m.query(q, bindings={"b": '"1"^^<http://www.w3.org/2001/XMLSchema#int>'})
    assert df.height == 1

def test_bindings_literal_string():
    q = """
    SELECT * WHERE {
        VALUES ?a {"a" "b"}
        FILTER(?a = ?b)
    }
    """
    m = Model()
    df = m.query(q, bindings={"b": '"b"'})
    assert df.height == 1

def test_bindings_iri():
    q = """
    SELECT * WHERE {
        VALUES ?a {"a" <urn:abc>}
        FILTER(?a = ?b)
    }
    """
    m = Model()
    df = m.query(q, bindings={"b": "<urn:abc>"})
    assert df.height == 1

def test_bindings_iri_class():
    q = """
    SELECT * WHERE {
        VALUES ?a {"a" <urn:abc>}
        FILTER(?a = ?b)
    }
    """
    m = Model()
    df = m.query(q, bindings={"b": IRI("urn:abc")})
    assert df.height == 1
