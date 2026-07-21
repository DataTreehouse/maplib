import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Model, RDFType, MaplibException, rdf


def test_struuidv5_dns():
    m = Model()
    sm = m.query("""
    SELECT * WHERE {
        VALUES ?name {"a" "b" UNDEF}
        BIND(maplib:struuidv5("dns", ?name) AS ?sv5)
        BIND(maplib:uuidv5("dns", ?name) AS ?v5)
    }
    """, solution_mappings=True)
    assert sm.mappings.shape == (3,3)
    assert sm.rdf_types["sv5"] == RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")
    assert sm.rdf_types["v5"] == RDFType.IRI

def test_struuidv5_oid():
    m = Model()
    sm = m.query("""
    SELECT * WHERE {
        VALUES ?name {"a" "b" UNDEF}
        BIND(maplib:struuidv5("oid", ?name) AS ?sv5)
        BIND(maplib:uuidv5("oid", ?name) AS ?v5)
    }
    """, solution_mappings=True)
    assert sm.mappings.shape == (3,3)
    assert sm.rdf_types["sv5"] == RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")
    assert sm.rdf_types["v5"] == RDFType.IRI

def test_struuidv5_url():
    m = Model()
    sm = m.query("""
    SELECT * WHERE {
        VALUES ?name {"a" "b" UNDEF}
        BIND(maplib:struuidv5("url", ?name) AS ?sv5)
        BIND(maplib:uuidv5("url", ?name) AS ?v5)
    }
    """, solution_mappings=True)
    assert sm.mappings.shape == (3,3)
    assert sm.rdf_types["sv5"] == RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")
    assert sm.rdf_types["v5"] == RDFType.IRI

def test_struuidv5_x500():
    m = Model()
    sm = m.query("""
    SELECT * WHERE {
        VALUES ?name {"a" "b" UNDEF}
        BIND(maplib:struuidv5("x500", ?name) AS ?sv5)
        BIND(maplib:uuidv5("x500", ?name) AS ?v5)
    }
    """, solution_mappings=True)
    assert sm.mappings.shape == (3,3)
    assert sm.rdf_types["sv5"] == RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")
    assert sm.rdf_types["v5"] == RDFType.IRI

def test_struuidv5_invalid():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.query("""
        SELECT * WHERE {
            VALUES ?name {"a" "b" UNDEF}
            BIND(maplib:struuidv5("abc", ?name) AS ?sv5)
            BIND(maplib:uuidv5("abc", ?name) AS ?v5)
        }
        """)
    assert "Namespace argument must either be a uuid string" in str(e)

def test_struuidv5_valid():
    m = Model()
    df = m.query("""
    SELECT * WHERE {
        VALUES ?name {"a" "b" UNDEF}
        BIND(maplib:struuidv5("8be4df61-93ca-11d2-aa0d-00e098032b8c", ?name) AS ?sv5)
        BIND(maplib:uuidv5("8be4df61-93ca-11d2-aa0d-00e098032b8e", ?name) AS ?v5)
    }
    """)
    assert df.height == 3

def test_concat_lang_string_same_lang():
    m = Model()
    df = m.query("""
    SELECT * WHERE {
        VALUES ?name {"bar"@en}
        BIND(concat("foo"@en, ?name) AS ?concatstr)
    }
    """)
    expected_df = pl.from_repr(
        """
    ┌─────────────┬──────────┐
    │ concatstr   ┆ name     │
    │ ---         ┆ ---      │
    │ str         ┆ str      │
    ╞═════════════╪══════════╡
    │ "foobar"@en ┆ "bar"@en │
    └─────────────┴──────────┘
    """
    )
    sm = m.query("""
    SELECT * WHERE {
        VALUES ?name {"bar"@en}
        BIND(concat("foo"@en, ?name) AS ?concatstr)
    }
    """, solution_mappings=True)
    assert_frame_equal(df, expected_df)
    assert sm.rdf_types["concatstr"] == RDFType.Literal(rdf.langString)

def test_concat_lang_string_diff_lang():
    m = Model()
    df = m.query("""
    SELECT * WHERE {
        VALUES ?name {"bar"@en}
        BIND(concat(?name, "noe"@no) AS ?concatstr)
    }
    """)
    expected_df = pl.from_repr("""
    ┌─────────────┬──────────┐
    │ concatstr   ┆ name     │
    │ ---         ┆ ---      │
    │ str         ┆ str      │
    ╞═════════════╪══════════╡
    │ "barnoe"@en ┆ "bar"@en │
    └─────────────┴──────────┘
    """
    )
    assert_frame_equal(df, expected_df)

def test_encoding_of_parenthesis():
    m = Model()
    df = m.query("""
    SELECT * WHERE {
        VALUES ?name {"Foo(bar)"}
        BIND(ENCODE_FOR_URI(?name) AS ?encodedstr)
    }
    """)
    expected_df = pl.from_repr("""
    ┌──────────────┬──────────┐
    │ encodedstr   ┆ name     │
    │ ---          ┆ ---      │
    │ str          ┆ str      │
    ╞══════════════╪══════════╡
    │ Foo%28bar%29 ┆ Foo(bar) │
    └──────────────┴──────────┘
    """
    )
    assert_frame_equal(df, expected_df)