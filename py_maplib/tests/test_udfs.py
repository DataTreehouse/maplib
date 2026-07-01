import polars as pl
import pytest
from polars import DataFrame
from maplib import Model, xsd, RDFType, MaplibException

def f(df: pl.DataFrame) -> pl.Series:
    df = df.with_columns(
        (pl.col("0") + pl.col("1")).alias("out")
    )
    return df.get_column("out")

def g(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        (pl.col("0").str.contains("abc")).alias("out")
    )
    return df

def h(df: DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        (pl.col("0") + pl.lit("_suf")).alias("out")
    )
    return df

def test_udf_query():
    m = Model()
    m.add_udf("urn:maplib:plusarg", f, xsd.integer)
    m.reads("""
            <http://example.org/a> <urn:maplib:hasnum> 1 .
            <http://example.org/a> <urn:maplib:hasnum> 2 .
            <http://example.org/b> <urn:maplib:hasnum> 3 .
            <http://example.org/b> <urn:maplib:hasnum> 4 .
        """, "turtle")
    result = m.query("""
        SELECT * {
            ?a <urn:maplib:hasnum> ?n1 .
            ?a <urn:maplib:hasnum> ?n2 .
            BIND(<urn:maplib:plusarg>(?n1, ?n2) AS ?sum)
        }
        """)
    assert result.get_column("sum").sort().to_list() == [2, 3, 3, 4, 6, 7, 7, 8]

def test_udf_query_strings():
    m = Model()
    m.add_udf("urn:maplib:findabc", g, xsd.boolean, [xsd.string])
    m.reads("""
            <http://example.org/a> <urn:maplib:hasstr> "1" .
            <http://example.org/a> <urn:maplib:hasstr> "ab2" .
            <http://example.org/b> <urn:maplib:hasstr> "abc" .
            <http://example.org/b> <urn:maplib:hasstr> 1 .
        """, "turtle")
    result = m.query("""
        SELECT * {
            ?a <urn:maplib:hasstr> ?s1 .
            BIND(<urn:maplib:findabc>(?s1) AS ?found)
        }
        """)
    print(result)
    assert result.get_column("found").sort().to_list() == [None, False, False, True]

def test_udf_query_iris():
    m = Model()
    m.add_udf("urn:maplib:addsuf", h, RDFType.IRI, [RDFType.IRI])
    m.reads("""
            <http://example.org/a> <urn:maplib:hasiri> <http://example.org/abe> .
            <http://example.org/a> <urn:maplib:hasiri> <http://example.org/abc> .
            <http://example.org/a> <urn:maplib:hasiri> <http://example.org/abd> .
        """, "turtle")
    result = m.query("""
        SELECT * {
            ?a <urn:maplib:hasiri> ?s1 .
            BIND(<urn:maplib:addsuf>(?s1) AS ?suffed)
        }
        """)
    assert result.get_column("suffed").sort().to_list() == ['<http://example.org/abc_suf>', '<http://example.org/abd_suf>', '<http://example.org/abe_suf>']

def test_list_udfs():
    m = Model()
    assert m.list_udfs() == []
    m.add_udf("urn:maplib:myudf", f, xsd.integer)
    assert m.list_udfs() == ["urn:maplib:myudf"]

def test_udf_error_message():
    m = Model()
    with pytest.raises(Exception, match="is not callable"):
        m.add_udf("urn:maplib:test", "test", xsd.boolean, [xsd.string])

def test_udf_wrong_output_type():
    m = Model()
    m.add_udf("urn:maplib:findabc", g, xsd.integer, [xsd.string])
    m.reads("""
            <http://example.org/a> <urn:maplib:hasstr> "1" .
            <http://example.org/a> <urn:maplib:hasstr> "ab2" .
            <http://example.org/b> <urn:maplib:hasstr> "abc" .
            <http://example.org/b> <urn:maplib:hasstr> 1 .
        """, "turtle")
    with pytest.raises(MaplibException) as e:
        result = m.query("""
            SELECT * {
                ?a <urn:maplib:hasstr> ?s1 .
                BIND(<urn:maplib:findabc>(?s1) AS ?found)
            }
            """)
        assert "did not have the expected type" in e