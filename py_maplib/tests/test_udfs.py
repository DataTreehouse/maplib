import polars as pl
from polars import DataFrame

from maplib import Model, xsd, RDFType


def f(df: DataFrame) -> pl.Series:
    df = df.with_columns(
        (pl.col("0") + pl.col("1")).alias("out")
    )
    return df.get_column("out")

def g(df: DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        (pl.col("0").str.contains("abc")).alias("out")
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
    assert result.get_column("found").sort().to_list() == [None, False, False, True]

def test_list_udfs():
    m = Model()
    assert m.list_udfs() == []
    m.add_udf("urn:maplib:myudf", f, xsd.integer)
    assert m.list_udfs() == ["urn:maplib:myudf"]
