import polars as pl
from polars import DataFrame

from maplib import Model, xsd, RDFType


def f(df: DataFrame) -> pl.Series:
    df = df.with_columns(
        (pl.col("0") + pl.col("1")).alias("out")
    )
    return df.get_column("out")

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
    print(result)

def test_list_udfs():
    m = Model()
    assert m.list_udfs() == []
    m.add_udf("urn:maplib:myudf", f, xsd.integer)
    assert m.list_udfs() == ["urn:maplib:myudf"]
