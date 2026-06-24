import polars as pl
import polars.testing as pl_testing
from polars import DataFrame, Series

from maplib import Model, xsd


def test_add_udf():
    m = Model()
    m.add_udf("myudf", lambda x: x + 1, xsd.integer)
    assert m.list_udfs() == ["myudf"]


def test_udf_run1():
    m = Model()
    m.add_udf("myudf", lambda x: x + 1, xsd.integer)
    df = pl.DataFrame({"x": [1, 2, 3]})
    result = m.run_udf("myudf", df)
    expected = pl.DataFrame({"x": [2, 3, 4]})
    pl.testing.assert_frame_equal(result, expected)
    print(df, result)


def udf_add_one(x):
    return x + 1


def udf_mul_two(x):
    return x * 2


def f(df: DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        (pl.col("0") + pl.col("1")).alias("out")
    )
    return df.get_column("out")


def test_udf_run2():
    m = Model()
    m.add_udf("myudf", udf_add_one, xsd.integer)
    df = pl.DataFrame({"x": [1, 2, 3]})
    result = m.run_udf("myudf", df)
    expected = pl.DataFrame({"x": [2, 3, 4]})
    pl.testing.assert_frame_equal(result, expected)
    print(df, result)


def test_udf_run3():
    m = Model()
    m.add_udf("myudf", udf_mul_two, xsd.integer)
    df = pl.DataFrame({"x": [3, 6, 9]})
    result = m.run_udf("myudf", df)
    expected = pl.DataFrame({"x": [6, 12, 18]})
    pl.testing.assert_frame_equal(result, expected)
    print(df, result)

def test_udf_run4():
    m = Model()
    m.add_udf("urn:maplib:plusarg", f, xsd.integer)
    df = pl.DataFrame({"0": [1, 2, 3], "1": [4, 5, 6]})
    result = m.run_udf("urn:maplib:plusarg", df)
    expected = pl.Series("out", [5, 7, 9])
    pl.testing.assert_series_equal(result, expected)
    print(df, result)

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
    print(m.list_udfs())
    m.add_udf("myudf", lambda x: x + 1, xsd.integer)
    assert m.list_udfs() == ["myudf"]
    print(m.list_udfs())