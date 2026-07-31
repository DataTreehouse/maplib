import pytest
from .disk import disk_params
from maplib import (
    Model,
    xsd,
    RDFType,
)
import polars as pl
import pathlib

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "jsons"

@pytest.mark.parametrize("disk", disk_params())
def test_map_df_easy(disk):
    df = pl.DataFrame({
        "a": ["abc", "def"],
        "b": [1,2]
    })
    m = Model()
    m.map_df(df)
    out_df = m.query("""
    SELECT * WHERE {
        ?root a fx:root .
        ?root fx:child ?row .
        ?row xyz:a ?a .
        ?row fx:childNumber ?ch_num .
        BIND(IRI(CONCAT("urn:my_ns:", ?a)) AS ?a_uri)
    }
    """)
    assert out_df.shape == (2,5)

@pytest.mark.parametrize("disk", disk_params())
def test_map_df_named_graph_arg(disk):
    df = pl.DataFrame({
        "a": ["abc", "def"],
        "b": [1,2]
    })
    m = Model()
    m.map_df(df, "urn:maplib:test")
    out_df = m.query("""
    SELECT * WHERE {
        ?root a fx:root .
        ?root fx:child ?row .
        ?row xyz:a ?a .
        ?row fx:childNumber ?ch_num .
        BIND(IRI(CONCAT("urn:my_ns:", ?a)) AS ?a_uri)
    }
    """, graph="urn:maplib:test")
    assert out_df.height == 2

def test_map_df_float():
    df = pl.DataFrame(
        {
            "a": [
                "abc",
                "def",
                "ghi",
            ],
            "b": [
                1.4,
                4.2,
                7.8,
            ],
        }
    )
    m = Model()
    m.map_df(df)
    res = m.query(
        """
    SELECT ?a ?c WHERE {?a xyz:b ?c} ORDER BY ?a ?c
    """, solution_mappings=True
    )
    print(res)
    assert res.rdf_types["c"] == RDFType.Literal(xsd.double)

def test_map_df_datetime():
    df = pl.DataFrame(
        {
            "a": [
                "abc",
                "def",
                "ghi",
            ],
            "b": [
                "2019-05-15T00:00:00",
                "2020-02-02T00:00:00",
                "2007-07-07T00:00:00",
            ],
        }
    )
    df = df.with_columns(pl.col("b").str.to_datetime())
    m = Model()
    m.map_df(df)
    res = m.query(
        """
    SELECT ?a ?c WHERE {?a xyz:b ?c} ORDER BY ?a ?c
    """, solution_mappings=True
    )
    assert res.rdf_types["c"] == RDFType.Literal(xsd.dateTime)

def test_map_df_uuid_v5_same_args():
    df = pl.DataFrame(
        {
            "a": [
                "abc",
                "def",
                "ghi",
            ],
            "b": [
                1.4,
                4.2,
                7.8,
            ],
        }
    )
    m = Model()
    m.map_df(df, uuid_namespace = "abc")
    df1 = m.query("""SELECT * WHERE {?a ?b ?c} ORDER BY ?a ?c""")

    m2 = Model()
    m2.map_df(df, uuid_namespace = "abc")
    df2 = m.query("""SELECT * WHERE {?a ?b ?c} ORDER BY ?a ?c""")
    assert df1.height == 13 and df2.height == 13
    assert df1.get_column("a").sort().to_list() == df2.get_column("a").sort().to_list()

def test_map_df_uuid_v5_diff_args():
    df = pl.DataFrame(
        {
            "a": [
                "abc",
                "def",
                "ghi",
            ],
            "b": [
                1.4,
                4.2,
                7.8,
            ],
        }
    )
    m = Model()
    m.map_df(df, uuid_namespace = "abc")
    df1 = m.query("""SELECT * WHERE {?a ?b ?c}""")

    m2 = Model()
    m2.map_df(df, uuid_namespace = "def")
    df2 = m2.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df1.height == 13 and df2.height == 13
    assert df1.get_column("a").sort().to_list() != df2.get_column("a").sort().to_list()