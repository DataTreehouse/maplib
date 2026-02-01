import polars as pl
import pytest
import rdflib
from polars.testing import assert_frame_equal
import pathlib
from maplib import Model

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

def test_write_turtle_default_prefixes():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    out = m.writes(format="turtle")
    m2 = Model()
    m2.reads(out, format="turtle")

def test_write_turtle_newline_bug():
    m = Model()
    m.read(str(TESTDATA_PATH / "write_turtle_newlines_bug.nt"))
    out = m.writes(format="turtle")
    m2 = Model()
    m2.reads(out, format="turtle")

def test_write_turtle_provided_prefixes():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    out = m.writes(format="turtle", prefixes={"myfoaf": "http://xmlns.com/foaf/0.1/"})
    m2 = Model()
    m2.reads(out, format="turtle")
    assert "myfoaf:" in out

def test_write_turtle_global_provided_prefixes():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    m.add_prefixes({"myfoaf": "http://xmlns.com/foaf/0.1/"})
    out = m.writes(format="turtle")
    m2 = Model()
    m2.reads(out, format="turtle")
    assert "myfoaf:" in out

def test_write_turtle_global_provided_prefixes_and_provided():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    m.add_prefixes({"myfoaf": "http://xmlns.com/foaf/0.1/"})
    out = m.writes(format="turtle", prefixes={})
    m2 = Model()
    m2.reads(out, format="turtle")
    assert "myfoaf:" in out

def test_write_multi_turtle_provided_prefixes():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 24

    out = m.writes(format="turtle", prefixes={"myfoaf": "http://xmlns.com/foaf/0.1/"})
    m2 = Model()
    m2.reads(out, format="turtle")
    df2 = m2.query("""SELECT * WHERE {?a ?b ?c}""")
    expected_path = TESTDATA_PATH / "pretty_turtle_write_multi_provided_expected.csv"
    #df2.write_csv(expected_path)
    print(df2)
    df2_expected = pl.read_csv(expected_path)
    df2 = df2.with_columns(
        pl.when(pl.col("a").str.starts_with("_:")).then(pl.lit("blank!")).otherwise(pl.col("a")).alias("a"),
        pl.when(pl.col("c").str.starts_with("_:")).then(pl.lit("blank!")).otherwise(pl.col("c")).alias("c"),
    ).sort(["a", "b", "c"])
    df2_expected = df2_expected.with_columns(
        pl.when(pl.col("a").str.starts_with("_:")).then(pl.lit("blank!")).otherwise(pl.col("a")).alias("a"),
        pl.when(pl.col("c").str.starts_with("_:")).then(pl.lit("blank!")).otherwise(pl.col("c")).alias("c"),
    ).sort(["a", "b", "c"])
    assert_frame_equal(df2_expected, df2)
    assert df2.height == 24

def test_write_lists():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_lists.ttl"))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 30

    out = m.writes(format="turtle", prefixes={"myfoaf": "http://xmlns.com/foaf/0.1/"})
    print(out)
    m2 = Model()
    m2.reads(out, format="turtle")
    df2 = m2.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df2.height == 30