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
    assert df2.height == 24

def test_write_lists():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_lists.ttl"))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 24

    out = m.writes(format="turtle", prefixes={"myfoaf": "http://xmlns.com/foaf/0.1/"})
    #print(out)
    m2 = Model()
    m2.reads(out, format="turtle")
    df2 = m2.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df2.height == 24