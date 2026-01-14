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
    print(out)
    m2 = Model()
    m2.reads(out, format="turtle")

def test_write_turtle_provided_prefixes():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    out = m.writes(format="turtle", prefixes={"myfoaf": "http://xmlns.com/foaf/0.1/"})
    print(out)
    m2 = Model()
    m2.reads(out, format="turtle")