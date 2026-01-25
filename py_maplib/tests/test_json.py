import os

from maplib import Model
import pytest
from polars.testing import assert_frame_equal
import polars as pl
from math import floor
import pathlib
import time

from pyparsing import stringStart

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "jsons"


def test_map_json_1():
    json_1 = TESTDATA_PATH / "1.json"
    m = Model()
    m.map_json(str(json_1))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 52

    df2 = m.query("""
    PREFIX mj:  <urn:maplib_json:> 
    PREFIX mjk: <urn:maplib_json_keys:>
    SELECT ?title WHERE {
    ?d a mj:Root .
    ?d mj:contains ?n1 .
    ?n1 mjk:glossary ?n2 .
    ?n2 mjk:title ?title . 
    }""")
    expect2 = pl.from_repr("""
┌──────────────────┐
│ title            │
│ ---              │
│ str              │
╞══════════════════╡
│ example glossary │
└──────────────────┘
""")
    assert_frame_equal(df2, expect2)

    df3 = m.query("""
    PREFIX mj:  <urn:maplib_json:> 
    PREFIX mjk: <urn:maplib_json_keys:>
    SELECT ?ks WHERE {
    mjk:glossary a mj:Key .
    mjk:glossary mj:keyString ?ks .  
    }""")
    expect3 = pl.from_repr("""
┌──────────────────┐
│ ks               │
│ ---              │
│ str              │
╞══════════════════╡
│ glossary         │
└──────────────────┘
""")
    assert_frame_equal(df3, expect3)


def test_map_json_2():
    json_2 = TESTDATA_PATH / "2.json"
    m = Model()
    m.map_json(str(json_2))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 83
    # We expect this number to increase when we start caring about array ordering

    df2 = m.query("""
    PREFIX mj:  <urn:maplib_json:> 
    PREFIX mjk: <urn:maplib_json_keys:>
    SELECT ?item WHERE {
    ?d a mj:Root .
    ?d mj:contains ?n1 .
    ?n1 mjk:menu ?n2 .
    ?n2 mjk:items ?item . 
    }""")
    assert df2.height == 19

def test_map_json_3():
    json_3 = TESTDATA_PATH / "3.json"
    m = Model()
    m.map_json(str(json_3))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 239
    # We expect this number to increase when we start caring about array ordering

    df2 = m.query("""
    PREFIX mj:  <urn:maplib_json:> 
    PREFIX mjk: <urn:maplib_json_keys:>
    SELECT ?log ?betaserver WHERE {
    ?d a mj:Root .
    ?d mj:contains ?n1 .
    ?n1 mjk:web-app ?n2 .
    ?n2 mjk:servlet ?n3 .
    ?n3 mjk:init-param ?n4 . 
    ?n4 mjk:log ?log .
    ?n5 mjk:betaServer ?betaserver .
    }""")
    assert df2.height == 1
    assert df2.get_column("log").dtype == pl.Int64
    assert df2.get_column("betaserver").dtype == pl.Boolean

