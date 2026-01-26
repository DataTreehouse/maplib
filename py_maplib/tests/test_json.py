from maplib import Model
from polars.testing import assert_frame_equal
import polars as pl
import pathlib

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "jsons"


def test_map_json_1():
    json_1 = TESTDATA_PATH / "1.json"
    m = Model()
    m.map_json(str(json_1))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 18

    print(m.writes("turtle"))
    df2 = m.query("""
    PREFIX fx:  <http://sparql.xyz/facade-x/ns/> 
    PREFIX xyz: <http://sparql.xyz/facade-x/data/>
    SELECT ?title WHERE {
    ?n1 a fx:root .
    ?n1 xyz:glossary ?n2 .
    ?n2 xyz:title ?title . 
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


def test_map_json_2():
    json_2 = TESTDATA_PATH / "2.json"
    m = Model()
    m.map_json(str(json_2))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 56
    # We expect this number to increase when we start caring about array ordering
    df2 = m.query("""
    PREFIX fx:  <http://sparql.xyz/facade-x/ns/> 
    PREFIX xyz: <http://sparql.xyz/facade-x/data/>
    PREFIX mj:  <urn:maplib_json:> 
    SELECT ?item WHERE {
    ?n1 a fx:root .
    ?n1 xyz:menu ?n2 .
    ?n2 xyz:items ?arr .
    ?arr ?ref ?item .
    } """)
    assert df2.height == 22

def test_map_json_3():
    json_3 = TESTDATA_PATH / "3.json"
    m = Model()
    m.map_json(str(json_3))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 88
    # We expect this number to increase when we start caring about array ordering

    df2 = m.query("""
    PREFIX fx:  <http://sparql.xyz/facade-x/ns/> 
    PREFIX xyz: <http://sparql.xyz/facade-x/data/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
    PREFIX mj:  <urn:maplib_json:> 
    SELECT ?log ?betaserver ?myfloat WHERE {
    ?n1 a fx:root .
    ?n1 xyz:web-app ?n2 .
    ?n2 xyz:servlet ?arr .
    ?arr rdf:_5 ?n3 .
    ?n3 xyz:init-param ?n4 . 
    ?n4 xyz:log ?log .
    ?n4 xyz:betaServer ?betaserver .
    ?n5 xyz:myFloat ?myfloat .
    }""")
    assert df2.height == 1
    assert df2.get_column("log").dtype == pl.Int64
    assert df2.get_column("betaserver").dtype == pl.Boolean
    assert df2.get_column("myfloat").dtype == pl.Float64

def test_map_json_string():
    s = """
    { "maplib_users": {
    "Alice": { "name": "Alice", "address": { "city": "Swansea" } },
    "Bob": { "name": "Bob", "address": { "city": "Swansea" } }
    }}
    """
    m = Model()
    m.map_json(s)
    height = m.query("SELECT * WHERE {?a ?b ?c}").height
    r = m.writes(format="turtle", prefixes={
        "mj":"urn:maplib_json:"
    })
    print(r)
    m2 = Model()
    m2.reads(r, format="turtle")
    height_after = m2.query("SELECT * WHERE {?a ?b ?c}").height
    assert height == height_after

def test_map_json_list_string():
    s = """
    { "maplib_users": [
    { "name": "Alice", "address": { "city": "Swansea" } },
    { "name": "Bob", "address": { "city": "Swansea" } }
    ]}
    """
    m = Model()
    m.map_json(s)
    height = m.query("SELECT * WHERE {?a ?b ?c}").height
    r = m.writes(format="turtle", prefixes={
        "mj":"urn:maplib_json:"
    })
    print(r)
    m2 = Model()
    m2.reads(r, format="turtle")
    height_after = m2.query("SELECT * WHERE {?a ?b ?c}").height
    assert height == height_after

def test_insert():
    s = """
    { "maplib_users": [
    { "name": "Alice", "address": { "city": "Swansea" } },
    { "name": "Bob", "address": { "city": "Swansea" } }
    ]}
    """
    m = Model()
    m.map_json(s, graph="urn:graph:tmp")
    users = """
        PREFIX :<https://github.com/DataTreehouse/maplib/users#>
        CONSTRUCT { ?u a :SwanseaUser; :name ?name } 
        WHERE {
            ?root xyz:maplib_users ?users.
            ?users ?i ?u.
            ?u xyz:address/xyz:city "Swansea"; xyz:name ?name.
        }"""
    m.insert(users, source_graph="urn:graph:tmp")
    #print(m.writes(format="turtle", prefixes={"":"https://github.com/DataTreehouse/maplib/users#"}))
    df = m.query("SELECT * WHERE {?a ?b ?c}")
    assert df.height == 4