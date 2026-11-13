import polars as pl
import pytest
from polars.testing import assert_frame_equal
import pathlib
from maplib import Model

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


def test_write_turtle_only_declares_used_prefixes():
    m = Model()
    m.reads(
        """
        <http://example.net/subj/a> <http://example.org/pred/p> <http://example.com/obj/b> .
        <http://example.net/subj/a> <http://example.org/pred/q> "1"^^<http://example.edu/dt/myType> .
        """,
        format="ntriples",
    )
    out = m.writes(
        format="turtle",
        prefixes={
            "s": "http://example.net/subj/",
            "p": "http://example.org/pred/",
            "o": "http://example.com/obj/",
            "dt": "http://example.edu/dt/",
            "unused": "http://example.net/other/",
            "unused2": "http://example.com/obj/b/c/",
        },
    )
    for used in ["s", "p", "o", "dt"]:
        assert f"@prefix {used}: " in out
    assert "@prefix unused:" not in out
    assert "@prefix unused2:" not in out
    m2 = Model()
    m2.reads(out, format="turtle")
    assert len(m2.query("SELECT * WHERE {?a ?b ?c}")) == 2


def test_write_turtle_default_prefixes():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    out = m.writes(format="turtle")
    m2 = Model()
    m2.reads(out, format="turtle")

def test_write_turtle_should_escape_suffix():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples_prefix_escaped.nt"))
    m.add_prefixes({"foaf":"http://xmlns.com/foaf/0.1/"})
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


def test_write_multi_turtle_provided_prefixes_stress():
    m = Model()
    for i in range(3000):
        m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 24000

    out = m.writes(format="turtle", prefixes={"myfoaf": "http://xmlns.com/foaf/0.1/"})
    m2 = Model()
    m2.reads(out, format="turtle", parallel=True)
    df2 = m2.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df2.height == 24000


def test_write_multi_turtle_provided_prefixes_some_stress():
    m = Model()
    for i in range(100):
        m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 800

    out = m.writes(format="turtle", prefixes={"myfoaf": "http://xmlns.com/foaf/0.1/"})
    m2 = Model()
    m2.reads(out, format="turtle", parallel=True)
    df2 = m2.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df2.height == 800


def test_query_write_multi_turtle_provided_prefixes_some_stress():
    m = Model()
    for i in range(100):
        m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    df = m.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df.height == 800

    out = m.query("CONSTRUCT {?a ?b ?c} WHERE {?a ?b ?c}", return_format="turtle")
    m2 = Model()
    m2.reads(out, format="turtle", parallel=True)
    df2 = m2.query("""SELECT * WHERE {?a ?b ?c}""")
    assert df2.height == 800


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

@pytest.mark.parametrize("format", ["turtle", "rdf/xml"])
@pytest.mark.parametrize(
    "ntriples",
    [
        # Blank node whose only reference is itself
        "_:j0 <http://example.org/property> _:j0 .",
        # Cycle of blank nodes with in-degree one
        """_:a <http://example.org/p> _:b .
        _:b <http://example.org/p> _:a .""",
        # Longer cycle with a tail and literals hanging off it
        """_:a <http://example.org/p> _:b .
        _:b <http://example.org/p> _:c .
        _:c <http://example.org/p> _:a .
        _:a <http://example.org/q> _:d .
        _:d <http://example.org/r> "x" .
        <http://example.org/s> <http://example.org/p> _:e .
        _:e <http://example.org/p> _:e .""",
    ],
)
def test_pretty_write_blank_node_cycles(ntriples, format):
    m = Model()
    m.reads(ntriples, format="ntriples")
    out = m.writes(format=format)
    m2 = Model()
    m2.reads(out, format=format)
    q = "SELECT ?a ?b ?c WHERE {?a ?b ?c}"
    n = len(m.query(q))
    assert len(m2.query(q)) == n
    # Self-loops must survive as self-loops, not as edges between two distinct blank nodes
    def self_loops(model):
        lines = [l.split() for l in model.writes(format="ntriples").splitlines() if l.strip()]
        return sum(1 for l in lines if l[0] == l[2])

    assert self_loops(m2) == self_loops(m)
