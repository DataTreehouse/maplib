import pytest

from maplib import Model, MaplibException

TRIG = """
@prefix ex: <http://example.org/> .

ex:s ex:p "default" .

ex:g1 {
    ex:s ex:p "one" .
    ex:s ex:q ex:o .
}

GRAPH ex:g2 {
    ex:s ex:p "two" .
}
"""

NQUADS = """<http://example.org/s> <http://example.org/p> "default" .
<http://example.org/s> <http://example.org/p> "one" <http://example.org/g1> .
<http://example.org/s> <http://example.org/q> <http://example.org/o> <http://example.org/g1> .
<http://example.org/s> <http://example.org/p> "two" <http://example.org/g2> .
"""


def graph_triples(m: Model, graph=None):
    df = m.query(
        """SELECT (STR(?s) AS ?ss) (STR(?p) AS ?ps) (STR(?o) AS ?os)
        WHERE { ?s ?p ?o }""",
        graph=graph,
    )
    return set(df.select(["ss", "ps", "os"]).iter_rows())


def assert_dataset(m: Model):
    ex = "http://example.org/"
    assert graph_triples(m) == {(ex + "s", ex + "p", "default")}
    assert graph_triples(m, ex + "g1") == {
        (ex + "s", ex + "p", "one"),
        (ex + "s", ex + "q", ex + "o"),
    }
    assert graph_triples(m, ex + "g2") == {(ex + "s", ex + "p", "two")}


@pytest.mark.parametrize(
    "s,format",
    [(TRIG, "trig"), (NQUADS, "nquads"), (NQUADS, "n-quads"), (NQUADS, "nq")],
)
def test_reads_dataset_formats(s, format):
    m = Model()
    m.reads(s, format=format)
    assert_dataset(m)


@pytest.mark.parametrize("s,suffix", [(TRIG, ".trig"), (NQUADS, ".nq")])
def test_read_dataset_formats_from_file_extension(tmp_path, s, suffix):
    path = tmp_path / f"data{suffix}"
    path.write_text(s)
    m = Model()
    m.read(path)
    assert_dataset(m)


def test_named_graphs_can_be_queried():
    m = Model()
    m.reads(TRIG, format="trig")
    df = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?o WHERE { ?s ex:p ?o }""",
        graph="http://example.org/g1",
    )
    assert df["o"].to_list() == ["one"]


def test_graph_argument_overrides_graph_names():
    m = Model()
    m.reads(TRIG, format="trig", graph="http://example.org/target")
    ex = "http://example.org/"
    assert graph_triples(m, ex + "target") == {
        (ex + "s", ex + "p", "default"),
        (ex + "s", ex + "p", "one"),
        (ex + "s", ex + "q", ex + "o"),
        (ex + "s", ex + "p", "two"),
    }
    assert graph_triples(m) == set()


def test_trig_base_iri():
    m = Model()
    m.reads("<g> { <s> <p> <o> . }", format="trig", base_iri="http://example.org/")
    assert graph_triples(m, "http://example.org/g") == {
        ("http://example.org/s", "http://example.org/p", "http://example.org/o")
    }


def test_trig_syntax_error():
    m = Model()
    with pytest.raises(MaplibException):
        m.reads("<http://example.org/g> { <http://example.org/s> ", format="trig")


@pytest.mark.parametrize("format", ["trig", "nquads"])
def test_writes_round_trip(format):
    m = Model()
    m.reads(TRIG, format="trig")
    s = m.writes(format=format)
    m2 = Model()
    m2.reads(s, format=format)
    assert_dataset(m2)


@pytest.mark.parametrize("format,suffix", [("trig", ".trig"), ("nquads", ".nq")])
def test_write_round_trip_via_file(tmp_path, format, suffix):
    m = Model()
    m.reads(NQUADS, format="nquads")
    path = tmp_path / f"out{suffix}"
    m.write(path, format=format)
    m2 = Model()
    m2.read(path)
    assert_dataset(m2)


def test_writes_round_trip_typed_literals_and_blank_nodes():
    data = """
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    ex:s ex:p "hello"@en, 42, "2020-01-01"^^xsd:date .
    ex:g1 { ex:s ex:q [ ex:r ex:o ] . }
    """
    m = Model()
    m.reads(data, format="trig")
    m2 = Model()
    m2.reads(m.writes(format="trig"), format="trig")
    q = """PREFIX ex: <http://example.org/>
    SELECT (STR(?o) AS ?v) (DATATYPE(?o) AS ?dt) (LANG(?o) AS ?l)
    WHERE { ex:s ex:p ?o } ORDER BY ?v"""
    assert m2.query(q).equals(m.query(q))
    df = m2.query(
        """PREFIX ex: <http://example.org/>
        SELECT (STR(?o) AS ?os) WHERE { ex:s ex:q ?b . ?b ex:r ?o }""",
        graph="http://example.org/g1",
    )
    assert df["os"].to_list() == ["http://example.org/o"]


def test_writes_single_graph():
    m = Model()
    m.reads(TRIG, format="trig")
    s = m.writes(format="nquads", graph="http://example.org/g2")
    assert s.strip() == (
        '<http://example.org/s> <http://example.org/p> "two" <http://example.org/g2> .'
    )


def test_writes_default_graph_only():
    m = Model()
    m.reads(TRIG, format="trig")
    s = m.writes(format="nquads", graph="https://datatreehouse.github.io/maplib/vocab#DefaultGraph")
    assert s.strip() == (
        '<http://example.org/s> <http://example.org/p> "default" .'
    )


def test_writes_missing_graph_is_empty():
    m = Model()
    m.reads(TRIG, format="trig")
    assert m.writes(format="nquads", graph="http://example.org/missing") == ""


def test_writes_trig_uses_prefixes():
    m = Model()
    m.reads(TRIG, format="trig")
    s = m.writes(format="trig", prefixes={"ex": "http://example.org/"})
    assert "@prefix ex: <http://example.org/> ." in s
    assert "ex:g1 {" in s
