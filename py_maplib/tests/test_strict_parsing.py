"""Values that cannot be parsed as their datatype are dropped with a logged warning, or raise an
error when reading with strict=True."""

import pytest
import polars as pl
from maplib import Model, MaplibException

BAD_TTL = """
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://a> <http://p> "1"^^xsd:integer .
<http://a> <http://p> "abc"^^xsd:integer .
<http://a> <http://q> "not a date"^^xsd:dateTime .
<http://a> <http://r> "maybe"^^xsd:boolean .
"""

BAD_NT = """<http://a> <http://p> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://a> <http://p> "abc"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""

COUNT = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"


def count(m):
    return m.query(COUNT)["n"][0]


@pytest.mark.parametrize(
    "s,fmt,kept,dropped",
    [
        (BAD_TTL, "turtle", 1, 3),
        (BAD_NT, "ntriples", 1, 1),
    ],
)
def test_dropped_values_are_logged(capfd, s, fmt, kept, dropped):
    m = Model()
    m.reads(s, format=fmt)
    assert count(m) == kept
    err = capfd.readouterr().err
    assert f"Dropped {dropped} triples" in err
    assert "abc" in err


def test_dropped_values_are_logged_parallel(capfd):
    m = Model()
    m.reads(BAD_TTL, format="turtle", parallel=True)
    assert count(m) == 1
    assert "Dropped 3 triples" in capfd.readouterr().err


def test_no_warning_without_dropped_values(capfd):
    m = Model()
    m.reads('<http://a> <http://p> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .', format="ntriples")
    assert count(m) == 1
    assert "Dropped" not in capfd.readouterr().err


@pytest.mark.parametrize("fmt,s", [("turtle", BAD_TTL), ("ntriples", BAD_NT)])
def test_strict_reads_raises(fmt, s):
    m = Model()
    with pytest.raises(MaplibException, match="Could not parse"):
        m.reads(s, format=fmt, strict=True)


def test_strict_reads_parallel_raises():
    m = Model()
    with pytest.raises(MaplibException, match="abc"):
        m.reads(BAD_TTL, format="turtle", parallel=True, strict=True)


def test_strict_read_file_raises(tmp_path):
    p = tmp_path / "bad.ttl"
    p.write_text(BAD_TTL)
    m = Model()
    with pytest.raises(MaplibException, match="Could not parse"):
        m.read(p, strict=True)
    m.read(p)
    assert count(m) == 1


def test_strict_reads_ok_when_valid():
    m = Model()
    m.reads(BAD_NT.splitlines()[0], format="ntriples", strict=True)
    assert count(m) == 1


def test_strict_rdfxml_raises():
    xml = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://ex/">
  <rdf:Description rdf:about="http://a">
    <ex:p rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">abc</ex:p>
    <ex:p rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">1</ex:p>
  </rdf:Description>
</rdf:RDF>"""
    with pytest.raises(MaplibException, match="Could not parse"):
        Model().reads(xml, format="rdf/xml", strict=True)
    m = Model()
    m.reads(xml, format="rdf/xml")
    assert count(m) == 1


def test_insert_data_logs_dropped_values(capfd):
    m = Model()
    m.update(
        """PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        INSERT DATA { <http://a> <http://p> "abc"^^xsd:integer . <http://a> <http://p> 1 . }"""
    )
    assert count(m) == 1
    assert "Dropped 1 triples" in capfd.readouterr().err


def test_hdt_round_trip_not_affected(tmp_path, capfd):
    m = Model()
    m.reads(BAD_NT, format="ntriples")
    p = tmp_path / "out.hdt"
    m.write(p, format="hdt")
    capfd.readouterr()
    m2 = Model()
    m2.read(p, strict=True)
    assert count(m2) == 1
    assert "Dropped" not in capfd.readouterr().err
