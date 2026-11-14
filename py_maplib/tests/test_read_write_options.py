"""Coverage for options of Model.read / reads / write / writes that were not exercised elsewhere:
format inference from file extension, base_iri, checked, triples_batch_size, parallel,
graph / replace_graph, prefixes in turtle output and round trips through every format."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Model

EX = "http://example.net/ns#"
G1 = "urn:graph:one"

NT = f"""
<{EX}a> <{EX}p> <{EX}b> .
<{EX}a> <{EX}name> "Alice" .
<{EX}b> <{EX}name> "Bob"@en .
<{EX}a> <{EX}age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
<{EX}b> <{EX}score> "1.5"^^<http://www.w3.org/2001/XMLSchema#double> .
<{EX}b> <{EX}flag> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
"""

SPO = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o"


def _model():
    m = Model()
    m.reads(NT, format="ntriples")
    return m


@pytest.mark.parametrize(
    "extension,format",
    [
        ("nt", "ntriples"),
        ("ttl", "turtle"),
        ("rdf", "rdf/xml"),
        ("nq", "nquads"),
        ("trig", "trig"),
    ],
)
def test_write_read_file_round_trip_with_extension_inference(
    tmp_path, extension, format
):
    m = _model()
    path = tmp_path / f"out.{extension}"
    m.write(str(path), format=format)
    m2 = Model()
    # The format is inferred from the extension
    m2.read(str(path))
    assert_frame_equal(m.query(SPO), m2.query(SPO))


@pytest.mark.parametrize("format", ["ntriples", "turtle", "rdf/xml", "nquads", "trig"])
def test_writes_reads_round_trip(format):
    m = _model()
    s = m.writes(format=format)
    m2 = Model()
    m2.reads(s, format=format)
    assert_frame_equal(m.query(SPO), m2.query(SPO))


def test_write_pathlib_path(tmp_path):
    m = _model()
    path = tmp_path / "out.nt"
    m.write(path, format="ntriples")
    m2 = Model()
    m2.read(path)
    assert m2.size() == m.size()


def test_writes_turtle_uses_prefixes():
    m = _model()
    s = m.writes(format="turtle", prefixes={"ex": EX})
    assert "@prefix ex:" in s or "PREFIX ex:" in s
    assert "ex:a" in s
    m2 = Model()
    m2.reads(s, format="turtle")
    assert_frame_equal(m.query(SPO), m2.query(SPO))


def test_writes_named_graph_only():
    m = _model()
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1)
    s = m.writes(format="ntriples", graph=G1)
    assert s.strip() == f"<{EX}x> <{EX}q> <{EX}y> ."


def test_writes_nquads_all_graphs():
    m = _model()
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1)
    s = m.writes(format="nquads")
    assert f"<{EX}x> <{EX}q> <{EX}y> <{G1}> ." in s
    m2 = Model()
    m2.reads(s, format="nquads")
    assert m2.size() == m.size()
    assert m2.size(G1) == 1


def test_read_turtle_with_base_iri():
    m = Model()
    # Relative IRIs are resolved per RFC 3986, so the fragment of the base is dropped
    m.reads("<a> <p> <#b> .", format="turtle", base_iri="http://example.net/ns/x#frag")
    df = m.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    assert df["s"].to_list() == ["<http://example.net/ns/a>"]
    assert df["p"].to_list() == ["<http://example.net/ns/p>"]
    assert df["o"].to_list() == ["<http://example.net/ns/x#b>"]


def test_read_file_into_named_graph_and_replace(tmp_path):
    path = tmp_path / "data.nt"
    path.write_text(NT)
    m = Model()
    m.read(str(path), graph=G1)
    assert m.size(G1) == 6
    assert m.size() == 0

    path2 = tmp_path / "other.nt"
    path2.write_text(f"<{EX}x> <{EX}q> <{EX}y> .\n")
    m.read(str(path2), graph=G1, replace_graph=True)
    assert m.size(G1) == 1


@pytest.mark.parametrize("parallel", [True, False])
@pytest.mark.parametrize("format", ["ntriples", "turtle"])
def test_read_parallel_and_small_batches(parallel, format):
    lines = "\n".join(
        f'<{EX}s{i}> <{EX}v> "{i}"^^<http://www.w3.org/2001/XMLSchema#integer> .'
        for i in range(1000)
    )
    m = Model()
    m.reads(lines, format=format, parallel=parallel, triples_batch_size=37)
    assert m.size() == 1000
    df = m.query(f"SELECT (SUM(?v) AS ?sum) WHERE {{ ?s <{EX}v> ?v }}")
    assert df["sum"].to_list() == [sum(range(1000))]


def test_read_unchecked():
    m = Model()
    m.reads(NT, format="ntriples", checked=False)
    assert m.size() == 6


def test_read_invalid_ntriples_raises():
    m = Model()
    with pytest.raises(Exception):
        m.reads(f"<{EX}a> <{EX}p> .", format="ntriples")


def test_read_missing_file_raises(tmp_path):
    m = Model()
    with pytest.raises(Exception):
        m.read(str(tmp_path / "missing.nt"))


@pytest.mark.xfail(
    strict=False,
    raises=BaseException,
    reason="An unknown file extension panics (todo!) instead of raising a MaplibException "
    "(fixed by the unwrap-handling work)",
)
def test_read_unknown_extension_raises(tmp_path):
    path = tmp_path / "data.unknownext"
    path.write_text(NT)
    m = Model()
    with pytest.raises(Exception):
        m.read(str(path))


def test_read_rdf_xml_string():
    xml = f"""<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="{EX}">
  <rdf:Description rdf:about="{EX}a">
    <ex:name>Alice</ex:name>
    <ex:p rdf:resource="{EX}b"/>
  </rdf:Description>
</rdf:RDF>
"""
    m = Model()
    m.reads(xml, format="rdf/xml")
    assert m.size() == 2
    assert m.query(f'ASK {{ <{EX}a> <{EX}name> "Alice" . <{EX}a> <{EX}p> <{EX}b> }}')


def test_typed_literals_survive_round_trip():
    m = _model()
    s = m.writes(format="turtle")
    m2 = Model()
    m2.reads(s, format="turtle")
    df = m2.query(
        f"""SELECT ?age ?score ?flag WHERE {{
            <{EX}a> <{EX}age> ?age .
            <{EX}b> <{EX}score> ?score .
            <{EX}b> <{EX}flag> ?flag .
        }}"""
    )
    assert df["age"].to_list() == [30]
    assert df["score"].to_list() == [1.5]
    assert df["flag"].to_list() == [True]
    assert df.schema["age"] == pl.Int64
    assert df.schema["score"] == pl.Float64
    assert df.schema["flag"] == pl.Boolean


@pytest.mark.parametrize("parallel", [True, False])
@pytest.mark.parametrize("checked", [True, False])
def test_read_file_options(tmp_path, parallel, checked):
    path = tmp_path / "data.ttl"
    path.write_text('<a> <p> <b> .\n<a> <q> "x" .\n')
    m = Model()
    m.read(
        str(path),
        format="turtle",
        base_iri="http://example.net/ns/",
        parallel=parallel,
        checked=checked,
    )
    df = m.query("SELECT ?s ?o WHERE { ?s <http://example.net/ns/p> ?o }")
    assert df["s"].to_list() == ["<http://example.net/ns/a>"]
    assert df["o"].to_list() == ["<http://example.net/ns/b>"]
    assert m.size() == 2


def test_reads_replace_graph():
    m = Model()
    m.reads(NT, format="ntriples", graph=G1)
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", graph=G1, replace_graph=True)
    assert m.size(G1) == 1
    m.reads(NT, format="ntriples")
    m.reads(f"<{EX}x> <{EX}q> <{EX}y> .", format="ntriples", replace_graph=True)
    assert m.size() == 1


def test_reads_jsonld_known_contexts():
    doc = """{
      "@context": "http://example.net/context.jsonld",
      "@id": "http://example.net/ns#a",
      "name": "Alice"
    }"""
    context = '{"@context": {"name": "http://example.net/ns#name"}}'
    m = Model()
    m.reads(
        doc,
        format="json-ld",
        known_contexts={"http://example.net/context.jsonld": context},
    )
    assert m.query(f'ASK {{ <{EX}a> <{EX}name> "Alice" }}')
