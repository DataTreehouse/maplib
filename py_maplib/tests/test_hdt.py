import pathlib

import polars as pl
import pytest
import rdflib
from rdflib.compare import isomorphic

from maplib import Model

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


def model_as_rdflib_graph(m: Model) -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse(data=m.writes(format="ntriples"), format="nt")
    return g


def test_write_read_hdt_round_trip_is_isomorphic(tmp_path):
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    hdt_path = tmp_path / "out.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path), format="hdt")

    assert isomorphic(model_as_rdflib_graph(m), model_as_rdflib_graph(m2))


def test_read_hdt_infers_format_from_file_extension(tmp_path):
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    hdt_path = tmp_path / "out.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))
    res = m2.query(
        """
        SELECT ?v ?o WHERE {
            ?s ?v ?o .
        }
        """
    )
    assert res.height == 8


def test_hdt_round_trip_preserves_typed_and_language_literals(tmp_path):
    m = Model()
    m.reads(
        """
        <urn:maplib:s1> <urn:maplib:p> "plain" .
        <urn:maplib:s1> <urn:maplib:lang> "hello"@en .
        <urn:maplib:s1> <urn:maplib:int> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
        <urn:maplib:s1> <urn:maplib:dec> "1.5"^^<http://www.w3.org/2001/XMLSchema#decimal> .
        <urn:maplib:s1> <urn:maplib:esc> "line1\\nline2 \\"quoted\\"" .
        <urn:maplib:s1> <urn:maplib:obj> <urn:maplib:s2> .
        """,
        format="ntriples",
    )
    hdt_path = tmp_path / "literals.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))

    assert isomorphic(model_as_rdflib_graph(m), model_as_rdflib_graph(m2))


def test_empty_graph_hdt_round_trip(tmp_path):
    m = Model()
    m.reads("<urn:maplib:s> <urn:maplib:p> <urn:maplib:o> .", format="ntriples")
    m.update("DELETE WHERE { ?s ?p ?o }")
    hdt_path = tmp_path / "empty.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path), format="hdt")
    res = m2.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    assert res.height == 0


def test_writes_with_hdt_format_raises_since_hdt_is_binary():
    m = Model()
    m.reads("<urn:maplib:s> <urn:maplib:p> <urn:maplib:o> .", format="ntriples")
    with pytest.raises(Exception, match="binary"):
        m.writes(format="hdt")


def test_reads_with_hdt_format_raises_since_hdt_is_binary():
    m = Model()
    with pytest.raises(Exception, match="binary"):
        m.reads("anything", format="hdt")


def test_hdt_round_trip_preserves_object_position_blank_nodes(tmp_path):
    m = Model()
    m.reads(
        """
        <urn:maplib:s1> <urn:maplib:p> _:b1 .
        _:b1 <urn:maplib:p> _:b2 .
        _:b2 <urn:maplib:p> "leaf" .
        """,
        format="ntriples",
    )
    hdt_path = tmp_path / "blanks.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))

    assert isomorphic(model_as_rdflib_graph(m), model_as_rdflib_graph(m2))


def test_hdt_round_trip_preserves_backslashes_in_literals(tmp_path):
    m = Model()
    m.reads(
        "<urn:maplib:s> <urn:maplib:p> \"C:\\\\new\\\\path\" .",
        format="ntriples",
    )
    hdt_path = tmp_path / "backslash.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))
    res = m2.query("SELECT ?o WHERE { ?s ?p ?o }")
    assert res.get_column("o").to_list() == ["C:\\new\\path"]


def test_reading_truncated_hdt_file_raises_normal_exception(tmp_path):
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    hdt_path = tmp_path / "out.hdt"
    m.write(str(hdt_path), format="hdt")

    content = hdt_path.read_bytes()
    truncated_path = tmp_path / "truncated.hdt"
    truncated_path.write_bytes(content[: len(content) // 2])

    m2 = Model()
    with pytest.raises(Exception, match="HDT"):
        m2.read(str(truncated_path))


def test_write_cim_xml_format_does_not_truncate_existing_file(tmp_path):
    m = Model()
    m.reads("<urn:maplib:s> <urn:maplib:p> <urn:maplib:o> .", format="ntriples")
    out_path = tmp_path / "out.xml"
    out_path.write_text("precious")
    with pytest.raises(Exception, match="write_cim_xml"):
        m.write(str(out_path), format="cim/xml")
    assert out_path.read_text() == "precious"


def test_hdt_round_trip_preserves_language_tags(tmp_path):
    m = Model()
    m.reads(
        '<urn:maplib:s> <urn:maplib:p> "hello"@en-US .',
        format="ntriples",
    )
    hdt_path = tmp_path / "lang.hdt"
    m.write(str(hdt_path), format="hdt")

    m2 = Model()
    m2.read(str(hdt_path))

    assert m.writes(format="ntriples").strip() == m2.writes(format="ntriples").strip()
