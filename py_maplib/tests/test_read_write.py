import polars as pl
import pytest
import rdflib
from polars.testing import assert_frame_equal
import pathlib
from maplib import Model

pl.Config.set_fmt_str_lengths(300)


PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"


def test_read_ntriples():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    res = m.query(
        """
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?s ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?s ?v ?o
            """
    ).sort(["s", "v", "o"])
    # TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "read_ntriples.csv"
    # res.write_csv(str(filename))
    expected_df = pl.scan_csv(filename).collect()
    pl.testing.assert_frame_equal(res, expected_df)

def test_read_many_different_numbers():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_nums.ttl"))
    res = m.query(
        """
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>
            PREFIX :<https://example.net/nums/> 

            SELECT ?s ?byte ?ubyte ?short ?ushort WHERE {
            ?s :byte ?byte ;
               :unsignedByte ?ubyte ;
               :short ?short ;
               :unsignedShort ?ushort .
            } 
        """
    )
    assert res.height == 1
    assert res.get_column("byte").dtype == pl.Int8
    assert res.get_column("ubyte").dtype == pl.UInt8
    assert res.get_column("short").dtype == pl.Int16
    assert res.get_column("ushort").dtype == pl.UInt16



def test_read_ntriples_twice_with_replace():
    m = Model()
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"))
    m.read(str(TESTDATA_PATH / "read_ntriples.nt"), replace_graph=True)
    res = m.query(
        """
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?s ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?s ?v ?o
            """
    ).sort(["s", "v", "o"])
    # TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "read_ntriples.csv"
    # res.write_csv(str(filename))
    assert res.shape == (8, 3)


def test_read_write_ntriples_string():
    m = Model()
    with open(TESTDATA_PATH / "read_ntriples.nt") as f:
        ntstring = f.read()
    m.reads(ntstring, format="ntriples")
    out_str = m.writes(format="ntriples")
    m2 = Model()
    m2.reads(out_str, format="ntriples")
    res = m2.query(
        """
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?v ?o
            """
    ).sort(["v", "o"])
    # TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "read_ntriples2.csv"
    # res.write_csv(str(filename))
    expected_df = pl.scan_csv(filename).select(["v", "o"]).sort(["v", "o"]).collect()
    pl.testing.assert_frame_equal(res, expected_df)


def test_read_write_turtle_string():
    m = Model()
    with open(TESTDATA_PATH / "read_ntriples.nt") as f:
        ntstring = f.read()
    m.reads(ntstring, format="ntriples")
    out_str = m.writes(format="turtle")
    m2 = Model()
    m2.reads(out_str, format="turtle")
    res = m2.query(
        """
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?v ?o
            """
    ).sort(["v", "o"])
    # TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "read_ntriples2.csv"
    # res.write_csv(str(filename))
    expected_df = pl.scan_csv(filename).select(["v", "o"]).sort(["v", "o"]).collect()
    pl.testing.assert_frame_equal(res, expected_df)


def test_read_write_xml_string():
    m = Model()
    with open(TESTDATA_PATH / "read_ntriples.nt") as f:
        ntstring = f.read()
    m.reads(ntstring, format="ntriples")
    out_str = m.writes(format="rdf/xml")
    m2 = Model()
    m2.reads(out_str, format="rdf/xml")
    res = m2.query(
        """
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?v ?o
            """
    ).sort(["v", "o"])
    # TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "read_ntriples2.csv"
    # res.write_csv(str(filename))
    expected_df = pl.scan_csv(filename).select(["v", "o"]).sort(["v", "o"]).collect()
    pl.testing.assert_frame_equal(res, expected_df)

