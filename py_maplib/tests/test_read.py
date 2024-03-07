import polars as pl
import pytest
import rdflib
from polars.testing import assert_frame_equal
import pathlib
from maplib import Mapping

pl.Config.set_fmt_str_lengths(300)


PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

def test_read_ntriples():
    m = Mapping()
    m.read_triples(str(TESTDATA_PATH / "read_ntriples.nt"))
    res = m.query("""
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?s ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?s ?v ?o
            """).sort(["s", "v", "o"])
    # TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "read_ntriples.csv"
    #res.write_csv(str(filename))
    expected_df = pl.scan_csv(filename).collect()
    pl.testing.assert_frame_equal(res, expected_df)

def test_read_write_ntriples_string():
    m = Mapping()
    with open(TESTDATA_PATH / "read_ntriples.nt") as f:
        ntstring = f.read()
    m.read_triples_string(ntstring, format="ntriples")
    out_str = m.write_ntriples_string()
    m2 = Mapping()
    m2.read_triples_string(out_str, format="ntriples")
    res = m2.query("""
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?v ?o
            """).sort(["v", "o"])
    #TODO: Fix multitype sorting
    filename = TESTDATA_PATH / "read_ntriples2.csv"
    #res.write_csv(str(filename))
    expected_df = pl.scan_csv(filename).select(["v", "o"]).sort(["v", "o"]).collect()
    pl.testing.assert_frame_equal(res, expected_df)