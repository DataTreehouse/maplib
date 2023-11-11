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
    df = m.query("""
            PREFIX foaf:<http://xmlns.com/foaf/0.1/>

            SELECT ?s ?v ?o WHERE {
            ?s ?v ?o .
            } ORDER BY ?s ?v ?o
            """)
    filename = TESTDATA_PATH / "read_ntriples.csv"
    #df.write_csv(str(filename))
    expected_df = pl.scan_csv(filename).collect()
    pl.testing.assert_frame_equal(df, expected_df)