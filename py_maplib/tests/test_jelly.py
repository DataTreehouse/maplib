import polars as pl
import pathlib

from polars import read_csv
from polars.testing import assert_frame_equal

from maplib import Model

from rdflib import Graph

pl.Config.set_fmt_str_lengths(300)


PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

def test_write_jelly():
    m = Model()
    m.read(TESTDATA_PATH / "sunspots.ttl")

    filename = TESTDATA_PATH / "output.jelly"
    m.write(filename, format="jelly")
    
    g = Graph()
    g.parse(filename, format="jelly")
    
    print("Triples from Jelly file:")
    for s, p, o in g:
        print(f"{s} {p} {o}")

def test_read_jelly():
    m = Model()
    if not (TESTDATA_PATH / "output.jelly").exists():
        test_write_jelly()

    filename = TESTDATA_PATH / "output.jelly"

    m.read(filename, format="jelly")

    df = m.query(
        """
            SELECT ?s ?p ?o WHERE {
                ?s ?p ?o .
            } ORDER BY ?s ?p ?o
        """
    )

    df.write_csv(TESTDATA_PATH / "output.csv")
    read_csv(TESTDATA_PATH / "output.csv")

    expected = read_csv(TESTDATA_PATH / "output.csv")

    print("\nDataFrame from Jelly file:")
    print(df)
    print("Expected DataFrame:")
    print(expected)

    assert_frame_equal(df, expected)