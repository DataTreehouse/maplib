import polars as pl
import pathlib
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

    # m2 = Model()
    # m2.read(filename, format="jelly")
    #
    # query = """
    # SELECT ?s ?p ?o WHERE {
    #     ?s ?p ?o .
    # } ORDER BY ?s ?p ?o
    # """
    # original = m.query(query).df
    # read_back = m2.query(query).df
    #
    # assert original.frame_equal(read_back), (
    #     f"Read back mismatch: \nOriginal:\n{original}\nRead back:\n{read_back}"
    # )
    
    g = Graph()
    g.parse(filename, format="jelly")
    
    print("Triples from Jelly file:")
    for s, p, o in g:
        print(f"{s} {p} {o}")