import polars as pl
import pathlib
from maplib import Model

pl.Config.set_fmt_str_lengths(300)


PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

def test_resolve_predicate():
    m = Model()
    m.add_prefixes({"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
    m.add_prefixes({"ex": "https://example.net/"})

    df = pl.DataFrame({
        "subject": ["http://example.net/subject1"],
        "object": ["http://example.net/object1"],
    })

    m.map_triples(df, predicate="rdf:type")

    result = m.query("""
        SELECT ?s ?o WHERE {
            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o .
        }
    """)

    assert result.height == 1
    assert result["s"][0] == "<http://example.net/subject1>"
    assert result["o"][0] == "<http://example.net/object1>"