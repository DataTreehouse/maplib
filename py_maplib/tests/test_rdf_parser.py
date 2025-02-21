import polars as pl
import pytest
import rdflib
from polars.testing import assert_frame_equal
import pathlib
from maplib import Mapping

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "rdf_parser"


def test_issue_8():
    m = Mapping()
    m.read_triples(TESTDATA_PATH / "date_panic.nt", format="ntriples")
    df = m.query("""SELECT ?c WHERE {?a ?b ?c}""", native_dataframe=True)
    expected = pl.from_repr(
        """
┌────────────┐
│ c          │
│ ---        │
│ date       │
╞════════════╡
│ 2035-01-23 │
└────────────┘
    """
    )
    assert_frame_equal(df, expected)
