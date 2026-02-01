import pytest

from maplib import Model, MaplibException
from polars.testing import assert_frame_equal
import polars as pl
import pathlib

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "jsonlds"

def test_read_jsonld():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.read(TESTDATA_PATH / "test.jsonld")

def test_csvw_example():
    CSVW_PATH = TESTDATA_PATH / "csvw.jsonld"
    with open(CSVW_PATH) as f:
        csvw = f.read()
    m = Model()
    m.read(TESTDATA_PATH / "grit_bins.json", known_contexts={"http://www.w3.org/ns/csvw":csvw})
    df = m.query("SELECT * WHERE {?a ?b ?c}")
    assert df.height == 17