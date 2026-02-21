from polars.testing import assert_frame_equal
import polars as pl
from maplib import Model
import pathlib
import pytest

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "compressed_iri_issue"

# This test checks an issue with sorting compressed IRIs.
# The naive sorting by prefix first and then by suffix does not work in general.
# See implementation of Ord for in Cats Compressed IRI.
@pytest.mark.parametrize("streaming", [True, False])
def test_compressed_iri_issue(streaming):
    m = Model()
    shape_graph = "urn:eu:shacl"
    m.read(TESTDATA_PATH / "skos-era-skos-SubCategories.ttl", graph=shape_graph)
    m.read(TESTDATA_PATH / "skos-era-skos-SubCategories.ttl", graph=shape_graph)
    m.read(TESTDATA_PATH / "skos-era-skos-SubCategories.ttl", graph=shape_graph)
    m.read(TESTDATA_PATH / "skos-era-skos-SubCategories.ttl", graph=shape_graph)
    m.read(TESTDATA_PATH / "skos-era-skos-SubCategories.ttl", graph=shape_graph)

