import polars as pl
from maplib import Mapping

def test_create_mapping_from_polars_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """

    df = pl.DataFrame({"MyValue": [1, 2]})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)
    triples = mapping.to_triples()
    actual_triples_strs = [t.__repr__() for t in triples]
    actual_triples_strs.sort()
    expected_triples_strs = [
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "1"^^<http://www.w3.org/2001/XMLSchema#long>',
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "2"^^<http://www.w3.org/2001/XMLSchema#long>']
    expected_triples_strs.sort()
    assert actual_triples_strs == expected_triples_strs
