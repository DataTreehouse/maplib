import polars as pl
from maplib import Mapping
from polars.testing import assert_frame_equal

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


def test_create_mapping_from_empty_polars_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """

    df = pl.DataFrame({"MyValue": []})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)

def test_create_mapping_from_empty_signature():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [] :: {
    ottr:Triple(ex:myObject, ex:hasObj, ex:myOtherObject)
    } .
    """

    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate")
    qdf = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj1 ?obj2 WHERE {
        ?obj1 ex:hasObj ?obj2
        } 
        """
    )
    expected_df = pl.DataFrame({"obj1":"http://example.net/ns#myObject",
                                "obj2":"http://example.net/ns#myOtherObject"})

    assert_frame_equal(qdf, expected_df)


def test_uri_subject_query():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [] :: {
    ottr:Triple(ex:myObject, ex:hasObj, ex:myOtherObject)
    } .
    """

    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate")
    qdf = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj2 WHERE {
        ex:myObject ex:hasObj ?obj2
        } 
        """
    )
    expected_df = pl.DataFrame({"obj2":"http://example.net/ns#myOtherObject"})

    assert_frame_equal(qdf, expected_df)


def test_create_and_write_no_bug_escaped_uri_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [xsd:anyURI ?MyURI] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyURI)
    } .
    """

    df = pl.DataFrame({"MyURI": ["http://example.net/ns#ExampleURI1", "http://example.net/ns#Ex>ampleURI2"]})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)

    ntfile = "create_and_write_no_bug_escaped_uri.nt"
    mapping.write_ntriples(ntfile)
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == ['<http://example.net/ns#myObject> <http://example.net/ns#hasValue> <http://example.net/ns#Ex\\>ampleURI2> .\n',
                     '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> <http://example.net/ns#ExampleURI1> .\n']

def test_create_and_write_no_bug_string_lit_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyString] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyString)
    } .
    """

    df = pl.DataFrame({"MyString": ["!!#\"", "ABC\#123"]})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)
    ntfile = "create_and_write_no_bug_string_lit.nt"
    mapping.write_ntriples(ntfile)
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == ['<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "!!\\#\\"" .\n',
                     '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "ABC\\\\\\#123" .\n']