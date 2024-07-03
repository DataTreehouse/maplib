import polars as pl
from maplib import Mapping, Template, IRI, Prefix, triple, Variable, Parameter
from polars.testing import assert_frame_equal

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

def test_create_mapping_with_optional_value_missing_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue, ??MyOtherValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue),
    ottr:Triple(ex:myObject, ex:hasOtherValue, ?MyOtherValue)
    } .
    """

    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)
    qres = mapping.query(
    """
    PREFIX ex:<http://example.net/ns#>
    
    SELECT ?A WHERE {
    ?obj1 ex:hasValue ?A
    } 
    """
    )
    expected_df = pl.DataFrame({"A":["A"],})
    assert_frame_equal(qres, expected_df)

    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A WHERE {
        ?obj1 ex:hasOtherValue ?A
        } 
        """
    )
    assert qres.height == 0


def test_create_programmatic_mapping_with_optional_value_missing_df():
    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Mapping()
    ex = Prefix("ex","http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value), Parameter(my_other_value, optional=True)],
        [
            triple(my_object, ex.suf("hasValue"), my_value),
            triple(my_object, ex.suf("hasOtherValue"), my_other_value)
         ]
    )
    mapping.expand(template, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A WHERE {
        ?obj1 ex:hasValue ?A
        } 
        """
    )
    expected_df = pl.DataFrame({"A":["A"],})
    assert_frame_equal(qres, expected_df)

    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A WHERE {
        ?obj1 ex:hasOtherValue ?A
        } 
        """
    )
    assert qres.height == 0

def test_create_mapping_from_empty_signature():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [] :: {
    ottr:Triple(ex:myObject, ex:hasObj, ex:myOtherObject)
    } .
    """

    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate")
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj1 ?obj2 WHERE {
        ?obj1 ex:hasObj ?obj2
        } 
        """
    )
    expected_df = pl.DataFrame({"obj1":"<http://example.net/ns#myObject>",
                                "obj2":"<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_uri_subject_query():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [] :: {
    ottr:Triple(ex:myObject, ex:hasObj, ex:myOtherObject)
    } .
    """

    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate")
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj2 WHERE {
        ex:myObject ex:hasObj ?obj2
        } 
        """
    )
    expected_df = pl.DataFrame({"obj2":"<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_programmatic_mapping():
    example_template = Template(
        iri=IRI("http://example.net/ns#ExampleTemplate"),
        parameters=[],
        instances=[triple(
            IRI("http://example.net/ns#myObject"),
            IRI("http://example.net/ns#hasObj"),
            IRI("http://example.net/ns#myOtherObject"),
        )]
    )

    mapping = Mapping()
    mapping.add_template(example_template)
    mapping.expand("http://example.net/ns#ExampleTemplate")
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj2 WHERE {
        ex:myObject ex:hasObj ?obj2
        } 
        """
    )
    expected_df = pl.DataFrame({"obj2":"<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_programmatic_mapping_to_string():
    ex = Prefix("ex", "http://example.net/ns#")
    example_template = Template(
        iri=ex.suf("ExampleTemplate"),
        parameters=[],
        instances=[triple(
            ex.suf("myObject"),
            ex.suf("hasObj"),
            ex.suf("myOtherObject"),
        )]
    )
    assert str(example_template) == """<http://example.net/ns#ExampleTemplate> [ ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#myObject>,<http://example.net/ns#hasObj>,<http://example.net/ns#myOtherObject>)
} . 
"""


def test_programmatic_mapping_with_prefix():
    ex = Prefix("ex", "http://example.net/ns#")
    example_template = Template(
        iri=ex.suf("ExampleTemplate"),
        parameters=[],
        instances=[triple(
            ex.suf("myObject"),
            ex.suf("hasObj"),
            ex.suf("myOtherObject"),
        )]
    )

    mapping = Mapping()
    mapping.expand(example_template)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj2 WHERE {
        ex:myObject ex:hasObj ?obj2
        } 
        """
    )
    expected_df = pl.DataFrame({"obj2":"<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_create_and_write_no_bug_string_lit_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyString] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyString)
    } .
    """

    df = pl.DataFrame({"MyString": ["\"!!\n#\"", "ABC#123\n\t\r"]})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)
    ntfile = "create_and_write_no_bug_string_lit.nt"
    mapping.write_ntriples(ntfile)
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == ['<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "ABC#123\\n\\t\\r" .\n',
                     '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "\\"!!\\n#\\"" .\n']

def test_create_and_write_no_bug_bool_lit_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyBool] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyBool)
    } .
    """

    df = pl.DataFrame({"MyBool": [True, False]})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)
    ntfile = "create_and_write_no_bug_bool_lit.nt"
    mapping.write_ntriples(ntfile)
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == ['<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n',
                     '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n']


def test_create_and_write_no_bug_lang_string_lit_df():
    mapping = Mapping()
    mapping.read_triples_string('<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "HELLO!!"@en .', format="ntriples")
    ntfile = "create_and_write_no_bug_string_lit.nt"
    mapping.write_ntriples(ntfile)
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == ['<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "HELLO!!"@en .\n']
