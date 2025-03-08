import polars as pl
from maplib import (
    Mapping,
    Template,
    IRI,
    Prefix,
    Triple,
    Variable,
    Parameter,
    Literal,
    XSD,
    RDFType,
)
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


def test_add_template_instead_of_constructor_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """

    df = pl.DataFrame({"MyValue": []})
    mapping = Mapping()
    mapping.add_template(doc)
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
    expected_df = pl.DataFrame(
        {
            "A": ["A"],
        }
    )
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
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value), Parameter(my_other_value, optional=True)],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
            Triple(my_object, ex.suf("hasOtherValue"), my_other_value),
        ],
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
    expected_df = pl.DataFrame(
        {
            "A": ["A"],
        }
    )
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


def test_create_programmatic_mapping_with_default():
    xsd = XSD()
    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    yet_another_value = Variable("YetAnotherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(my_value),
            Parameter(my_other_value, default_value=Literal("123")),
            Parameter(
                yet_another_value, default_value=Literal("123", data_type=xsd.integer)
            ),
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
            Triple(my_object, ex.suf("hasOtherValue"), my_other_value),
            Triple(my_object, ex.suf("hasYetAnotherValue"), yet_another_value),
        ],
    )
    mapping.expand(template, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A ?B WHERE {
        ?obj1 ex:hasOtherValue ?A .
        ?obj1 ex:hasYetAnotherValue ?B .
        } 
        """
    )
    expected_df = pl.DataFrame({"A": ["123"], "B": [123]})
    assert_frame_equal(qres, expected_df)


def test_create_programmatic_mapping_with_nested_default():
    xsd = XSD()
    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    yet_another_value = Variable("YetAnotherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(my_value),
            Parameter(my_other_value, default_value=Literal("123")),
            Parameter(
                yet_another_value, default_value=Literal("123", data_type=xsd.integer)
            ),
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
            Triple(my_object, ex.suf("hasOtherValue"), my_other_value),
            Triple(my_object, ex.suf("hasYetAnotherValue"), yet_another_value),
        ],
    )
    template2 = Template(
        ex.suf("ExampleTemplate2"),
        [Parameter(my_value)],
        [template.instance([my_value, None, None])],
    )
    mapping.add_template(template)
    mapping.expand(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A ?B WHERE {
        ?obj1 ex:hasOtherValue ?A .
        ?obj1 ex:hasYetAnotherValue ?B .
        } 
        """
    )
    expected_df = pl.DataFrame({"A": ["123"], "B": [123]})
    assert_frame_equal(qres, expected_df)


def test_create_programmatic_mapping_with_nested_default_and_missing_column():
    xsd = XSD()
    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    yet_another_value = Variable("YetAnotherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(my_value),
            Parameter(my_other_value, default_value=Literal("123")),
            Parameter(
                yet_another_value, default_value=Literal("123", data_type=xsd.integer)
            ),
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
            Triple(my_object, ex.suf("hasOtherValue"), my_other_value),
            Triple(my_object, ex.suf("hasYetAnotherValue"), yet_another_value),
        ],
    )
    template2 = Template(
        ex.suf("ExampleTemplate2"),
        [
            Parameter(my_value),
            Parameter(my_other_value, optional=True),
            Parameter(yet_another_value, optional=True),
        ],
        [template.instance([my_value, my_other_value, yet_another_value])],
    )
    mapping.add_template(template)
    mapping.expand(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A ?B WHERE {
        ?obj1 ex:hasOtherValue ?A .
        ?obj1 ex:hasYetAnotherValue ?B .
        } 
        """
    )
    expected_df = pl.DataFrame({"A": ["123"], "B": [123]})
    assert_frame_equal(qres, expected_df)


def test_create_programmatic_mapping_with_nested_partial_default():
    xsd = XSD()
    df = pl.DataFrame(
        {
            "MyValue": ["A", "B"],
            "MyOtherValue": ["321", None],
            "YetAnotherValue": [None, 321],
        }
    )
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    yet_another_value = Variable("YetAnotherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(my_value),
            Parameter(my_other_value, default_value=Literal("123")),
            Parameter(
                yet_another_value, default_value=Literal("123", data_type=xsd.long)
            ),
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
            Triple(my_object, ex.suf("hasOtherValue"), my_other_value),
            Triple(my_object, ex.suf("hasYetAnotherValue"), yet_another_value),
        ],
    )
    template2 = Template(
        ex.suf("ExampleTemplate2"),
        [
            my_value,
            Parameter(my_other_value, optional=True),
            Parameter(yet_another_value, optional=True),
        ],
        [template.instance([my_value, my_other_value, yet_another_value])],
    )
    mapping.add_template(template)
    mapping.expand(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?S ?A ?B WHERE {
        ?S ex:hasOtherValue ?A .
        ?S ex:hasYetAnotherValue ?B .
        } ORDER BY ?S ?A ?B
        """
    )
    expected_df = pl.DataFrame(
        {
            "S": [f"<{my_object.iri}>"] * 4,
            "A": ["123", "123", "321", "321"],
            "B": [123, 321, 123, 321],
        }
    )
    assert_frame_equal(qres, expected_df)


def test_create_programmatic_mapping_with_nested_none_optional():
    xsd = XSD()
    df = pl.DataFrame({"MyValue": ["A", "B"]})
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            my_value,
            Parameter(
                my_other_value, optional=True, rdf_type=RDFType.Literal(xsd.string)
            ),
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
            Triple(my_object, ex.suf("hasOtherValue"), my_other_value),
        ],
    )
    template2 = Template(
        ex.suf("ExampleTemplate2"), [my_value], [template.instance([my_value, None])]
    )
    mapping.add_template(template)
    mapping.expand(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?S ?A ?B WHERE {
        ?S ex:hasValue ?A .
        OPTIONAL {
            ?S ex:hasOtherValue ?B .
        }
        } ORDER BY ?S ?A ?B
        """
    )
    expected_df = pl.DataFrame(
        {"S": [f"<{my_object.iri}>"] * 2, "A": ["A", "B"], "B": [None, None]}
    )
    expected_df = expected_df.with_columns(pl.col("B").cast(pl.Boolean))
    assert_frame_equal(qres, expected_df)


def test_create_programmatic_mapping_with_partial_default():
    xsd = XSD()
    df = pl.DataFrame(
        {
            "MyValue": ["A", "B"],
            "MyOtherValue": ["321", None],
            "YetAnotherValue": [None, 321],
        }
    )
    mapping = Mapping()
    ex = Prefix("ex", "http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    yet_another_value = Variable("YetAnotherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(my_value),
            Parameter(my_other_value, default_value=Literal("123")),
            Parameter(
                yet_another_value, default_value=Literal("123", data_type=xsd.long)
            ),
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
            Triple(my_object, ex.suf("hasOtherValue"), my_other_value),
            Triple(my_object, ex.suf("hasYetAnotherValue"), yet_another_value),
        ],
    )
    mapping.expand(template, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?S ?A ?B WHERE {
        ?S ex:hasOtherValue ?A .
        ?S ex:hasYetAnotherValue ?B .
        } ORDER BY ?S ?A ?B
        """
    )
    expected_df = pl.DataFrame(
        {
            "S": [f"<{my_object.iri}>"] * 4,
            "A": ["123", "123", "321", "321"],
            "B": [123, 321, 123, 321],
        }
    )
    assert_frame_equal(qres, expected_df)


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
    expected_df = pl.DataFrame(
        {
            "obj1": "<http://example.net/ns#myObject>",
            "obj2": "<http://example.net/ns#myOtherObject>",
        }
    )

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
    expected_df = pl.DataFrame({"obj2": "<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_programmatic_mapping():
    example_template = Template(
        iri=IRI("http://example.net/ns#ExampleTemplate"),
        parameters=[],
        instances=[
            Triple(
                IRI("http://example.net/ns#myObject"),
                IRI("http://example.net/ns#hasObj"),
                IRI("http://example.net/ns#myOtherObject"),
            )
        ],
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
    expected_df = pl.DataFrame({"obj2": "<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_programmatic_mapping_to_string():
    ex = Prefix("ex", "http://example.net/ns#")
    example_template = Template(
        iri=ex.suf("ExampleTemplate"),
        parameters=[],
        instances=[
            Triple(
                ex.suf("myObject"),
                ex.suf("hasObj"),
                ex.suf("myOtherObject"),
            )
        ],
    )
    assert (
        str(example_template)
        == """<http://example.net/ns#ExampleTemplate> [ ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#myObject>,<http://example.net/ns#hasObj>,<http://example.net/ns#myOtherObject>)
} . 
"""
    )


def test_programmatic_mapping_with_prefix():
    ex = Prefix("ex", "http://example.net/ns#")
    example_template = Template(
        iri=ex.suf("ExampleTemplate"),
        parameters=[],
        instances=[
            Triple(
                ex.suf("myObject"),
                ex.suf("hasObj"),
                ex.suf("myOtherObject"),
            )
        ],
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
    expected_df = pl.DataFrame({"obj2": "<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_create_and_write_no_bug_string_lit_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyString] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyString)
    } .
    """

    df = pl.DataFrame({"MyString": ['"!!\n#"', "ABC#123\n\t\r"]})
    mapping = Mapping([doc])
    mapping.expand("http://example.net/ns#ExampleTemplate", df)
    ntfile = "create_and_write_no_bug_string_lit.nt"
    mapping.write_ntriples(ntfile)
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == [
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "ABC#123\\n\\t\\r" .\n',
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "\\"!!\\n#\\"" .\n',
    ]


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
    assert lines == [
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n',
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n',
    ]


def test_create_and_write_no_bug_lang_string_lit_df():
    mapping = Mapping()
    mapping.read_triples_string(
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "HELLO!!"@en .',
        format="ntriples",
    )
    ntfile = "create_and_write_no_bug_string_lit.nt"
    mapping.write_ntriples(ntfile)
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == [
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "HELLO!!"@en .\n'
    ]


def test_nested_template_empty_list():
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ ?MyValue ] :: {
  cross | <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,++?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [] :: {
   <http://example.net/ns#ExampleNestedTemplate>((1,2))
} . 
    """
    mapping = Mapping(templates)
    mapping.expand("http://example.net/ns#ExampleTemplate")
    r = mapping.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(XSD().integer)


def test_bool_func():
    m = Mapping([])
    df = m.query(
        """
    PREFIX : <http://example.net/> 
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?a WHERE {
    VALUES ?a {"true" "false"}
    FILTER(xsd:boolean(?a))
    }
    """
    )
    assert_frame_equal(df, pl.DataFrame({"a": ["true"]}))


def test_expand_triples():
    df = pl.DataFrame(
        {
            "subject": [
                "http://example.net/ns#A",
                "http://example.net/ns#B",
                "http://example.net/ns#C",
            ],
            "object": [
                "http://example.net/ns#D",
                "http://example.net/ns#E",
                "http://example.net/ns#F",
            ],
        }
    )
    types = {"subject": RDFType.IRI(), "object": RDFType.IRI()}
    verb = "http://example.net/ns#hasRel"
    m = Mapping()
    m.expand_triples(df, types=types, verb=verb)
    df = m.query(
        """
    SELECT ?a ?b ?c WHERE {?a ?b ?c} ORDER BY ?a ?b ?c
    """
    )
    expected = pl.from_repr(
        """
┌───────────────────────────┬────────────────────────────────┬───────────────────────────┐
│ a                         ┆ b                              ┆ c                         │
│ ---                       ┆ ---                            ┆ ---                       │
│ str                       ┆ str                            ┆ str                       │
╞═══════════════════════════╪════════════════════════════════╪═══════════════════════════╡
│ <http://example.net/ns#A> ┆ <http://example.net/ns#hasRel> ┆ <http://example.net/ns#D> │
│ <http://example.net/ns#B> ┆ <http://example.net/ns#hasRel> ┆ <http://example.net/ns#E> │
│ <http://example.net/ns#C> ┆ <http://example.net/ns#hasRel> ┆ <http://example.net/ns#F> │
└───────────────────────────┴────────────────────────────────┴───────────────────────────┘
    """
    )
    assert_frame_equal(df, expected)



def test_expand_default_triples():
    df = pl.DataFrame(
        {
            "subject": [
                "http://example.net/ns#A",
                "http://example.net/ns#B",
                "http://example.net/ns#C",
            ],
            "object": [
                "http://example.net/ns#D",
                "http://example.net/ns#E",
                "http://example.net/ns#F",
            ],
        }
    )
    m = Mapping()
    tpl = m.expand_default(df, primary_key_column="subject")
    #print(tpl)
    df = m.query(
        """
    SELECT ?a ?b ?c WHERE {?a ?b ?c} ORDER BY ?a ?b ?c
    """
    )
    expected = pl.from_repr(
        """
┌───────────────────────────┬────────────────────────────────┬───────────────────────────┐
│ a                         ┆ b                              ┆ c                         │
│ ---                       ┆ ---                            ┆ ---                       │
│ str                       ┆ str                            ┆ str                       │
╞═══════════════════════════╪════════════════════════════════╪═══════════════════════════╡
│ <http://example.net/ns#A> ┆ <urn:maplib_default:object>    ┆ <http://example.net/ns#D> │
│ <http://example.net/ns#B> ┆ <urn:maplib_default:object>    ┆ <http://example.net/ns#E> │
│ <http://example.net/ns#C> ┆ <urn:maplib_default:object>    ┆ <http://example.net/ns#F> │
└───────────────────────────┴────────────────────────────────┴───────────────────────────┘
    """
    )
    assert_frame_equal(df, expected)

def test_expand_default_triples_non_iri_object():
    df = pl.DataFrame(
        {
            "subject": [
                "http://example.net/ns#A",
                "http://example.net/ns#B",
                "http://example.net/ns#C",
            ],
            "object": [
                "1",
                "3",
                "5",
            ],
        }
    )
    m = Mapping()
    tpl = m.expand_default(df, primary_key_column="subject")
    #print(tpl)
    df = m.query(
        """
    SELECT ?a ?b ?c WHERE {?a ?b ?c} ORDER BY ?a ?b ?c
    """
    )
    expected = pl.from_repr(
        """
┌───────────────────────────┬────────────────────────────────┬───────────────────────────┐
│ a                         ┆ b                              ┆ c                         │
│ ---                       ┆ ---                            ┆ ---                       │
│ str                       ┆ str                            ┆ str                       │
╞═══════════════════════════╪════════════════════════════════╪═══════════════════════════╡
│ <http://example.net/ns#A> ┆ <urn:maplib_default:object>    ┆ 1                         │
│ <http://example.net/ns#B> ┆ <urn:maplib_default:object>    ┆ 3                         │
│ <http://example.net/ns#C> ┆ <urn:maplib_default:object>    ┆ 5                         │
└───────────────────────────┴────────────────────────────────┴───────────────────────────┘
    """
    )
    assert_frame_equal(df, expected)


def test_expand_generated_default_triples_non_iri_object():
    df = pl.DataFrame(
        {
            "subject": [
                "http://example.net/ns#A",
                "http://example.net/ns#B",
                "http://example.net/ns#C",
            ],
            "object": [
                "1",
                "3",
                "5",
            ],
        }
    )
    m = Mapping()
    tpl = """<urn:maplib_default:default_template_0> [ <http://ns.ottr.xyz/0.4/IRI> ?subject,  <http://www.w3.org/2001/XMLSchema#string> ?object ] :: {
                ottr:Triple(?subject,<urn:maplib_default:object>,?object)
            } . """
    m.expand(tpl, df)
    df = m.query(
        """
    SELECT ?a ?b ?c WHERE {?a ?b ?c} ORDER BY ?a ?b ?c
    """
    )
    expected = pl.from_repr(
        """
┌───────────────────────────┬────────────────────────────────┬───────────────────────────┐
│ a                         ┆ b                              ┆ c                         │
│ ---                       ┆ ---                            ┆ ---                       │
│ str                       ┆ str                            ┆ str                       │
╞═══════════════════════════╪════════════════════════════════╪═══════════════════════════╡
│ <http://example.net/ns#A> ┆ <urn:maplib_default:object>    ┆ 1                         │
│ <http://example.net/ns#B> ┆ <urn:maplib_default:object>    ┆ 3                         │
│ <http://example.net/ns#C> ┆ <urn:maplib_default:object>    ┆ 5                         │
└───────────────────────────┴────────────────────────────────┴───────────────────────────┘
    """
    )
    assert_frame_equal(df, expected)

