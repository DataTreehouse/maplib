import polars as pl
import pytest

from maplib import (
    Model,
    Template,
    IRI,
    Prefix,
    Triple,
    Variable,
    Parameter,
    Literal,
    xsd,
    RDFType,
)
from polars.testing import assert_frame_equal

def test_write_empty_model():
    m = Model()
    s = m.writes()

def test_create_model_from_empty_polars_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
        ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """

    df = pl.DataFrame({"MyValue": []})
    model = Model()
    model.add_template(doc)
    model.map("http://example.net/ns#ExampleTemplate", df)


def test_add_template_instead_of_constructor_df():
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """

    df = pl.DataFrame({"MyValue": []})
    model = Model()
    model.add_template(doc)
    model.map("http://example.net/ns#ExampleTemplate", df)


@pytest.mark.parametrize("streaming", [True, False])
def test_create_model_with_optional_value_missing_df(streaming):
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue, ??MyOtherValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue),
    ottr:Triple(ex:myObject, ex:hasOtherValue, ?MyOtherValue)
    } .
    """

    df = pl.DataFrame({"MyValue": ["A"]})
    model = Model()
    model.add_template(doc)
    model.map("http://example.net/ns#ExampleTemplate", df)
    qres = model.query(
        """
    PREFIX ex:<http://example.net/ns#>
    
    SELECT ?A WHERE {
    ?obj1 ex:hasValue ?A
    } 
    """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame(
        {
            "A": ["A"],
        }
    )
    assert_frame_equal(qres, expected_df)

    qres = model.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A WHERE {
        ?obj1 ex:hasOtherValue ?A
        } 
        """,
        streaming=streaming,
    )
    assert qres.height == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_create_programmatic_model_with_optional_value_missing_df(streaming):
    df = pl.DataFrame({"MyValue": ["A"]})
    model = Model()
    ex = Prefix("http://example.net/ns#")
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
    model.map(template, df)
    qres = model.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A WHERE {
        ?obj1 ex:hasValue ?A
        } 
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame(
        {
            "A": ["A"],
        }
    )
    assert_frame_equal(qres, expected_df)

    qres = model.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A WHERE {
        ?obj1 ex:hasOtherValue ?A
        } 
        """,
        streaming=streaming,
    )
    assert qres.height == 0


@pytest.mark.parametrize("streaming", [True, False])
def test_create_programmatic_model_with_default(streaming):
    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Model()
    ex = Prefix("http://example.net/ns#")
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
    mapping.map(template, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A ?B WHERE {
        ?obj1 ex:hasOtherValue ?A .
        ?obj1 ex:hasYetAnotherValue ?B .
        } 
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame({"A": ["123"], "B": [123]})
    assert_frame_equal(qres, expected_df)


@pytest.mark.parametrize("streaming", [True, False])
def test_create_programmatic_mapping_with_nested_default(streaming):
    
    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Model()
    ex = Prefix("http://example.net/ns#")
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
    mapping.map(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A ?B WHERE {
        ?obj1 ex:hasOtherValue ?A .
        ?obj1 ex:hasYetAnotherValue ?B .
        } 
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame({"A": ["123"], "B": [123]})
    assert_frame_equal(qres, expected_df)


@pytest.mark.parametrize("streaming", [True, False])
def test_create_programmatic_mapping_with_nested_default_and_missing_column(streaming):
    
    df = pl.DataFrame({"MyValue": ["A"]})
    mapping = Model()
    ex = Prefix("http://example.net/ns#")
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
    mapping.map(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?A ?B WHERE {
        ?obj1 ex:hasOtherValue ?A .
        ?obj1 ex:hasYetAnotherValue ?B .
        } 
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame({"A": ["123"], "B": [123]})
    assert_frame_equal(qres, expected_df)


@pytest.mark.parametrize("streaming", [True, False])
def test_create_programmatic_mapping_with_nested_partial_default(streaming):
    
    df = pl.DataFrame(
        {
            "MyValue": ["A", "B"],
            "MyOtherValue": ["321", None],
            "YetAnotherValue": [None, 321],
        }
    )
    mapping = Model()
    ex = Prefix("http://example.net/ns#")
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
    mapping.map(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?S ?A ?B WHERE {
        ?S ex:hasOtherValue ?A .
        ?S ex:hasYetAnotherValue ?B .
        } ORDER BY ?S ?A ?B
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame(
        {
            "S": [f"<{my_object.iri}>"] * 4,
            "A": ["123", "123", "321", "321"],
            "B": [123, 321, 123, 321],
        }
    )
    assert_frame_equal(qres, expected_df)


@pytest.mark.parametrize("streaming", [True, False])
def test_create_programmatic_mapping_with_nested_none_optional(streaming):
    
    df = pl.DataFrame({"MyValue": ["A", "B"]})
    mapping = Model()
    ex = Prefix("http://example.net/ns#")
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
    mapping.map(template2, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?S ?A ?B WHERE {
        ?S ex:hasValue ?A .
        OPTIONAL {
            ?S ex:hasOtherValue ?B .
        }
        } ORDER BY ?S ?A ?B
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame(
        {"S": [f"<{my_object.iri}>"] * 2, "A": ["A", "B"], "B": [None, None]}
    )
    expected_df = expected_df.with_columns(pl.col("B").cast(pl.String))
    assert_frame_equal(qres, expected_df)


@pytest.mark.parametrize("streaming", [True, False])
def test_create_programmatic_mapping_with_partial_default(streaming):
    
    df = pl.DataFrame(
        {
            "MyValue": ["A", "B"],
            "MyOtherValue": ["321", None],
            "YetAnotherValue": [None, 321],
        }
    )
    mapping = Model()
    ex = Prefix("http://example.net/ns#")
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
    mapping.map(template, df)
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>
        
        SELECT ?S ?A ?B WHERE {
        ?S ex:hasOtherValue ?A .
        ?S ex:hasYetAnotherValue ?B .
        } ORDER BY ?S ?A ?B
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame(
        {
            "S": [f"<{my_object.iri}>"] * 4,
            "A": ["123", "123", "321", "321"],
            "B": [123, 321, 123, 321],
        }
    )
    assert_frame_equal(qres, expected_df)


@pytest.mark.parametrize("streaming", [True, False])
def test_create_mapping_from_empty_signature(streaming):
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [] :: {
    ottr:Triple(ex:myObject, ex:hasObj, ex:myOtherObject)
    } .
    """

    mapping = Model()
    mapping.add_template(doc)
    mapping.map("http://example.net/ns#ExampleTemplate")
    qres = mapping.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj1 ?obj2 WHERE {
        ?obj1 ex:hasObj ?obj2
        } 
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame(
        {
            "obj1": "<http://example.net/ns#myObject>",
            "obj2": "<http://example.net/ns#myOtherObject>",
        }
    )

    assert_frame_equal(qres, expected_df)


@pytest.mark.parametrize("streaming", [True, False])
def test_uri_subject_query(streaming):
    doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [] :: {
    ottr:Triple(ex:myObject, ex:hasObj, ex:myOtherObject)
    } .
    """

    model = Model()
    model.add_template(doc)
    model.map("http://example.net/ns#ExampleTemplate")
    qres = model.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj2 WHERE {
        ex:myObject ex:hasObj ?obj2
        } 
        """,
        streaming=streaming,
    )
    expected_df = pl.DataFrame({"obj2": "<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_programmatic_model():
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

    model = Model()
    model.add_template(example_template)
    model.map("http://example.net/ns#ExampleTemplate")
    qres = model.query(
        """
        PREFIX ex:<http://example.net/ns#>

        SELECT ?obj2 WHERE {
        ex:myObject ex:hasObj ?obj2
        } 
        """
    )
    expected_df = pl.DataFrame({"obj2": "<http://example.net/ns#myOtherObject>"})

    assert_frame_equal(qres, expected_df)


def test_programmatic_model_to_string():
    ex = Prefix("http://example.net/ns#", prefix_name="ex")
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
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#myObject>, <http://example.net/ns#hasObj>, <http://example.net/ns#myOtherObject>)
} . 
"""
    )


def test_programmatic_model_with_prefix():
    ex = Prefix("http://example.net/ns#", "ex")
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

    model = Model()
    model.map(example_template)
    qres = model.query(
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
    model = Model()
    model.add_template(doc)
    model.map("http://example.net/ns#ExampleTemplate", df)
    ntfile = "create_and_write_no_bug_string_lit.nt"
    model.write(ntfile, format="ntriples")
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
    model = Model()
    model.add_template(doc)
    model.map("http://example.net/ns#ExampleTemplate", df)
    ntfile = "create_and_write_no_bug_bool_lit.nt"
    model.write(ntfile, format="ntriples")
    with open(ntfile) as f:
        lines = f.readlines()

    lines.sort()
    assert lines == [
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n',
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n',
    ]


def test_create_and_write_no_bug_lang_string_lit_df():
    model = Model()
    model.reads(
        '<http://example.net/ns#myObject> <http://example.net/ns#hasValue> "HELLO!!"@en .',
        format="ntriples",
    )
    ntfile = "create_and_write_no_bug_string_lit.nt"
    model.write(ntfile, format="ntriples")
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
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate")
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.integer)


def test_bool_func():
    m = Model()
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


def test_map_triples():
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
    types = {"subject": RDFType.IRI, "object": RDFType.IRI}
    predicate = "http://example.net/ns#hasRel"
    m = Model()
    m.map_triples(df, types=types, predicate=predicate)
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


def test_map_default_triples():
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
    m = Model()
    tpl = m.map_default(df, primary_key_column="subject")
    # print(tpl)
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


def test_map_default_triples_non_iri_object():
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
    m = Model()
    tpl = m.map_default(df, primary_key_column="subject")
    # print(tpl)
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


def test_map_generated_default_triples_non_iri_object():
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
    m = Model()
    tpl = """<urn:maplib_default:default_template_0> [ <http://ns.ottr.xyz/0.4/IRI> ?subject,  <http://www.w3.org/2001/XMLSchema#string> ?object ] :: {
                ottr:Triple(?subject,<urn:maplib_default:object>,?object)
            } . """
    m.map(tpl, df)
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



def test_list_expansion_correct():
    df = pl.DataFrame(
        {
            "MySubject": [
                "http://example.net/ns#subject1",
                "http://example.net/ns#subject2",
            ],
            "MyValue": [[1, 2], [3, 4]],
        }
    )
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleTemplate> [ ottr:IRI ?MySubject, List<rdfs:Resource> ?MyValue ] :: {
  ottr:Triple(?MySubject,<http://example.net/ns#hasValueList>,?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    df = model.query(
        """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
    SELECT ?a ?e1 ?e2 ?e3 WHERE {
        ?a <http://example.net/ns#hasValueList> ?b .
        ?b rdf:first ?e1 .
        ?b rdf:rest ?c .
        ?c rdf:first ?e2 .
        ?c rdf:rest ?e3 .
    } ORDER BY ?a
    """
    )
    expected = pl.from_repr(
        """
┌──────────────────────────────────┬─────┬─────┬──────────────────────────────────────────────────┐
│ a                                ┆ e1  ┆ e2  ┆ e3                                               │
│ ---                              ┆ --- ┆ --- ┆ ---                                              │
│ str                              ┆ i64 ┆ i64 ┆ str                                              │
╞══════════════════════════════════╪═════╪═════╪══════════════════════════════════════════════════╡
│ <http://example.net/ns#subject1> ┆ 1   ┆ 2   ┆ <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> │
│ <http://example.net/ns#subject2> ┆ 3   ┆ 4   ┆ <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> │
└──────────────────────────────────┴─────┴─────┴──────────────────────────────────────────────────┘
    """
    )
    assert_frame_equal(df, expected)
