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
    xsd,
    RDFType,
    Argument,
)
from polars.testing import assert_frame_equal


def test_want_float_got_int64():
    df = pl.DataFrame({"MyValue": [1]})
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal(xsd.float))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    with pytest.raises(Exception):
        model.map(template, df)


def test_want_rdfs_literal_got_int64():
    df = pl.DataFrame({"MyValue": [1]})
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(
                my_value,
                rdf_type=RDFType.Literal(
                    "http://www.w3.org/2000/01/rdf-schema#Literal"
                ),
            )
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    model.map(template, df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_want_rdfs_resource_got_int64():
    df = pl.DataFrame({"MyValue": [1]})
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(
                my_value,
                rdf_type=RDFType.Literal(
                    "http://www.w3.org/2000/01/rdf-schema#Resource"
                ),
            )
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    model.map(template, df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_want_xsd_int_got_xsd_boolean():
    
    df = pl.DataFrame({"MyValue": [True]})
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal(xsd.int_))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    with pytest.raises(Exception):
        model.map(template, df)


def test_autoconverted_datetime_to_date():
    
    df = pl.DataFrame({"MyValue": ["2020-02-02T00:00:00Z"]}).cast(pl.Datetime("ns"))
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal(xsd.date))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    model.map(template, df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.date)


def test_autoconverted_optional_datetime_to_date():
    
    df = pl.DataFrame({"MyValue": ["2020-02-02T00:00:00Z"]}).cast(pl.Datetime("ns"))
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_other_value = Variable("MyOtherValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [
            Parameter(my_value, rdf_type=RDFType.Literal(xsd.date)),
            Parameter(
                my_other_value, rdf_type=RDFType.Literal(xsd.date), optional=True
            ),
        ],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    model.map(template, df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.date)


def test_autoconverted_datetime_list_to_date_list_1():
    
    df = pl.DataFrame({"MyValue": [["2020-02-02T00:00:00Z"]]}).cast(
        pl.List(pl.Datetime("ns"))
    )
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Nested(RDFType.Literal(xsd.date)))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    model.map(template, df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Multi(
        [
            RDFType.IRI,
            RDFType.BlankNode,
            RDFType.Literal("http://www.w3.org/2001/XMLSchema#date"),
        ]
    )


def test_autoconverted_datetime_list_to_date_list_2():
    
    df = pl.DataFrame({"MyValue": [["2020-02-02T00:00:00Z"]]}).cast(
        pl.List(pl.Datetime("ns"))
    )
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Nested(RDFType.Literal(xsd.date)))],
        [
            Triple(
                my_object,
                ex.suf("hasValue"),
                Argument(my_value, list_expand=True),
                list_expander="cross",
            ),
        ],
    )
    model.map(template, df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal("http://www.w3.org/2001/XMLSchema#date")


def test_want_xsd_long_got_xsd_short():
    
    df = pl.DataFrame({"MyValue": [1]})
    df = df.with_columns(pl.col("MyValue").cast(pl.Int16))
    model = Model()
    ex = Prefix("http://example.net/ns#")
    my_value = Variable("MyValue")
    my_object = ex.suf("MyObject")
    template = Template(
        ex.suf("ExampleTemplate"),
        [Parameter(my_value, rdf_type=RDFType.Literal(xsd.long))],
        [
            Triple(my_object, ex.suf("hasValue"), my_value),
        ],
    )
    model.map(template, df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.short)


def test_nested_template_more_general():
    df = pl.DataFrame({"MyValue": ["hello"]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://example.net/ns#ExampleNestedTemplate> [ rdfs:Literal ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ xsd:string ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.string)


def test_nested_template_not_found():
    df = pl.DataFrame({"MyValue": ["hello"]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://example.net/ns#ExampleNestedTemplate> [ rdfs:Literal ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ xsd:string ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplateWrong>(?MyValue)
} . 
    """
    with pytest.raises(Exception):
        m = Model(templates)
        m.map("http://example.net/ns#ExampleTemplate", df)


def test_nested_template_more_specific():
    df = pl.DataFrame({"MyValue": ["hello"]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://example.net/ns#ExampleNestedTemplate> [ xsd:string ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ rdfs:Literal ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.string)


def test_nested_template_more_specific_but_is_IRI():
    df = pl.DataFrame({"MyValue": ["hello"]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<http://example.net/ns#ExampleNestedTemplate> [ ottr:IRI ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ rdfs:Literal ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    with pytest.raises(Exception):
        m = Model(templates)
        m.map("http://example.net/ns#ExampleTemplate", df)


def test_nested_template_more_general_but_is_IRI():
    df = pl.DataFrame({"MyValue": ["http://example.net/ns#MyValue"]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ owl:Class ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ xsd:string ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    with pytest.raises(Exception):
        m = Model(templates)
        m.map("http://example.net/ns#ExampleTemplate", df)


def test_nested_template_both_general_but_always_incompatible():
    df = pl.DataFrame({"MyValue": ["http://example.net/ns#MyValue"]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ owl:Class ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ rdfs:Literal ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    with pytest.raises(Exception):
        m = Model(templates)
        m.map("http://example.net/ns#ExampleTemplate", df)


def test_nested_template_both_are_general_literal_and_compatible():
    df = pl.DataFrame({"MyValue": [1]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ owl:real ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ rdfs:Literal ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_nested_template_both_are_general_literal_and_compatible_2():
    df = pl.DataFrame({"MyValue": [1]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ owl:rational ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ owl:real ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_nested_template_both_are_general_literal_and_possibly_but_not_necessarily_incompatible():
    df = pl.DataFrame({"MyValue": [1]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ owl:real ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ owl:rational ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_nested_template_both_are_general_and_possibly_but_not_necessarily_incompatible():
    df = pl.DataFrame({"MyValue": [1]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ owl:real ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ rdfs:Resource ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_nested_template_both_are_general_and_compatible_2():
    df = pl.DataFrame({"MyValue": [1]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ rdfs:Resource ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ owl:real ?MyValue ] :: {
  <http://example.net/ns#ExampleNestedTemplate>(?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_nested_template_both_are_general_and_possibly_but_not_necessarily_incompatible_list():
    df = pl.DataFrame({"MyValue": [[1]]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ owl:real ?MyValue ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValue>,?MyValue)
} . 
<http://example.net/ns#ExampleTemplate> [ List<rdfs:Resource> ?MyValue ] :: {
  cross | <http://example.net/ns#ExampleNestedTemplate>(++?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.rdf_types["c"] == RDFType.Literal(xsd.long)


def test_list_arg_to_ottr_triple_should_get_rdf_representation():
    df = pl.DataFrame({"MyValue": [[1]]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleTemplate> [ List<rdfs:Resource> ?MyValue ] :: {
  ottr:Triple(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValueList>,?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.mappings.height == 3
    assert r.rdf_types["c"] == RDFType.Multi(
        [RDFType.IRI, RDFType.BlankNode, RDFType.Literal(xsd.long)]
    )


def test_list_arg_to_ottr_triple_should_get_rdf_representation_nested():
    df = pl.DataFrame({"MyValue": [[1, 2]]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleNestedTemplate> [ ?a, ?b, rdfs:Resource ?c ] :: {
  ottr:Triple(?a, ?b, ?c)
} . 
<http://example.net/ns#ExampleTemplate> [ List<rdfs:Resource> ?MyValue ] :: {
    <http://example.net/ns#ExampleNestedTemplate>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValueList>,?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.mappings.height == 5
    assert r.rdf_types["c"] == RDFType.Multi(
        [RDFType.IRI, RDFType.BlankNode, RDFType.Literal(xsd.long)]
    )


def test_list_arg_to_ottr_triple_should_get_rdf_representation_multiple_executions():
    df1 = pl.DataFrame({"MyValue": [[1, 2]]})
    df2 = pl.DataFrame({"MyValue": [[3, 4]]})
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example.net/ns#ExampleTemplate> [ List<rdfs:Resource> ?MyValue ] :: {
  ottr:Triple(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValueList>,?MyValue)
} . 
    """
    model = Model()
    model.add_template(templates)
    model.map("http://example.net/ns#ExampleTemplate", df1)
    model.map("http://example.net/ns#ExampleTemplate", df2)
    r = model.query(
        """
    SELECT ?a ?b ?c WHERE {
        ?a ?b ?c
    }
    """,
        include_datatypes=True,
    )
    assert r.mappings.height == 10
    assert r.rdf_types["c"] == RDFType.Multi(
        [RDFType.IRI, RDFType.BlankNode, RDFType.Literal(xsd.long)]
    )


def test_constant_arg_has_valid_subtype():
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
<http://example.net/ns#ExampleNestedTemplate> [ ?a, ?b, rdfs:Resource ?c ] :: {
  ottr:Triple(?a, ?b, ?c)
} . 
<http://example.net/ns#ExampleTemplate> [ ] :: {
    <http://example.net/ns#ExampleNestedTemplate>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValueList>, rdf:type)
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
    assert r.mappings.height == 1
    assert r.rdf_types["c"] == RDFType.IRI


def test_iri_datatype():
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
<http://example.net/ns#ExampleNestedTemplate> [ ?a, ?b, <http://www.w3.org/1999/02/22-rdf-syntax-ns#Resource> ?c ] :: {
  ottr:Triple(?a, ?b, ?c)
} . 
<http://example.net/ns#ExampleTemplate> [ ] :: {
    <http://example.net/ns#ExampleNestedTemplate>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValueList>, rdf:type)
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
    assert r.mappings.height == 1
    assert r.rdf_types["c"] == RDFType.IRI


@pytest.mark.skip("Missing general validation logic")
def test_constant_arg_has_invalid_subtype():
    templates = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
<http://example.net/ns#ExampleNestedTemplate> [ ?a, ?b, rdfs:Literal ?c ] :: {
  ottr:Triple(?a, ?b, ?c)
} . 
<http://example.net/ns#ExampleTemplate> [ ] :: {
    <http://example.net/ns#ExampleNestedTemplate>(<http://example.net/ns#MyObject>,<http://example.net/ns#hasValueList>, rdf:type)
} . 
    """
    with pytest.raises(Exception):
        Model(templates)
