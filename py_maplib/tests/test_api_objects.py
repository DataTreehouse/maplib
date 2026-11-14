"""Coverage for the Python-facing term and template objects:
IRI, BlankNode, Literal, Variable, Prefix, RDFType, Parameter, Argument, Instance, Template,
and the vocabulary namespaces (xsd, rdf, rdfs, owl)."""

from datetime import date, datetime

import polars as pl
import pytest

from maplib import (
    IRI,
    Argument,
    BlankNode,
    Instance,
    Literal,
    Model,
    Parameter,
    Prefix,
    RDFType,
    Template,
    Triple,
    Variable,
    a,
    owl,
    rdf,
    rdfs,
    xsd,
)

EX = "http://example.net/ns#"


def test_iri_attribute():
    iri = IRI(EX + "thing")
    assert iri.iri == EX + "thing"


def test_iri_invalid_raises():
    with pytest.raises(Exception):
        IRI("not an iri with spaces")


def test_blank_node_name():
    b = BlankNode("b0")
    assert b.name == "b0"


def test_variable_name():
    v = Variable("myVar")
    assert v.name == "myVar"


def test_prefix_suf():
    ex = Prefix(EX)
    iri = ex.suf("thing")
    assert isinstance(iri, IRI)
    assert iri.iri == EX + "thing"


def test_vocabulary_namespaces():
    assert xsd.string.iri == "http://www.w3.org/2001/XMLSchema#string"
    assert xsd.integer.iri == "http://www.w3.org/2001/XMLSchema#integer"
    assert rdf.type.iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    assert rdfs.label.iri == "http://www.w3.org/2000/01/rdf-schema#label"
    assert rdfs.subClassOf.iri == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
    assert owl.Class.iri == "http://www.w3.org/2002/07/owl#Class"
    assert a.iri == rdf.type.iri


def test_literal_plain_is_xsd_string():
    lit = Literal("abc")
    assert lit.value == "abc"
    assert lit.datatype.iri == xsd.string.iri
    assert lit.language is None


def test_literal_language_tagged():
    lit = Literal("hei", language="no")
    assert lit.value == "hei"
    assert lit.language == "no"
    assert lit.datatype.iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"


def test_literal_typed():
    lit = Literal("42", data_type=xsd.integer)
    assert lit.value == "42"
    assert lit.datatype.iri == xsd.integer.iri
    assert lit.language is None


@pytest.mark.parametrize(
    "value,datatype,expected",
    [
        ("42", xsd.integer, 42),
        ("-7", xsd.int_, -7),
        ("1.5", xsd.double, 1.5),
        ("2.25", xsd.float, 2.25),
        ("true", xsd.boolean, True),
        ("false", xsd.boolean, False),
        ("abc", xsd.string, "abc"),
        pytest.param(
            "2024-03-01",
            xsd.date,
            date(2024, 3, 1),
            marks=pytest.mark.xfail(
                strict=False,
                raises=BaseException,
                reason="Literal.to_native panics with 'not yet implemented' for xsd:date "
                "(fixed by the unwrap-handling work)",
            ),
        ),
    ],
)
def test_literal_to_native(value, datatype, expected):
    native = Literal(value, data_type=datatype).to_native()
    assert native == expected
    assert type(native) is type(expected)


def test_literal_to_native_datetime():
    native = Literal("2024-03-01T12:30:00", data_type=xsd.dateTime).to_native()
    assert isinstance(native, datetime)
    assert (native.year, native.month, native.day, native.hour, native.minute) == (
        2024,
        3,
        1,
        12,
        30,
    )


def test_rdftype_constructors():
    # These should all construct without error and be usable as parameter types.
    types = [
        RDFType.IRI,
        RDFType.BlankNode,
        RDFType.Unknown,
        RDFType.Literal(xsd.string),
        RDFType.Literal(xsd.integer.iri),
        RDFType.Nested(RDFType.IRI),
        RDFType.Multi([RDFType.IRI, RDFType.Literal(xsd.string)]),
    ]
    for t in types:
        assert t is not None


def test_parameter_attributes():
    v = Variable("x")
    p = Parameter(
        v,
        optional=True,
        allow_blank=False,
        rdf_type=RDFType.Literal(xsd.integer),
        default_value=Literal("3", data_type=xsd.integer),
    )
    assert p.variable.name == "x"
    assert p.optional is True
    assert p.allow_blank is False
    assert p.rdf_type is not None
    assert p.default_value.value == "3"


def test_parameter_defaults():
    p = Parameter(Variable("x"))
    assert p.optional is False
    assert p.allow_blank is True
    assert p.rdf_type is None
    assert p.default_value is None


def test_argument_variable():
    arg = Argument(Variable("x"))
    assert arg.variable is not None
    assert arg.variable.name == "x"
    arg2 = Argument(IRI(EX + "a"))
    assert arg2.variable is None
    # An Argument is returned as-is
    arg3 = Argument(arg)
    assert arg3.variable.name == "x"


def _simple_template():
    s = Variable("s")
    o = Variable("o")
    return Template(
        IRI(EX + "SimpleTemplate"),
        [Parameter(s, rdf_type=RDFType.IRI), o],
        [Triple(s, IRI(EX + "p"), o)],
    )


def test_template_fields():
    t = _simple_template()
    assert t.iri.iri == EX + "SimpleTemplate"
    assert [p.variable.name for p in t.parameters] == ["s", "o"]
    assert len(t.instances) == 1
    assert t.instances[0].iri == "http://ns.ottr.xyz/0.4/Triple"


def test_template_fields_are_copies_and_assignable():
    t = _simple_template()
    params = t.parameters
    params.pop()
    # Mutating the returned list does not change the template
    assert len(t.parameters) == 2

    s = Variable("s")
    o = Variable("o")
    t.instances = [Triple(s, IRI(EX + "p"), o), Triple(s, IRI(EX + "q"), o)]
    assert len(t.instances) == 2

    m = Model()
    m.add_template(t)
    m.map(
        EX + "SimpleTemplate",
        pl.DataFrame({"s": [EX + "a"], "o": ["v"]}),
    )
    assert m.query(f"SELECT ?o WHERE {{ <{EX}a> <{EX}q> ?o }}")["o"].to_list() == ["v"]


def test_template_instance_method():
    t = _simple_template()
    inst = t.instance([IRI(EX + "x"), Literal("y")])
    assert isinstance(inst, Instance)
    assert inst.iri == EX + "SimpleTemplate"

    # Use it in a new template that has no parameters
    wrapper = Template(IRI(EX + "Wrapper"), [], [inst])
    m = Model()
    m.add_template(t)
    m.add_template(wrapper)
    m.map(EX + "Wrapper")
    df = m.query(f"SELECT ?s ?o WHERE {{ ?s <{EX}p> ?o }}")
    assert df["s"].to_list() == [f"<{EX}x>"]
    assert df["o"].to_list() == ["y"]


def test_instance_with_none_argument_for_optional():
    s = Variable("s")
    o = Variable("o")
    inner = Template(
        IRI(EX + "Inner"),
        [s, Parameter(o, optional=True)],
        [Triple(s, IRI(EX + "p"), o)],
    )
    outer = Template(
        IRI(EX + "Outer"),
        [Variable("x")],
        [Instance(IRI(EX + "Inner"), [Variable("x"), None])],
    )
    m = Model()
    m.add_template(inner)
    m.add_template(outer)
    m.map(EX + "Outer", pl.DataFrame({"x": [EX + "a"]}))
    # ottr:none in an optional argument means the triple is not produced
    assert m.size() == 0


def test_template_map_by_template_object_and_iri():
    t = _simple_template()
    df = pl.DataFrame({"s": [EX + "a", EX + "b"], "o": ["1", "2"]})
    m1 = Model()
    m1.map(t, df)
    m2 = Model()
    m2.add_template(t)
    m2.map(IRI(EX + "SimpleTemplate"), df)
    assert m1.size() == m2.size() == 2


def test_get_templates_round_trip_programmatic():
    m = Model()
    m.add_template(_simple_template())
    templates = m.get_templates()
    assert [t.iri.iri for t in templates] == [EX + "SimpleTemplate"]


def test_add_template_overwrites_same_iri():
    m = Model()
    m.add_template(_simple_template())
    s = Variable("s")
    o = Variable("o")
    replacement = Template(
        IRI(EX + "SimpleTemplate"),
        [s, o],
        [Triple(s, IRI(EX + "other"), o)],
    )
    m.add_template(replacement)
    assert len(m.get_templates()) == 1
    m.map(EX + "SimpleTemplate", pl.DataFrame({"s": [EX + "a"], "o": ["v"]}))
    assert m.query(f"SELECT ?o WHERE {{ ?s <{EX}other> ?o }}").height == 1
    assert m.query(f"SELECT ?o WHERE {{ ?s <{EX}p> ?o }}").height == 0


def test_template_map_by_template_object():
    t = _simple_template()
    df = pl.DataFrame({"s": [EX + "a", EX + "b"], "o": ["1", "2"]})
    m = Model()
    m.map(t, df)
    assert m.size() == 2


def test_add_prefixes_used_in_query():
    m = Model()
    m.add_prefixes({"ex": EX})
    m.add_template(_simple_template())
    m.map(EX + "SimpleTemplate", pl.DataFrame({"s": [EX + "a"], "o": ["v"]}))
    df = m.query("SELECT ?o WHERE { ex:a ex:p ?o }")
    assert df["o"].to_list() == ["v"]


def test_add_prefixes_used_in_map():
    m = Model()
    m.add_prefixes({"ex": EX})
    m.add_template(_simple_template())
    m.map("ex:SimpleTemplate", pl.DataFrame({"s": [EX + "a"], "o": ["v"]}))
    assert m.size() == 1


def test_read_template_from_file(tmp_path):
    path = tmp_path / "tpl.stottr"
    path.write_text(
        """
@prefix ex: <http://example.net/ns#> .
@prefix ottr: <http://ns.ottr.xyz/0.4/> .
ex:FileTemplate [ ottr:IRI ?s, ?o ] :: {
    ottr:Triple(?s, ex:p, ?o)
} .
"""
    )
    m = Model()
    m.read_template(str(path))
    assert [t.iri.iri for t in m.get_templates()] == [EX + "FileTemplate"]
    m.map(EX + "FileTemplate", pl.DataFrame({"s": [EX + "a"], "o": ["v"]}))
    assert m.size() == 1
