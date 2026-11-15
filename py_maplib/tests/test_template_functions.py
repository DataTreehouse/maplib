"""Function calls in templates.

The syntax and semantics follow Frog (Marlen Jarholt, "Frog: Functions for ontologies -- An
extension for the OTTR-framework", Master's thesis, University of Oslo, 2022,
https://hdl.handle.net/10852/96925), with SPARQL's built-in functions as base functions.
"""

import polars as pl
import pytest

from maplib import (
    Model,
    MaplibException,
    Template,
    Parameter,
    Variable,
    RDFType,
    Triple,
    FunctionCall,
    Literal,
    IRI,
    xsd,
)

pl.Config.set_fmt_str_lengths(300)

PREFIXES = """
@prefix ex: <http://example.net/ns#> .
@prefix fn: <http://ns.frog.ottr.xyz/0.1/function/> .
@prefix frog: <http://ns.frog.ottr.xyz/0.1#> .
"""


def objects(m: Model, predicate: str) -> list:
    df = m.query(
        f"""
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?o WHERE {{ ?s {predicate} ?o }} ORDER BY ?s ?o
        """
    )
    return list(zip(df["s"].to_list(), df["o"].to_list()))


def test_build_iris_from_columns():
    m = Model()
    m.add_template(
        PREFIXES
        + """
    ex:Person [ xsd:string ?id, xsd:string ?name ] :: {
        ottr:Triple((IRI (CONCAT "http://example.net/person/" (ENCODE_FOR_URI ?id))), ex:name, (UCASE ?name))
    } .
    """
    )
    m.map("ex:Person", pl.DataFrame({"id": ["a 1", "b"], "name": ["Ann", "Bob"]}))
    assert objects(m, "ex:name") == [
        ("<http://example.net/person/a%201>", 'ANN'),
        ("<http://example.net/person/b>", 'BOB'),
    ]


def test_casts_arithmetic_and_dates():
    m = Model()
    m.add_template(
        PREFIXES
        + """
    ex:Obs [ ottr:IRI ?s, xsd:string ?v, xsd:dateTime ?t ] :: {
        ottr:Triple(?s, ex:double, (* (xsd:integer ?v) 2)),
        ottr:Triple(?s, ex:year, (YEAR ?t)),
        ottr:Triple(?s, ex:big, (IF (>= (xsd:integer ?v) 10) "big" "small"))
    } .
    """
    )
    from datetime import datetime

    m.map(
        "ex:Obs",
        pl.DataFrame(
            {
                "s": ["http://example.net/o1", "http://example.net/o2"],
                "v": ["4", "21"],
                "t": [datetime(2020, 1, 1), datetime(2021, 6, 1)],
            }
        ),
    )
    df = m.query(
        """
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?d ?y ?b WHERE { ?s ex:double ?d ; ex:year ?y ; ex:big ?b } ORDER BY ?s
        """
    )
    assert df["d"].to_list() == [8, 42]
    assert df["y"].to_list() == [2020, 2021]
    assert df["b"].to_list() == ["small", "big"]


def test_frog_function_definitions():
    """Frog's def syntax, with the functions of Figure 6.5 of the thesis."""
    m = Model()
    m.add_template(
        PREFIXES
        + """
    def ex:toIRI(xsd:string ?namespace, xsd:string ?localName) -> ottr:IRI :: (
        fn:castToIRI (fn:concat ?namespace (REPLACE ?localName " " "_"))
    ) .
    def ex:toIRIWNamespace(xsd:string ?localName) -> ottr:IRI :: (
        ex:toIRI "http://example.org/data/" ?localName
    ) .
    ex:Planet [ xsd:string ?name, ? xsd:string ?starName ] :: {
        ottr:Triple((ex:toIRIWNamespace ?name), ex:orbitsStar, (ex:toIRIWNamespace ?starName))
    } .
    """
    )
    m.map(
        "ex:Planet",
        pl.DataFrame({"name": ["11 Com b", "Rogue"], "starName": ["11 Com", None]}),
    )
    assert objects(m, "ex:orbitsStar") == [
        ("<http://example.org/data/11_Com_b>", "<http://example.org/data/11_Com>"),
    ]


def test_none_discards_instance_and_defaults_are_used():
    m = Model()
    m.add_template(
        PREFIXES
        + """
    ex:Inner [ ottr:IRI ?s, xsd:string ?req, xsd:string ?def = "default" ] :: {
        ottr:Triple(?s, ex:req, ?req),
        ottr:Triple(?s, ex:def, ?def),
        ottr:Triple(?s, ex:always, "inner")
    } .
    ex:Outer [ ottr:IRI ?s, ? xsd:string ?v ] :: {
        ex:Inner(?s, (UCASE ?v), (LCASE ?v)),
        ex:Inner(?s, "x", (LCASE ?v))
    } .
    """
    )
    m.map(
        "ex:Outer",
        pl.DataFrame({"s": ["http://example.net/1", "http://example.net/2"], "v": ["Ab", None]}),
    )
    assert objects(m, "ex:req") == [
        ("<http://example.net/1>", 'AB'),
        ("<http://example.net/1>", 'x'),
        ("<http://example.net/2>", 'x'),
    ]
    assert objects(m, "ex:def") == [
        ("<http://example.net/1>", 'ab'),
        ("<http://example.net/2>", 'default'),
    ]


def test_programmatic_function_calls():
    s = Variable("s")
    name = Variable("name")
    t = Template(
        IRI("http://example.net/ns#P"),
        [Parameter(s, rdf_type=RDFType.IRI), Parameter(name, rdf_type=RDFType.Literal(xsd.string))],
        [
            Triple(
                s,
                IRI("http://example.net/ns#label"),
                FunctionCall("CONCAT", [FunctionCall("UCASE", [name]), Literal("!")]),
            ),
            Triple(
                s,
                IRI("http://example.net/ns#len"),
                FunctionCall(xsd.string, [FunctionCall("STRLEN", [name])]),
            ),
        ],
    )
    assert "(CONCAT (UCASE ?name)" in str(FunctionCall("CONCAT", [FunctionCall("UCASE", [name]), Literal("!")]))
    m = Model()
    m.add_template(t)
    m.map(t, pl.DataFrame({"s": ["http://example.net/a"], "name": ["abc"]}))
    assert objects(m, "ex:label") == [("<http://example.net/a>", 'ABC!')]
    assert objects(m, "ex:len") == [("<http://example.net/a>", '3')]


def test_unknown_function_name_is_a_parse_error():
    m = Model()
    with pytest.raises(MaplibException):
        m.add_template(
            PREFIXES
            + """
        ex:T [ ?x ] :: { ottr:Triple(ex:s, ex:p, (NOSUCHFUNCTION ?x)) } .
        """
        )
    with pytest.raises(Exception):
        FunctionCall("NOSUCHFUNCTION", [Variable("x")])


def test_wrong_arity_is_an_error():
    m = Model()
    m.add_template(
        PREFIXES
        + """
    ex:T [ xsd:string ?x ] :: { ottr:Triple(ex:s, ex:p, (STRLEN ?x ?x)) } .
    """
    )
    with pytest.raises(MaplibException):
        m.map("ex:T", pl.DataFrame({"x": ["a"]}))


def test_template_with_function_calls_round_trips_as_string():
    m = Model()
    m.add_template(
        PREFIXES
        + """
    def ex:f(xsd:string ?x) -> xsd:string :: (CONCAT ?x "!") .
    ex:T [ xsd:string ?x ] :: { ottr:Triple(ex:s, ex:p, (ex:f (UCASE ?x))) } .
    """
    )
    (t,) = m.get_templates()
    printed = str(t)
    assert "(UCASE ?x))" in printed
    m2 = Model()
    # The printed template starts with its prefix declarations.
    m2.add_template(PREFIXES + printed + """
    def ex:f(xsd:string ?x) -> xsd:string :: (CONCAT ?x "!") .
    """)
    m2.map("ex:T", pl.DataFrame({"x": ["a"]}))
    assert objects(m2, "ex:p") == [("<http://example.net/ns#s>", "A!")]
