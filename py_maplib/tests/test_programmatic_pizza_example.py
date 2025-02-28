import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import BlankNode

from maplib import (
    Mapping,
    Prefix,
    Template,
    Argument,
    Parameter,
    Variable,
    RDFType,
    Triple,
    a,
)

pl.Config.set_fmt_str_lengths(200)


@pytest.fixture(scope="function")
def template() -> Template:
    pi = "https://github.com/DataTreehouse/maplib/pizza#"
    pi = Prefix("pi", pi)

    p_var = Variable("p")
    c_var = Variable("c")
    ings_var = Variable("ings")

    template = Template(
        iri=pi.suf("PizzaTemplate"),
        parameters=[
            Parameter(variable=p_var, rdf_type=RDFType.IRI()),
            Parameter(variable=c_var, rdf_type=RDFType.IRI()),
            Parameter(variable=ings_var, rdf_type=RDFType.Nested(RDFType.IRI())),
        ],
        instances=[
            Triple(p_var, a(), pi.suf("Pizza")),
            Triple(p_var, pi.suf("fromCountry"), c_var),
            Triple(p_var, pi.suf("hasBlank"), BlankNode("MyBlank")),
            Triple(
                p_var,
                pi.suf("hasIngredient"),
                Argument(term=ings_var, list_expand=True),
                list_expander="cross",
            ),
        ],
    )
    return template


@pytest.fixture(scope="function")
def pizzas_mapping(template: Template):

    pi = "https://github.com/DataTreehouse/maplib/pizza#"
    df = pl.DataFrame(
        {
            "p": [pi + "Hawaiian", pi + "Grandiosa"],
            "c": [pi + "CAN", pi + "NOR"],
            "ings": [[pi + "Pineapple", pi + "Ham"], [pi + "Pepper", pi + "Meat"]],
        }
    )
    # print(df)

    m = Mapping()
    m.expand(template, df)
    hpizzas = """
    PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>
    CONSTRUCT { ?p a pi:HeterodoxPizza } 
    WHERE {
        ?p a pi:Pizza .
        ?p pi:hasIngredient pi:Pineapple .
    }"""
    m.insert(hpizzas)
    return m


def test_simple_query_no_error(pizzas_mapping):
    res = pizzas_mapping.query(
        """
    PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>

    SELECT ?p WHERE {
    ?p a pi:HeterodoxPizza
    }
    """
    )

    expected_df = pl.DataFrame(
        {"p": ["<https://github.com/DataTreehouse/maplib/pizza#Hawaiian>"]}
    )
    assert_frame_equal(res, expected_df)


def test_insert_new_thing(pizzas_mapping):
    hpizzas = """
    PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>
    CONSTRUCT { ?p a pi:HeterodoxPizza2 } 
    WHERE {
        ?p a pi:Pizza .
        ?p pi:hasIngredient pi:Pineapple .
    }"""
    res1 = pizzas_mapping.insert(hpizzas, delay_index=False)
    assert isinstance(res1, dict)
    assert len(res1) == 1
    assert res1["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"].shape == (1, 2)
    res2 = pizzas_mapping.insert(hpizzas, delay_index=False)
    assert len(res2) == 0


def test_insert_new_things(pizzas_mapping):
    hpizzas = """
    PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>
    CONSTRUCT { 
        ?p a pi:HeterodoxPizza2 .
        ?p pi:abc pi:123 .
     } 
    WHERE {
        ?p a pi:Pizza .
    }"""
    res1 = pizzas_mapping.insert(hpizzas, delay_index=False)
    assert isinstance(res1, dict)
    assert len(res1) == 2
    assert res1["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"].shape == (2, 2)
    assert res1["https://github.com/DataTreehouse/maplib/pizza#abc"].shape == (2, 2)
    res2 = pizzas_mapping.insert(hpizzas, delay_index=False)
    assert len(res2) == 0


def test_print_template(template: Template):
    s = str(template)
    assert (
        s
        == """<https://github.com/DataTreehouse/maplib/pizza#PizzaTemplate> [ <http://ns.ottr.xyz/0.4/IRI> ?p,  <http://ns.ottr.xyz/0.4/IRI> ?c,  List<<http://ns.ottr.xyz/0.4/IRI>> ?ings ] :: {
  <http://ns.ottr.xyz/0.4/Triple>(?p,<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>,<https://github.com/DataTreehouse/maplib/pizza#Pizza>) ,
  <http://ns.ottr.xyz/0.4/Triple>(?p,<https://github.com/DataTreehouse/maplib/pizza#fromCountry>,?c) ,
  <http://ns.ottr.xyz/0.4/Triple>(?p,<https://github.com/DataTreehouse/maplib/pizza#hasBlank>,_:MyBlank) ,
  cross | <http://ns.ottr.xyz/0.4/Triple>(?p,<https://github.com/DataTreehouse/maplib/pizza#hasIngredient>,++ ?ings)
} . 
"""
    )
