import polars as pl
import pytest
from polars.testing import assert_frame_equal
pl.Config.set_fmt_str_lengths(300)


@pytest.fixture(scope="function")
def pizzas_mapping():
    from maplib import Mapping, Prefix, Template, Argument, Parameter, Variable, RDFType, triple, a
    import polars as pl
    pl.Config.set_fmt_str_lengths(150)

    pi = "https://github.com/DataTreehouse/maplib/pizza#"
    df = pl.DataFrame({
        "p":[pi + "Hawaiian", pi + "Grandiosa"],
        "c":[pi + "CAN", pi + "NOR"],
        "ings": [[pi + "Pineapple", pi + "Ham"],
                 [pi + "Pepper", pi + "Meat"]]
    })
    #print(df)

    pi = Prefix("pi", pi)

    p_var = Variable("p")
    c_var = Variable("c")
    ings_var = Variable("ings")

    template = Template(
        iri= pi.suf("PizzaTemplate"),
        parameters= [
            Parameter(variable=p_var, rdf_type=RDFType.IRI()),
            Parameter(variable=c_var, rdf_type=RDFType.IRI()),
            Parameter(variable=ings_var, rdf_type=RDFType.Nested(RDFType.IRI()))
        ],
        instances= [
            triple(p_var, a(), pi.suf("Pizza")),
            triple(p_var, pi.suf("fromCountry"), c_var),
            triple(p_var, pi.suf("hasIngredient"), Argument(term=ings_var, list_expand=True), list_expander="cross")
        ]
    )

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
    res = pizzas_mapping.query("""
    PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>

    SELECT ?p WHERE {
    ?p a pi:HeterodoxPizza
    }
    """)

    expected_df = pl.DataFrame({"p": ["<https://github.com/DataTreehouse/maplib/pizza#Hawaiian>"]})
    assert_frame_equal(res, expected_df)