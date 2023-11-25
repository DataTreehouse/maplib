import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Mapping

pl.Config.set_fmt_str_lengths(300)


@pytest.fixture(scope="function")
def pizzas_mapping():
    doc = """
    @prefix pizza:<https://github.com/magbak/maplib/pizza#>.
    @prefix xsd:<http://www.w3.org/2001/XMLSchema#>.
    @prefix ex:<https://github.com/magbak/maplib/pizza#>.

    ex:Pizza[?p, xsd:anyURI ?c, List<xsd:anyURI> ?is] :: {
    ottr:Triple(?p, a, pizza:Pizza),
    ottr:Triple(?p, pizza:fromCountry, ?c),
    cross | ottr:Triple(?p, pizza:hasIngredient, ++?is)
    }.
    """

    m = Mapping([doc])

    ex = "https://github.com/magbak/maplib/example#"
    co = "https://github.com/magbak/maplib/countries#"
    pi = "https://github.com/magbak/maplib/pizza#"
    ing = "https://github.com/magbak/maplib/pizza/ingredients#"

    df = pl.DataFrame({"p": [pi + "Hawaiian", pi + "Grandiosa"],
                       "c": [co + "CAN", co + "NOR"],
                       "is": [[ing + "Pineapple", ing + "Ham"],
                              [ing + "Pepper", ing + "Meat"]]})
    m.expand("ex:Pizza", df)
    hpizzas = """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
    PREFIX ing:<https://github.com/magbak/maplib/pizza/ingredients#>
    CONSTRUCT { ?p a pizza:HeterodoxPizza } 
    WHERE {
        ?p a pizza:Pizza .
        ?p pizza:hasIngredient ing:Pineapple .
    }"""
    print(m.query(hpizzas))
    m.insert(hpizzas)
    return m


def test_simple_query_no_error(pizzas_mapping):
    df = pizzas_mapping.query("""
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>

    SELECT ?p WHERE {
    ?p a pizza:HeterodoxPizza
    }
    """)

    expected_df = pl.DataFrame({"p": ["<https://github.com/magbak/maplib/pizza#Hawaiian>"]})
    assert_frame_equal(df, expected_df)
