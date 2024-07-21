import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Mapping, RDFType

pl.Config.set_fmt_str_lengths(300)


@pytest.fixture(scope="function")
def pizzas_mapping():
    doc = """
    @prefix pizza:<https://github.com/DataTreehouse/maplib/pizza#>.
    @prefix xsd:<http://www.w3.org/2001/XMLSchema#>.
    @prefix ex:<https://github.com/DataTreehouse/maplib/pizza#>.

    ex:Pizza[?p, xsd:anyURI ?c, List<ottr:IRI> ?is] :: {
    ottr:Triple(?p, a, pizza:Pizza),
    ottr:Triple(?p, pizza:fromCountry, ?c),
    cross | ottr:Triple(?p, pizza:hasIngredient, ++?is)
    }.
    """

    m = Mapping([doc])

    co = "https://github.com/DataTreehouse/maplib/countries#"
    pi = "https://github.com/DataTreehouse/maplib/pizza#"
    ing = "https://github.com/DataTreehouse/maplib/pizza/ingredients#"

    df = pl.DataFrame({"p": [pi + "Hawaiian", pi + "Grandiosa"],
                       "c": [co + "CAN", co + "NOR"],
                       "is": [[ing + "Pineapple", ing + "Ham"],
                              [ing + "Pepper", ing + "Meat"]]})
    m.expand("ex:Pizza", df)
    hpizzas = """
    PREFIX pizza:<https://github.com/DataTreehouse/maplib/pizza#>
    PREFIX ing:<https://github.com/DataTreehouse/maplib/pizza/ingredients#>
    CONSTRUCT { ?p a pizza:HeterodoxPizza } 
    WHERE {
        ?p a pizza:Pizza .
        ?p pizza:hasIngredient ing:Pineapple .
    }"""
    m.insert(hpizzas)
    return m


def test_simple_query_no_error(pizzas_mapping):
    res = pizzas_mapping.query("""
    PREFIX pizza:<https://github.com/DataTreehouse/maplib/pizza#>

    SELECT ?p WHERE {
    ?p a pizza:HeterodoxPizza
    }
    """)

    expected_df = pl.DataFrame({"p": ["<https://github.com/DataTreehouse/maplib/pizza#Hawaiian>"]})
    assert_frame_equal(res, expected_df)


def test_construct_pvalues(pizzas_mapping):
    h_df = pl.DataFrame({"h1": ["https://github.com/DataTreehouse/maplib/pizza#Hawaiian1", "https://github.com/DataTreehouse/maplib/pizza#Hawaiian2"],
                         "h2": ["https://github.com/DataTreehouse/maplib/pizza#Hawaiian3", "https://github.com/DataTreehouse/maplib/pizza#Hawaiian4"]})

    res = pizzas_mapping.query("""
    PREFIX pizza:<https://github.com/DataTreehouse/maplib/pizza#>

    CONSTRUCT {
        ?h1 a pizza:Pizza . 
        ?h2 a pizza:Pizza .
    } WHERE {
        PVALUES (?h1 ?h2) h
    }
    """, parameters={"h":(h_df, {"h1":RDFType.IRI(), "h2":RDFType.IRI()})})
    res0 = res[0]
    expected_dtypes = {'object': 'IRI', 'subject': 'IRI'}
    #assert res0.rdf_datatypes == expected_dtypes
    res1 = res[1]
    #assert res1.rdf_datatypes == expected_dtypes
    assert res0.height == 2
    assert res1.height == 2


def test_construct_pvalues2(pizzas_mapping):
    h_df = pl.DataFrame({"h1": ["https://github.com/DataTreehouse/maplib/pizza#Hawaiian", "https://github.com/DataTreehouse/maplib/pizza#Hawaiian2"]})
    res = pizzas_mapping.query("""
    PREFIX pizza:<https://github.com/DataTreehouse/maplib/pizza#>

    CONSTRUCT {
        ?h1 a pizza:Pizza . 
        ?i a pizza:Pizza .
    } WHERE {
        ?h1 pizza:hasIngredient ?i .
        PVALUES (?h1) h
    }
    """, parameters={"h":(h_df,{"h1":RDFType.IRI()})})
    res0 = res[0]
    expected_dtypes = {'object': 'IRI', 'subject': 'IRI'}
    #assert res0.rdf_datatypes == expected_dtypes
    res1 = res[1]
    #assert res1.rdf_datatypes == expected_dtypes
    assert res0.height == 1
    assert res1.height == 2
