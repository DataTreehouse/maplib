import polars as pl
import pytest
from maplib.maplib import SolutionMappings, MaplibException
from polars.testing import assert_frame_equal

from maplib import Model, RDFType

pl.Config.set_fmt_str_lengths(300)


@pytest.fixture(scope="function")
def pizzas_model():
    doc = """
    @prefix pizza:<https://github.com/magbak/maplib/pizza#>.
    @prefix xsd:<http://www.w3.org/2001/XMLSchema#>.
    @prefix ex:<https://github.com/magbak/maplib/pizza#>.

    ex:Pizza[?p, xsd:anyURI ?c, List<ottr:IRI> ?is] :: {
    ottr:Triple(?p, a, pizza:Pizza),
    ottr:Triple(?p, pizza:fromCountry, ?c),
    cross | ottr:Triple(?p, pizza:hasIngredient, ++?is)
    }.
    """

    m = Model()
    m.add_template(doc)

    co = "https://github.com/magbak/maplib/countries#"
    pi = "https://github.com/magbak/maplib/pizza#"
    ing = "https://github.com/magbak/maplib/pizza/ingredients#"

    df = pl.DataFrame(
        {
            "p": [pi + "Hawaiian", pi + "Grandiosa"],
            "c": [co + "CAN", co + "NOR"],
            "is": [[ing + "Pineapple", ing + "Ham"], [ing + "Pepper", ing + "Meat"]],
        }
    )
    m.map("ex:Pizza", df)
    hpizzas = """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
    PREFIX ing:<https://github.com/magbak/maplib/pizza/ingredients#>
    CONSTRUCT { ?p a pizza:HeterodoxPizza } 
    WHERE {
        ?p a pizza:Pizza .
        ?p pizza:hasIngredient ing:Pineapple .
    }"""
    m.insert(hpizzas)
    return m


def test_simple_query_no_error(pizzas_model):
    res = pizzas_model.query(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>

    SELECT ?p WHERE {
    ?p a pizza:HeterodoxPizza
    }
    """
    )

    expected_df = pl.DataFrame(
        {"p": ["<https://github.com/magbak/maplib/pizza#Hawaiian>"]}
    )
    assert_frame_equal(res, expected_df)


def test_construct_pvalues(pizzas_model):
    h_df = pl.DataFrame(
        {
            "h1": [
                "https://github.com/magbak/maplib/pizza#Hawaiian1",
                "https://github.com/magbak/maplib/pizza#Hawaiian2",
            ],
            "h2": [
                "https://github.com/magbak/maplib/pizza#Hawaiian3",
                "https://github.com/magbak/maplib/pizza#Hawaiian4",
            ],
        }
    )
    h_param = SolutionMappings(h_df, {"h1": RDFType.IRI, "h2": RDFType.IRI})
    res = pizzas_model.query(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>

    CONSTRUCT {
        ?h1 a pizza:Pizza . 
        ?h2 a pizza:Pizza .
    } WHERE {
        PVALUES (?h1 ?h2) h
    }
    """,
        parameters={"h": h_param},
    )
    res0 = res[0]
    expected_dtypes = {"object": "IRI", "subject": "IRI"}
    # assert res0.rdf_datatypes == expected_dtypes
    res1 = res[1]
    # assert res1.rdf_datatypes == expected_dtypes
    assert res0.height == 2
    assert res1.height == 2


def test_construct_pvalues2(pizzas_model):
    h_df = pl.DataFrame(
        {
            "h1": [
                "https://github.com/magbak/maplib/pizza#Hawaiian",
                "https://github.com/magbak/maplib/pizza#Hawaiian2",
            ]
        }
    )
    h_sm = SolutionMappings(h_df, {"h1": RDFType.IRI})
    res = pizzas_model.query(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>

    CONSTRUCT {
        ?h1 a pizza:Pizza . 
        ?i a pizza:Pizza .
    } WHERE {
        ?h1 pizza:hasIngredient ?i .
        PVALUES (?h1) h
    }
    """,
        parameters={"h": h_sm},
    )
    res0 = res[0]
    res1 = res[1]
    assert res0.height == 1
    assert res1.height == 2


def test_having_not_so_nice(pizzas_model):
    res = pizzas_model.query(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>

    SELECT ?p (COUNT(?i) AS ?c)
    WHERE {
        ?p pizza:hasIngredient ?i .
    } 
    GROUP BY ?p
    HAVING (?c > 1)
    """
    )
    assert res.height == 2


def test_update_insert_delete(pizzas_model):
    pizzas_model.update(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
    
    DELETE  {
        ?p a pizza:Pizza .
    } INSERT {
        ?p a pizza:NewPizza . 
    }WHERE {
        ?p a pizza:Pizza .
    }
    """
    )
    df = pizzas_model.query(
        """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        SELECT ?a ?b ?c WHERE {
            ?a ?b ?c
        }
    """
    )
    assert df.height == 9
    df_filtered = df.filter(pl.col("c").str.contains(pl.lit("#NewPizza")))
    assert df_filtered.height == 2


def test_update_insert(pizzas_model):
    pizzas_model.update(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
    
    INSERT {
        ?p a pizza:NewPizza . 
    }WHERE {
        ?p a pizza:Pizza .
    }
    """
    )
    df = pizzas_model.query(
        """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        SELECT ?a ?b ?c WHERE {
            ?a ?b ?c
        }
    """
    )
    assert df.height == 11
    df_filtered_orig = df.filter(pl.col("c").str.contains(pl.lit("#Pizza")))
    assert df_filtered_orig.height == 2
    df_filtered_new = df.filter(pl.col("c").str.contains(pl.lit("#NewPizza")))
    assert df_filtered_new.height == 2


def test_update_delete(pizzas_model):
    pizzas_model.update(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
    
    DELETE  {
        ?p a pizza:Pizza .
    } WHERE {
        ?p a pizza:Pizza .
    }
    """
    )
    df = pizzas_model.query(
        """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        SELECT ?a ?b ?c WHERE {
            ?a ?b ?c
        }
    """
    )
    assert df.height == 7
    df_filtered_orig = df.filter(pl.col("c").str.contains(pl.lit("#Pizza")))
    assert df_filtered_orig.height == 0
    df_filtered_new = df.filter(pl.col("c").str.contains(pl.lit("#NewPizza")))
    assert df_filtered_new.height == 0


def test_update_insert_delete_multiple(pizzas_model):
    pizzas_model.update(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
    
    DELETE  {
        ?p a pizza:Pizza .
        ?p pizza:fromCountry ?c .
    } INSERT {
        ?p a pizza:NewPizza . 
    }WHERE {
        ?p a pizza:Pizza .
        ?p pizza:fromCountry ?c .
    }
    """
    )
    df = pizzas_model.query(
        """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        SELECT ?a ?b ?c WHERE {
            ?a ?b ?c
        }
    """
    )
    assert df.height == 7
    df_filtered = df.filter(pl.col("c").str.contains(pl.lit("#NewPizza")))
    assert df_filtered.height == 2
    df_filtered = df.filter(pl.col("b").str.contains(pl.lit("Country")))
    assert df_filtered.height == 0


def test_count_star(pizzas_model):
    count = pizzas_model.query("SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o . }")
    assert count.height == 1
    assert count.get_column("count")[0] == 9


def test_update_insert_delete_non_existent(pizzas_model):
    pizzas_model.update(
        """
    PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
    
    DELETE  {
        ?s ?p ?o .
    } WHERE {
        ?s ?p ?o .
    }
    """
    )

    df = pizzas_model.query(
        """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        SELECT ?a ?b ?c WHERE {
            ?a ?b ?c
        }
    """
    )
    assert df.height == 0


def test_select_same_subject_object(pizzas_model):
    df = pizzas_model.query(
        """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        SELECT ?a ?b WHERE {
            ?a ?b ?a
        }
    """
    )
    assert df.height == 0


def test_select_same_subject_predicate_object(pizzas_model):
    df = pizzas_model.query(
        """
        SELECT ?a WHERE {
            ?a ?a ?a 
        }
        """
    )

    assert df.height == 0


def test_select_same_predicate_object(pizzas_model):
    df = pizzas_model.query(
        """
        SELECT ?a ?b WHERE {
            ?b ?a ?a 
        }
        """
    )

    assert df.height == 0


def test_select_same_subject_predicate(pizzas_model):
    df = pizzas_model.query(
        """
        SELECT ?a ?b WHERE {
            ?a ?a ?b 
        }
        """
    )

    assert df.height == 0


# -----------

def test_select_same_subject_object_has_results(pizzas_model):
    pizzas_model.reads("""
    <https://github.com/magbak/maplib/pizza#a> <https://github.com/magbak/maplib/pizza#predicate> <https://github.com/magbak/maplib/pizza#a> .
    """, format="turtle")

    df = pizzas_model.query(
        """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        SELECT ?a ?b WHERE {
            ?a ?b ?a
        }
    """
    )
    assert df.height == 1


def test_select_same_subject_predicate_object_has_results(pizzas_model):
    pizzas_model.reads("""
    <https://github.com/magbak/maplib/pizza#a> <https://github.com/magbak/maplib/pizza#a> <https://github.com/magbak/maplib/pizza#a> .
    """, format="turtle")

    df = pizzas_model.query(
        """
        SELECT ?a WHERE {
            ?a ?a ?a 
        }
        """
    )

    assert df.height == 1


def test_select_same_predicate_object_has_results(pizzas_model):
    pizzas_model.reads("""
    <https://github.com/magbak/maplib/pizza#subject> <https://github.com/magbak/maplib/pizza#a> <https://github.com/magbak/maplib/pizza#a> .
    """, format="turtle")

    df = pizzas_model.query(
        """
        SELECT ?a ?b WHERE {
            ?b ?a ?a 
        }
        """
    )

    assert df.height == 1


def test_select_same_subject_predicate_has_results(pizzas_model):
    pizzas_model.reads("""
    <https://github.com/magbak/maplib/pizza#a> <https://github.com/magbak/maplib/pizza#a> <https://github.com/magbak/maplib/pizza#object> .
    """, format="turtle")

    df = pizzas_model.query(
        """
        SELECT ?a ?b WHERE {
            ?a ?a ?b 
        }
        """
    )

    assert df.height == 1


def test_update_with_undefined_variable(pizzas_model):
    with pytest.raises(MaplibException) as e:
        pizzas_model.update(
            """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        PREFIX fromContry:<https://github.com/magbak/maplib/pizza#fromCountry>
        
        INSERT {
            ?s pizza:NewPred ?o  . 
        }WHERE {
            ?s fromContry: ?b .
        }
        """
        )

        df = pizzas_model.query(
            """
            PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
            SELECT ?s ?o WHERE {
                ?s pizza:NewPred ?o
            }
        """
        )

    assert "Variable ?o not found" in str(e)


def test_simple_insert_construct_query(pizzas_model):
    with pytest.raises(MaplibException) as e:
        pizzas_model.insert(

            """
        PREFIX pizza:<https://github.com/magbak/maplib/pizza#>
        PREFIX ct:<https://github.com/magbak/maplib/pizza#fromCountry>
            
        CONSTRUCT {
        ?a a ct:somethingTestit.
        ?b a ct:nothingTestit. 
        } WHERE {?a a ?c}"""
        )
    assert "Construct query with undefined variable" in str(e)
