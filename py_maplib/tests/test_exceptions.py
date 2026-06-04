import pytest
from maplib import Model
from maplib import MaplibException


def test_model_exception():
    m = Model()

    with pytest.raises(MaplibException) as e:
        m.reads("abc", format="turtle", graph="http://example.com/data")

def test_query_groupby_exception():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.query("""
            SELECT (COUNT(?a) as ?ca) WHERE {?a ?b ?c .} GROUP BY ?ca
        """)

def test_query_filter_exception():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.query("""
            SELECT * WHERE {
                ?a ?b ?c .
                FILTER("a")
            }
        """)

def test_query_contains_function_invalid_args_exception():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.query("""
            SELECT * WHERE {
                BIND(contains("foobar", 12) AS ?b)
            }
        """)
    assert "should resolve to an xsd:string" in str(e)

def test_query_invalid_replace_arg_exception():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.query("""
            SELECT * WHERE {
                BIND(replace("abcd", "b", 12) AS ?b)
            }
        """)
    assert "should resolve to an xsd:string" in str(e)

def test_query_invalid_substr_arg_exception():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.query("""
            SELECT * WHERE {
                BIND(substr("foobar", "s") AS ?b)
            }
        """)
    assert "SUBSTR expected" in str(e)

