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
