from pyparsing import col

from maplib import Model
import polars as pl


def test_count_zero():
    m = Model()
    df = m.query("""
    SELECT (COUNT(?x) AS ?c) WHERE {
        VALUES ?x {} 
    } 
    """)

    assert df.get_column("c").to_list() == [0]
