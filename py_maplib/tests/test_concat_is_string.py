import polars as pl
import pytest
from polars.testing import assert_frame_equal

from maplib import Model, MaplibException
def test_concat_function_lang_strings():
    m = Model()
    df = m.query("""
    SELECT * WHERE {
        BIND(concat("foo"@en, "ba"@es, "r"@nb) AS ?concatstr)
    }
    """)
    expected_df = pl.from_repr(
        """
        ┌─────────────┐
        │ concatstr   │
        │ ---         │
        │ str         │
        ╞═════════════╡
        │ "foobar"@en │
        └─────────────┘
    """
    )
    assert_frame_equal(df, expected_df)
    print(df)

def test_concat_function_regular_strings():
    m = Model()
    df = m.query("""
    SELECT * WHERE {
        BIND(concat("foo", "ba", "r") AS ?concatstr)
    }
    """)
    expected_df = pl.from_repr(
        """
        ┌───────────┐
        │ concatstr │
        │ ---       │
        │ str       │
        ╞═══════════╡
        │ foobar    │
        └───────────┘
    """
    )
    assert_frame_equal(df, expected_df)
    print(df)

def test_concat_function_one_of_each():
    m = Model()
    with pytest.raises(MaplibException) as e:
        m.query("""
        SELECT * WHERE {
            BIND(concat("bbb", "bar"@en, 1) AS ?concatstr)
        }
        """)
    assert "Expected string arguments" in str(e)