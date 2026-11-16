"""
The FILTER of an OPTIONAL is evaluated on the merged left and right solutions (SPARQL 1.1,
section 18.6: LeftJoin), so it can use variables that only the left side binds. It used to be
evaluated on the right solutions alone, where such variables are unbound, so no match passed.
"""

import polars as pl
import pytest

from maplib import Model

PREFIX = "PREFIX ex: <http://example.org/>\n"
DATA = """
@prefix ex: <http://example.org/> .
ex:a ex:min 2 ; ex:v 1, 5, 3 .
ex:b ex:min 8 ; ex:v 10, 2 .
ex:c ex:min 0 ; ex:v 7 .
ex:d ex:min 5 .
"""


@pytest.fixture(scope="module")
def m():
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def rows(df: pl.DataFrame):
    return sorted(tuple(None if x is None else str(x) for x in r) for r in df.iter_rows())


def test_filter_uses_left_variable(m):
    df = m.query(
        PREFIX
        + """SELECT ?s ?v WHERE { ?s ex:min ?min OPTIONAL { ?s ex:v ?v FILTER(?v > ?min) } }"""
    )
    assert rows(df.select(["s", "v"])) == rows(
        pl.DataFrame(
            {
                "s": ["<http://example.org/a>"] * 2
                + ["<http://example.org/b>", "<http://example.org/c>", "<http://example.org/d>"],
                "v": [5, 3, 10, 7, None],
            }
        )
    )


def test_left_solution_without_passing_match_is_kept_once(m):
    # ex:a has three values but only some pass; a left solution where none pass is kept once.
    df = m.query(
        PREFIX
        + """SELECT ?s ?v WHERE { ?s ex:min ?min OPTIONAL { ?s ex:v ?v FILTER(?v > ?min + 100) } }"""
    )
    assert df.height == 4
    assert df["v"].null_count() == 4


def test_filter_uses_left_and_right_variables_with_duplicate_left_solutions(m):
    # The left side has duplicate solutions (one per value of ?w), which each get their matches.
    df = m.query(
        PREFIX
        + """SELECT ?s ?w ?v WHERE { ?s ex:v ?w OPTIONAL { ?s ex:v ?v FILTER(?v > ?w) } }"""
    )
    got = rows(df.select(["w", "v"]).with_columns(pl.col("v").cast(pl.Int64)))
    # a: 1 -> 5, 3; 5 -> none; 3 -> 5. b: 10 -> none; 2 -> 10. c: 7 -> none.
    expected = rows(
        pl.DataFrame(
            {"w": [1, 1, 5, 3, 10, 2, 7], "v": [5, 3, None, 5, None, 10, None]}
        )
    )
    assert got == expected
