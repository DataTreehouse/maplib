import pytest

from maplib import Model

# SPARQL 1.1 17.4.3.9 (STRBEFORE) and 17.4.3.10 (STRAFTER):
# "If there is no such occurrence, an empty simple literal is returned."


@pytest.mark.parametrize(
    "expression,expected",
    [
        ('STRAFTER("abc", "b")', "c"),
        ('STRAFTER("abc", "xyz")', ""),
        ('STRAFTER("abc", "")', "abc"),
        ('STRBEFORE("abc", "b")', "a"),
        ('STRBEFORE("abc", "xyz")', ""),
        ('STRBEFORE("abc", "")', ""),
    ],
)
def test_str_before_after_simple_literal(expression, expected):
    m = Model()
    df = m.query(f"SELECT (STR({expression}) AS ?s) WHERE {{}}")
    assert df["s"].to_list() == [expected]


@pytest.mark.parametrize(
    "expression,expected",
    [
        ('STRAFTER("abc"@en, "b")', "c"),
        ('STRAFTER("abc"@en, "z")', ""),
        ('STRBEFORE("abc"@en, "b")', "a"),
        ('STRBEFORE("abc"@en, "z")', ""),
    ],
)
def test_str_before_after_lang_string(expression, expected):
    m = Model()
    df = m.query(f"SELECT (STR({expression}) AS ?s) WHERE {{}}")
    assert df["s"].to_list() == [expected]


def test_str_after_no_match_on_data():
    m = Model()
    m.insert(
        """PREFIX ex: <http://example.org/>
        CONSTRUCT { ex:a ex:v "prefix_a" . ex:b ex:v "other_b" . } WHERE {}"""
    )
    df = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?x (STRAFTER(?v, "prefix_") AS ?s) WHERE { ?x ex:v ?v } ORDER BY ?x"""
    )
    assert df["s"].to_list() == ["a", ""]
