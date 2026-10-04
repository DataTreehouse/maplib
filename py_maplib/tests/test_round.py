import pytest

from maplib import Model

# SPARQL 1.1 17.4.4.2: "Returns the number with no fractional part that is closest to the
# argument. If there are two such numbers, then the one that is closest to positive
# infinity is returned." Examples: round(2.5) = 3.0, round(-2.5) = -2.0, round(2.4999) = 2.0


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("ROUND(2.5)", 3),
        ("ROUND(-2.5)", -2),
        ("ROUND(2.4999)", 2),
        ("ROUND(-0.5)", 0),
        ("ROUND(-36.5)", -36),
        ("ROUND(-36.6)", -37),
        ("ROUND(-36.4)", -36),
        ("ROUND(2.5e0)", 3),
        ("ROUND(-2.5e0)", -2),
        ("ROUND(-2.6e0)", -3),
        ("ROUND(-3)", -3),
        ("ROUND(4)", 4),
    ],
)
def test_round_ties_towards_positive_infinity(expression, expected):
    m = Model()
    df = m.query(f"SELECT ({expression} AS ?r) WHERE {{}}")
    assert float(df["r"][0]) == expected


def test_round_on_data():
    m = Model()
    m.insert(
        """PREFIX ex: <http://example.org/>
        CONSTRUCT { ex:a ex:v -36.5 . ex:b ex:v 36.5 . ex:c ex:v -36.51 . } WHERE {}"""
    )
    df = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?x (ROUND(?v) AS ?r) WHERE { ?x ex:v ?v } ORDER BY ?x"""
    )
    assert [float(x) for x in df["r"].to_list()] == [-36, 37, -37]
