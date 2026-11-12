import math

import pytest

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"
FLOAT = f'"1"^^<{XSD}float>'


def divide(expression: str):
    df = Model().query(
        f"SELECT ?r (STR(?r) AS ?v) (DATATYPE(?r) AS ?t) WHERE {{ BIND({expression} AS ?r) }}"
    )
    v = df["v"][0]
    t = df["t"][0]
    return (float(v) if v is not None else None), (t.strip("<>").replace(XSD, "") if v is not None else None)


@pytest.mark.parametrize(
    "expression,value,datatype",
    [
        ("5 / 2", 2.5, "decimal"),
        ("-7 / 2", -3.5, "decimal"),
        ("4 / 2", 2, "decimal"),
        ("5.0 / 2", 2.5, "decimal"),
        ("1 / 4.0", 0.25, "decimal"),
        (f"{FLOAT} / 4", 0.25, "float"),
        (f"{FLOAT} / 4.0", 0.25, "float"),
        ("1 / 2.0e0", 0.5, "double"),
        (f"{FLOAT} / 2.0e0", 0.5, "double"),
    ],
)
def test_divide_type_promotion(expression, value, datatype):
    assert divide(expression) == (pytest.approx(value), datatype)


def test_integer_division_does_not_truncate():
    v, t = divide("1 / 3")
    assert v == pytest.approx(1 / 3)
    assert t == "decimal"


@pytest.mark.parametrize("expression", ["1 / 0", "1.0 / 0", "1 / 0.0"])
def test_decimal_division_by_zero_is_unbound(expression):
    assert divide(expression) == (None, None)


def test_double_division_by_zero_follows_ieee():
    assert divide("1.0e0 / 0") == (math.inf, "double")
    assert divide("-1.0e0 / 0") == (-math.inf, "double")
    v, t = divide("0.0e0 / 0")
    assert math.isnan(v) and t == "double"


def test_divide_on_data_with_multiple_types():
    m = Model()
    m.reads(
        """@prefix : <http://example.org/> .
        :a :v 5 . :b :v 5.0 . :c :v 5.0e0 . :d :v :iri .""",
        format="turtle",
    )
    df = m.query(
        """PREFIX : <http://example.org/>
        SELECT ?s (STR(?v / 2) AS ?r) (DATATYPE(?v / 2) AS ?t)
        WHERE { ?s :v ?v } ORDER BY ?s"""
    )
    rows = [(s.rsplit("/", 1)[-1].rstrip(">"), r, t) for s, r, t in df.iter_rows()]
    assert [(s, float(r), t) for s, r, t in rows[:3]] == [
        ("a", 2.5, f"<{XSD}decimal>"),
        ("b", 2.5, f"<{XSD}decimal>"),
        ("c", 2.5, f"<{XSD}double>"),
    ]
    # Dividing an IRI is an error
    assert rows[3][1] is None
