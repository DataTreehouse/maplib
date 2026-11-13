"""= on numeric literals compares values with numeric type promotion (SPARQL 1.1 section 17.3,
op:numeric-equal), so FILTER(?v = 1) matches 1, 1.0, 1.0E0, "1"^^xsd:int etc.

FILTER(?v = constant) and FILTER(?v IN (...)) are pushed down as constraints on the datatype
and value of ?v. These tests check that the pushdowns do not exclude numerically equal values
of other numeric datatypes, by comparing with the same filter written so that it is not pushed
down (COALESCE hides the variable from the pushdown).
"""

import pytest

from maplib import Model

DATA = """
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:integer ex:value 1 .
ex:decimal ex:value 1.0 .
ex:double ex:value 1.0E0 .
ex:int ex:value "1"^^xsd:int .
ex:float ex:value "1"^^xsd:float .
ex:unsignedByte ex:value "1"^^xsd:unsignedByte .
ex:two ex:value 2 .
ex:string ex:value "1" .
ex:decimalOneAndAHalf ex:value 1.5 .
ex:doubleOneAndAHalf ex:value 1.5E0 .
ex:floatOneAndAHalf ex:value "1.5"^^xsd:float .
"""

PREFIXES = """
PREFIX ex: <http://example.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

# Every numeric representation of one, but not the string "1" or the number two.
ONE = sorted(
    f"<http://example.org/{s}>"
    for s in ["integer", "decimal", "double", "int", "float", "unsignedByte"]
)

CONSTANTS = [
    "1",
    "1.0",
    "1.0E0",
    '"1"^^xsd:int',
    '"1"^^xsd:float',
    '"1"^^xsd:unsignedByte',
]


@pytest.fixture(scope="module")
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def subjects(m: Model, filter_expression: str) -> list:
    df = m.query(
        PREFIXES + f"SELECT ?s WHERE {{ ?s ex:value ?v . FILTER({filter_expression}) }}"
    )
    return sorted(df["s"].to_list())


@pytest.mark.parametrize("constant", CONSTANTS)
def test_numeric_equal_without_pushdown(model, constant):
    # The reference: COALESCE(?v) is not a variable, so nothing is pushed down.
    assert subjects(model, f"COALESCE(?v) = {constant}") == ONE


@pytest.mark.parametrize("constant", CONSTANTS)
def test_numeric_equal_pushdown(model, constant):
    assert subjects(model, f"?v = {constant}") == ONE


@pytest.mark.parametrize("constant", CONSTANTS)
def test_numeric_equal_pushdown_reversed(model, constant):
    assert subjects(model, f"{constant} = ?v") == ONE


@pytest.mark.parametrize("constant", CONSTANTS)
def test_numeric_in_pushdown(model, constant):
    assert subjects(model, f"?v IN ({constant}, 3)") == ONE


@pytest.mark.parametrize("constant", CONSTANTS)
def test_numeric_same_term_is_exact(model, constant):
    # sameTerm is RDF term equality, so only the value with the same datatype matches.
    matching = subjects(model, f"sameTerm(?v, {constant})")
    assert len(matching) <= 1


# 1.5 is exact in binary, so it is the same value as a decimal, double and float.
ONE_AND_A_HALF = sorted(
    f"<http://example.org/{s}>"
    for s in ["decimalOneAndAHalf", "doubleOneAndAHalf", "floatOneAndAHalf"]
)


@pytest.mark.parametrize("constant", ["1.5", "1.5E0", '"1.5"^^xsd:float'])
@pytest.mark.parametrize(
    "template", ["COALESCE(?v) = {}", "?v = {}", "{} = ?v", "?v IN ({}, 3)"]
)
def test_numeric_equal_non_integer(model, template, constant):
    assert subjects(model, template.format(constant)) == ONE_AND_A_HALF
