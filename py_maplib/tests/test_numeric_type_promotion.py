"""Arithmetic on variables with many numeric datatypes, and the datatype of its result.

A variable bound by a triple pattern has every datatype of the objects of the predicate, so
``?l + ?r`` on a predicate used with all 16 XSD numeric datatypes is evaluated for 256 pairs of
datatypes. Combining those with the ordering of COALESCE made a plan of many GB (the DAWG
type-promotion tests used more than 3 GB within seconds). The result datatype follows SPARQL's
numeric type promotion: integer types < decimal < float < double, and any two types derived
from xsd:integer give xsd:integer.
"""

import pytest

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"
VALUES = {
    "decimal": "1",
    "float": "1",
    "double": "1",
    "integer": "1",
    "nonPositiveInteger": "-1",
    "negativeInteger": "-1",
    "long": "1",
    "int": "1",
    "short": "1",
    "byte": "1",
    "nonNegativeInteger": "1",
    "unsignedLong": "1",
    "unsignedInt": "1",
    "unsignedShort": "1",
    "unsignedByte": "1",
    "positiveInteger": "1",
}
LEVEL = {"double": 3, "float": 2, "decimal": 1}
PROMOTED = {3: "double", 2: "float", 1: "decimal", 0: "integer"}


@pytest.fixture(scope="module")
def model():
    m = Model()
    ttl = "".join(
        f'<http://ex/{t}> <http://ex/value> "{v}"^^<{XSD}{t}> .\n' for t, v in VALUES.items()
    )
    m.reads(ttl, format="turtle")
    return m


def promoted(a: str, b: str) -> str:
    return PROMOTED[max(LEVEL.get(a, 0), LEVEL.get(b, 0))]


PAIRS = [(a, b) for a in VALUES for b in ("short", "float", "decimal", "double", "integer")]


@pytest.mark.parametrize("a,b", PAIRS, ids=[f"{a}-{b}" for a, b in PAIRS])
def test_datatype_of_sum(model, a, b):
    q = f"""
    SELECT (DATATYPE(?l + ?r) AS ?dt) (?l + ?r AS ?s) WHERE {{
        <http://ex/{a}> <http://ex/value> ?l .
        <http://ex/{b}> <http://ex/value> ?r .
    }}"""
    df = model.query(q)
    assert df.height == 1
    assert df["dt"][0] == f"<{XSD}{promoted(a, b)}>"
    assert float(df["s"][0]) == float(VALUES[a]) + float(VALUES[b])


def test_all_pairs_at_once(model):
    """Every pair of datatypes in one query: 256 rows, each with the promoted datatype."""
    q = """
    SELECT ?a ?b (DATATYPE(?l * ?r) AS ?dt) WHERE {
        ?a <http://ex/value> ?l .
        ?b <http://ex/value> ?r .
    }"""
    df = model.query(q)
    assert df.height == len(VALUES) ** 2
    for a, b, dt in df.iter_rows():
        ta, tb = a.rsplit("/", 1)[1].rstrip(">"), b.rsplit("/", 1)[1].rstrip(">")
        assert dt == f"<{XSD}{promoted(ta, tb)}>", (ta, tb)


def test_short_sum_does_not_overflow():
    m = Model()
    m.reads(
        f'<http://ex/a> <http://ex/v> "30000"^^<{XSD}short> .\n', format="turtle"
    )
    df = m.query(
        "SELECT (?x + ?x AS ?s) WHERE { <http://ex/a> <http://ex/v> ?x }"
    )
    assert int(df["s"][0]) == 60000
