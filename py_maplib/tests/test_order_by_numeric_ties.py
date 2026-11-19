"""ORDER BY treats equal numbers of different numeric datatypes as equal (SPARQL 1.1, 15.1: the
`<` operator, with numeric type promotion), so they are ordered by the next sort key, as
Oxigraph does. It used to order them by datatype: `"0.0"^^xsd:double` before `0`.

Each query orders the subjects completely, and the order is compared with Oxigraph's."""

import json

import pyoxigraph as ox
import pytest

from maplib import Model

EX = "http://ex.org/"
XSD = "http://www.w3.org/2001/XMLSchema#"
PFX = f"PREFIX : <{EX}>\nPREFIX xsd: <{XSD}>\n"


def lit(value, datatype):
    return f'"{value}"^^<{XSD}{datatype}>'


def triples(objects, p="o"):
    """One subject per object, named s00, s01, ... in a shuffled order, so that the subject
    order is not the object order."""
    n = len(objects)
    order = [(i * 7 + 3) % n for i in range(n)] if n % 7 else list(range(n))[::-1]
    return "".join(
        f"<{EX}s{order[i]:02d}> <{EX}{p}> {o} .\n" for i, o in enumerate(objects)
    )


def subjects_maplib(m: Model, q: str):
    j = json.loads(m.query(PFX + q, return_format="json"))
    return [row["s"]["value"] for row in j["results"]["bindings"]]


def subjects_oxigraph(store: ox.Store, q: str):
    return [sol["s"].value for sol in store.query(PFX + q)]


def check(data: str, queries):
    m = Model()
    m.reads(data, format="ntriples")
    store = ox.Store()
    store.load(data.encode(), ox.RdfFormat.N_TRIPLES)
    for q in queries:
        assert subjects_maplib(m, q) == subjects_oxigraph(store, q), q


# Equal values of several numeric datatypes, and values just apart.
TIES = [
    lit("0.0", "double"),
    lit("0", "integer"),
    lit("0.0", "double"),
    lit("-0.0", "double"),
    lit("0.0", "decimal"),
    lit("0", "float"),
    lit("1", "integer"),
    lit("1.0", "decimal"),
    lit("1.0E0", "double"),
    lit("1", "float"),
    lit("1", "int"),
    lit("1", "byte"),
    lit("1", "unsignedLong"),
    lit("1", "long"),
    lit("1", "short"),
    lit("1", "nonNegativeInteger"),
    lit("1", "positiveInteger"),
    lit("0.1", "decimal"),
    lit("0.1", "double"),
    # Not "0.1"^^xsd:float: as a float it equals the decimal 0.1, which equals the double 0.1,
    # which it does not equal as a double, so Oxigraph's order of the three is not defined.
    lit("0.5", "float"),
    lit("0.5", "decimal"),
    lit("5E-1", "double"),
    lit("-2", "integer"),
    lit("-2", "negativeInteger"),
    lit("-2", "nonPositiveInteger"),
    lit("-2.0", "double"),
    lit("10", "nonNegativeInteger"),
    lit("9", "nonNegativeInteger"),
    lit("9.5", "decimal"),
    lit("1.5", "decimal"),
    lit("1.5E0", "double"),
]


def test_equal_numbers_of_two_types_are_ordered_by_the_next_key():
    data = (
        f"<{EX}a> <{EX}o> {lit('0.0', 'double')} .\n"
        f"<{EX}b> <{EX}o> {lit('0', 'integer')} .\n"
        f"<{EX}c> <{EX}o> {lit('0.0', 'double')} .\n"
    )
    check(data, ["SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s"])


def test_numbers_of_many_datatypes_with_ties():
    check(
        triples(TIES),
        [
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s",
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o DESC(?s)",
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) ?s",
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) DESC(?s)",
        ],
    )


def test_numbers_of_many_datatypes_with_ties_and_limit():
    check(
        triples(TIES),
        [
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s LIMIT 7",
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) ?s LIMIT 7 OFFSET 3",
        ],
    )


def test_several_sort_keys_with_numeric_ties():
    # ?o ties across datatypes, ?k ties across other datatypes, ?s breaks what is left.
    keys = [lit(v, t) for v, t in [("2", "integer"), ("2.0", "double"), ("1.0", "decimal")]]
    data = triples(TIES) + "".join(
        f"<{EX}s{i:02d}> <{EX}k> {keys[i % 3]} .\n" for i in range(len(TIES))
    )
    check(
        data,
        [
            "SELECT ?s WHERE { ?s :o ?o ; :k ?k } ORDER BY ?o ?k ?s",
            "SELECT ?s WHERE { ?s :o ?o ; :k ?k } ORDER BY ?o DESC(?k) ?s",
            "SELECT ?s WHERE { ?s :o ?o ; :k ?k } ORDER BY DESC(?k) ?o DESC(?s)",
            "SELECT ?s WHERE { ?s :o ?o ; :k ?k } ORDER BY (?o + ?k) ?s",
            "SELECT ?s WHERE { ?s :o ?o ; :k ?k } ORDER BY DESC(?o * 2) ?k ?s",
        ],
    )


def test_numbers_next_to_unbound_blank_nodes_iris_and_other_literals():
    # SPARQL 1.1, 15.1: unbound, then blank nodes, then IRIs, then literals, where numbers are
    # ordered by value. Only one value of each other literal datatype, whose order relative to
    # the numbers SPARQL leaves to the implementation, so the order of the literals by
    # datatype is not tested here.
    others = [
        "_:b1",
        "_:b1",
        f"<{EX}x>",
        f"<{EX}y>",
    ]
    data = triples(TIES + others) + f"<{EX}s99> <{EX}p> <{EX}x> .\n"
    check(
        data,
        [
            "SELECT ?s WHERE { ?s :o|:p [] OPTIONAL { ?s :o ?o } } "
            "ORDER BY ?o ?s",
            "SELECT ?s WHERE { ?s :o|:p [] OPTIONAL { ?s :o ?o } } "
            "ORDER BY DESC(?o) ?s",
        ],
    )


@pytest.mark.parametrize("other", ['"x"', '"x"@en', lit("true", "boolean"),
                                    lit("2024-01-01", "date")])
def test_numbers_with_ties_next_to_another_literal(other):
    # The numbers keep their order and ties next to a non-numeric literal. Where that literal
    # goes is implementation defined, and Oxigraph, which compares it with no number, may put
    # it among the numbers and change their order. So the expected order is Oxigraph's of the
    # numbers alone, and maplib's order of the numbers is that of all the values.
    numbers = triples(TIES)
    data = numbers + f"<{EX}t> <{EX}o> {other} .\n"
    m = Model()
    m.reads(data, format="ntriples")
    store = ox.Store()
    store.load(numbers.encode(), ox.RdfFormat.N_TRIPLES)
    for q in [
        "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s",
        "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) DESC(?s)",
    ]:
        got = [s for s in subjects_maplib(m, q) if s != f"{EX}t"]
        assert got == subjects_oxigraph(store, q), q
        assert len(subjects_maplib(m, q)) == len(TIES) + 1


def test_derived_integer_types_are_ordered_by_value():
    # Stored as their lexical forms, but ordered as numbers: 9 before 10.
    data = triples([lit(v, "nonNegativeInteger") for v in ["10", "9", "100", "0", "9"]])
    check(
        data,
        [
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s",
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) ?s",
        ],
    )


def test_large_integers_next_to_decimals_and_ties():
    # Doubles can not tell 2^53 and 2^53 + 1 apart; integers and decimals are compared exactly.
    data = triples(
        [
            lit("9007199254740993", "integer"),
            lit("9007199254740992", "integer"),
            lit("9007199254740992.0", "decimal"),
            lit("9007199254740993", "long"),
            lit("0.5", "decimal"),
        ]
    )
    check(
        data,
        [
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s",
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) ?s",
        ],
    )


def test_integers_and_doubles_with_ties():
    # Without decimals, the exact order of integers is kept without decimal arithmetic.
    no_decimals = [o for o in TIES if "decimal" not in o]
    check(
        triples(no_decimals),
        [
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s",
            "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) DESC(?s)",
        ],
    )


# Integers close to 2^63 and 2^64, whose doubles are 2^63 and 2^64, next to unsigned longs and
# decimals with the same doubles. Oxigraph's integers have 64 bits with a sign, so these are
# compared with their exact order.
BIG = [
    ("9223372036854775807", "integer"),
    ("9223372036854775806", "long"),
    ("9223372036854775296", "integer"),
    ("9223372036854775808", "unsignedLong"),
    ("9223372036854775809", "unsignedLong"),
    ("9223372036854775807.5", "decimal"),
    ("9223372036854775808", "decimal"),
    ("18446744073709551615", "unsignedLong"),
    ("18446744073709551614", "unsignedLong"),
    ("18446744073709550000", "unsignedLong"),
    ("18446744073709551614.5", "decimal"),
    ("-9223372036854775808", "integer"),
    ("-9223372036854775807", "long"),
    ("-9223372036854775807.5", "decimal"),
    ("9007199254740993", "integer"),
    ("9007199254740992", "integer"),
    ("9007199254740992.5", "decimal"),
    ("9007199254740993", "decimal"),
    ("9007199254740993", "unsignedLong"),
    ("12345678901234567890123", "nonNegativeInteger"),
]


@pytest.mark.parametrize("desc", [False, True])
def test_integers_close_to_64_bits_in_exact_order(desc):
    from decimal import Decimal

    data = triples([lit(v, t) for v, t in BIG])
    m = Model()
    m.reads(data, format="ntriples")
    n = len(BIG)
    order = [(i * 7 + 3) % n for i in range(n)]
    subject = {i: f"{EX}s{order[i]:02d}" for i in range(n)}
    # Equal values (9007199254740993 three times, 9223372036854775808 twice) by subject.
    expected = sorted(
        range(n), key=lambda i: (Decimal(BIG[i][0]), subject[i]), reverse=desc
    )
    expected = [subject[i] for i in expected]
    q = (
        "SELECT ?s WHERE { ?s :o ?o } ORDER BY DESC(?o) DESC(?s)"
        if desc
        else "SELECT ?s WHERE { ?s :o ?o } ORDER BY ?o ?s"
    )
    # 12345678901234567890123 does not fit a decimal, it is ordered by its double, last.
    assert subjects_maplib(m, q) == expected
