from maplib import Model

# XSD 1.1 Part 2, 5.4: minimally conforming processors must support decimal values
# i / 10^k with i < 10^16 and k <= 16 (sixteen total digits).


def _roundtrip(lexical: str) -> str:
    m = Model()
    m.reads(
        f"""@prefix ex: <http://example.org/> .
        ex:a ex:v {lexical} ."""
    , format="turtle")
    nt = m.writes(format="ntriples")
    return nt.split('"')[1]


def test_decimal_with_13_fraction_digits_is_not_truncated():
    assert _roundtrip("760.3865996838672") == "760.3865996838672"


def test_decimal_with_16_fraction_digits_is_not_truncated():
    assert _roundtrip("0.0000000000000001") == "0.0000000000000001"


def test_decimal_with_large_integer_part():
    assert _roundtrip("123456789012.5") == "123456789012.5"


def test_decimal_query_value():
    m = Model()
    m.reads(
        """@prefix ex: <http://example.org/> .
        ex:a ex:v 760.3865996838672 .""", format="turtle"
    )
    df = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT (STR(?v) AS ?s) WHERE { ex:a ex:v ?v }"""
    )
    assert df["s"][0] == "760.3865996838672"


def test_decimal_str_is_canonical():
    m = Model()
    m.reads(
        """@prefix ex: <http://example.org/> .
        ex:a ex:v 1.5 . ex:b ex:v 798.0 . ex:c ex:v 0.000 . ex:d ex:v -12.340 .""",
        format="turtle",
    )
    df = m.query(
        """PREFIX ex: <http://example.org/>
        SELECT ?x (STR(?v) AS ?s) WHERE { ?x ex:v ?v } ORDER BY ?x"""
    )
    assert df["s"].to_list() == ["1.5", "798", "0", "-12.34"]


def test_large_decimals_keep_their_value():
    values = ["9999999999999.5", "99999999999999.5", "1234567890123456789.5", "-10000000.25"]
    m = Model()
    m.reads(
        "@prefix ex: <http://example.org/> .\n"
        + "\n".join(f"ex:a{i} ex:v {v} ." for i, v in enumerate(values)),
        format="turtle",
    )
    df = m.query(
        """SELECT ?s (STR(?v) AS ?lex) WHERE { ?s ?p ?v } ORDER BY ?s"""
    )
    assert df["lex"].to_list() == values
