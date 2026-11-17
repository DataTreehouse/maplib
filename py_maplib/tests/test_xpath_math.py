"""Compliance tests of the XPath math functions (XPath and XQuery Functions and Operators 3.1,
section 4.8, https://www.w3.org/TR/xpath-functions-31/#trigonometry): every example of the
specification, with the special cases of -0, INF and NaN, and the rules on arguments (numeric
types cast to xs:double, empty or non-numeric arguments give no value)."""

import math

import polars as pl
import pytest

from maplib import Model

PREFIX = "PREFIX math: <http://www.w3.org/2005/xpath-functions/math#>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"

NAN = float("nan")
INF = float("inf")
PI = math.pi

# The specification's xs:double('NaN'), xs:double('INF') and xs:double('-INF').
NaN = 'xsd:double("NaN")'
Inf = 'xsd:double("INF")'
NegInf = 'xsd:double("-INF")'

# (expression, expected, exact): exact compares bit for bit (also the sign of 0); otherwise the
# specification says "approximately" and the value is compared to 1e-15 relative or absolute.
EXAMPLES = [
    # 4.8.1 math:pi
    ("math:pi()", 3.141592653589793, True),
    ("60 * (math:pi() / 180)", 1.0471975511965976, False),
    # 4.8.2 math:exp
    ("math:exp(0)", 1.0, True),
    ("math:exp(1)", 2.7182818284590455, False),
    ("math:exp(2)", 7.38905609893065, False),
    ("math:exp(-1)", 0.36787944117144233, False),
    ("math:exp(math:pi())", 23.140692632779267, False),
    (f"math:exp({NaN})", NAN, True),
    (f"math:exp({Inf})", INF, True),
    (f"math:exp({NegInf})", 0.0, True),
    # 4.8.3 math:exp10
    ("math:exp10(0)", 1.0, True),
    ("math:exp10(1)", 10.0, True),
    ("math:exp10(0.5)", 3.1622776601683795, False),
    ("math:exp10(-1)", 0.1, False),
    (f"math:exp10({NaN})", NAN, True),
    (f"math:exp10({Inf})", INF, True),
    (f"math:exp10({NegInf})", 0.0, True),
    # 4.8.4 math:log
    ("math:log(0)", -INF, True),
    ("math:log(math:exp(1))", 1.0, False),
    ("math:log(1.0e-3)", -6.907755278982137, False),
    ("math:log(2)", 0.6931471805599453, False),
    ("math:log(-1)", NAN, True),
    (f"math:log({NaN})", NAN, True),
    (f"math:log({Inf})", INF, True),
    (f"math:log({NegInf})", NAN, True),
    # 4.8.5 math:log10
    ("math:log10(0)", -INF, True),
    ("math:log10(1.0e3)", 3.0, True),
    ("math:log10(1.0e-3)", -3.0, True),
    ("math:log10(2)", 0.3010299956639812, False),
    ("math:log10(-1)", NAN, True),
    (f"math:log10({NaN})", NAN, True),
    (f"math:log10({Inf})", INF, True),
    (f"math:log10({NegInf})", NAN, True),
    # 4.8.6 math:sqrt
    ("math:sqrt(0.0e0)", 0.0, True),
    ("math:sqrt(-0.0e0)", -0.0, True),
    ("math:sqrt(1.0e6)", 1.0e3, True),
    ("math:sqrt(2.0e0)", 1.4142135623730951, True),
    ("math:sqrt(-2.0e0)", NAN, True),
    (f"math:sqrt({NaN})", NAN, True),
    (f"math:sqrt({Inf})", INF, True),
    (f"math:sqrt({NegInf})", NAN, True),
    # 4.8.7 math:pow
    ("math:pow(2, 3)", 8.0, True),
    ("math:pow(-2, 3)", -8.0, True),
    ("math:pow(2, -3)", 0.125, True),
    ("math:pow(-2, -3)", -0.125, True),
    ("math:pow(2, 0)", 1.0, True),
    ("math:pow(0, 0)", 1.0, True),
    (f"math:pow({Inf}, 0)", 1.0, True),
    (f"math:pow({NaN}, 0)", 1.0, True),
    ("math:pow(-math:pi(), 0)", 1.0, True),
    ("math:pow(0e0, 3)", 0.0, True),
    ("math:pow(0e0, 4)", 0.0, True),
    ("math:pow(-0e0, 3)", -0.0, True),
    ("math:pow(0, 4)", 0.0, True),
    ("math:pow(0e0, -3)", INF, True),
    ("math:pow(0e0, -4)", INF, True),
    ("math:pow(-0e0, -3)", -INF, True),
    ("math:pow(0, -4)", INF, True),
    ("math:pow(16, 0.5e0)", 4.0, True),
    ("math:pow(16, 0.25e0)", 2.0, True),
    ("math:pow(0e0, -3.0e0)", INF, True),
    ("math:pow(-0e0, -3.0e0)", -INF, True),
    ("math:pow(0e0, -3.1e0)", INF, True),
    ("math:pow(-0e0, -3.1e0)", INF, True),
    ("math:pow(0e0, 3.0e0)", 0.0, True),
    ("math:pow(-0e0, 3.0e0)", -0.0, True),
    ("math:pow(0e0, 3.1e0)", 0.0, True),
    ("math:pow(-0e0, 3.1e0)", 0.0, True),
    (f"math:pow(-1, {Inf})", 1.0, True),
    (f"math:pow(-1, {NegInf})", 1.0, True),
    (f"math:pow(1, {Inf})", 1.0, True),
    (f"math:pow(1, {NegInf})", 1.0, True),
    (f"math:pow(1, {NaN})", 1.0, True),
    ("math:pow(-2.5e0, 2.0e0)", 6.25, True),
    ("math:pow(-2.5e0, 2.00000001e0)", NAN, True),
    # 4.8.8 math:sin
    ("math:sin(0)", 0.0, True),
    ("math:sin(-0.0e0)", -0.0, True),
    ("math:sin(math:pi() / 2)", 1.0, False),
    ("math:sin(-math:pi() / 2)", -1.0, False),
    ("math:sin(math:pi())", 0.0, False),
    (f"math:sin({NaN})", NAN, True),
    (f"math:sin({Inf})", NAN, True),
    (f"math:sin({NegInf})", NAN, True),
    # 4.8.9 math:cos
    ("math:cos(0)", 1.0, True),
    ("math:cos(-0.0e0)", 1.0, True),
    ("math:cos(math:pi() / 2)", 0.0, False),
    ("math:cos(-math:pi() / 2)", 0.0, False),
    ("math:cos(math:pi())", -1.0, False),
    (f"math:cos({NaN})", NAN, True),
    (f"math:cos({Inf})", NAN, True),
    (f"math:cos({NegInf})", NAN, True),
    # 4.8.10 math:tan
    ("math:tan(0)", 0.0, True),
    ("math:tan(-0.0e0)", -0.0, True),
    ("math:tan(math:pi() / 4)", 1.0, False),
    ("math:tan(-math:pi() / 4)", -1.0, False),
    ("1 / math:tan(math:pi() / 2)", 0.0, False),
    ("1 / math:tan(-math:pi() / 2)", 0.0, False),
    ("math:tan(math:pi())", 0.0, False),
    (f"math:tan({NaN})", NAN, True),
    (f"math:tan({Inf})", NAN, True),
    (f"math:tan({NegInf})", NAN, True),
    # 4.8.11 math:asin
    ("math:asin(0)", 0.0, True),
    ("math:asin(-0.0e0)", -0.0, True),
    ("math:asin(1.0e0)", 1.5707963267948966, True),
    ("math:asin(-1.0e0)", -1.5707963267948966, True),
    ("math:asin(2.0e0)", NAN, True),
    (f"math:asin({NaN})", NAN, True),
    (f"math:asin({Inf})", NAN, True),
    (f"math:asin({NegInf})", NAN, True),
    # 4.8.12 math:acos
    ("math:acos(0)", 1.5707963267948966, True),
    ("math:acos(-0.0e0)", 1.5707963267948966, True),
    ("math:acos(1.0e0)", 0.0, True),
    ("math:acos(-1.0e0)", 3.141592653589793, True),
    ("math:acos(2.0e0)", NAN, True),
    (f"math:acos({NaN})", NAN, True),
    (f"math:acos({Inf})", NAN, True),
    (f"math:acos({NegInf})", NAN, True),
    # 4.8.13 math:atan
    ("math:atan(0)", 0.0, True),
    ("math:atan(-0.0e0)", -0.0, True),
    ("math:atan(1.0e0)", 0.7853981633974483, True),
    ("math:atan(-1.0e0)", -0.7853981633974483, True),
    (f"math:atan({NaN})", NAN, True),
    (f"math:atan({Inf})", 1.5707963267948966, True),
    (f"math:atan({NegInf})", -1.5707963267948966, True),
    # 4.8.14 math:atan2
    ("math:atan2(+0.0e0, 0.0e0)", 0.0, True),
    ("math:atan2(-0.0e0, 0.0e0)", -0.0, True),
    ("math:atan2(+0.0e0, -0.0e0)", PI, True),
    ("math:atan2(-0.0e0, -0.0e0)", -PI, True),
    ("math:atan2(-1, 0.0e0)", -1.5707963267948966, True),
    ("math:atan2(+1, 0.0e0)", 1.5707963267948966, True),
    ("math:atan2(-0.0e0, -1)", -PI, True),
    ("math:atan2(+0.0e0, -1)", PI, True),
    ("math:atan2(-0.0e0, +1)", -0.0, True),
    ("math:atan2(+0.0e0, +1)", 0.0, True),
]


def evaluate(expression: str):
    df = Model().query(PREFIX + f"SELECT ?r WHERE {{ BIND({expression} AS ?r) }}")
    assert df.height == 1
    return df.get_column("r")[0]


def same(got: float, expected: float, exact: bool) -> bool:
    if math.isnan(expected):
        return math.isnan(got)
    if exact:
        return got == expected and math.copysign(1.0, got) == math.copysign(1.0, expected)
    return math.isclose(got, expected, rel_tol=1e-15, abs_tol=1e-15)


@pytest.mark.parametrize("expression,expected,exact", EXAMPLES, ids=[e[0] for e in EXAMPLES])
def test_specification_examples(expression, expected, exact):
    got = evaluate(expression)
    assert isinstance(got, float), f"{expression} gave {got!r}, not an xs:double"
    assert same(got, expected, exact), f"{expression} gave {got!r}, expected {expected!r}"


def test_result_is_xsd_double():
    m = Model()
    df = m.query(PREFIX + "SELECT (DATATYPE(math:sqrt(4)) AS ?t) WHERE {}")
    assert df.get_column("t")[0] == "<http://www.w3.org/2001/XMLSchema#double>"


@pytest.mark.parametrize("literal", ['"4"^^xsd:integer', '"4.0"^^xsd:decimal', '"4.0"^^xsd:float', '"4.0e0"^^xsd:double', '"4"^^xsd:int', '"4"^^xsd:unsignedByte'])
def test_numeric_arguments_are_cast_to_double(literal):
    assert evaluate(f"math:sqrt({literal})") == 2.0


def test_empty_argument_gives_no_value():
    """math:pow((), 93.7) is (): an unbound argument leaves the result unbound."""
    m = Model()
    m.reads("<urn:a> <urn:p> <urn:b> .", format="ntriples")
    df = m.query(PREFIX + "SELECT ?r WHERE { ?a <urn:p> ?b OPTIONAL { ?a <urn:missing> ?x } BIND(math:pow(?x, 93.7) AS ?r) }")
    assert df.get_column("r").to_list() == [None]


def test_non_numeric_argument_gives_no_value():
    """A string is not converted to xs:double (a type error), so the expression has no value."""
    assert evaluate('math:sqrt("4")') is None
    assert evaluate("math:sqrt(<urn:a>)") is None


def test_wrong_number_of_arguments_is_an_error():
    with pytest.raises(Exception):
        Model().query(PREFIX + "SELECT ?r WHERE { BIND(math:sqrt(1, 2) AS ?r) }")
    with pytest.raises(Exception):
        Model().query(PREFIX + "SELECT ?r WHERE { BIND(math:pow(2) AS ?r) }")


def test_columns_of_data_and_mixed_types():
    """Applied per row, over a column of several numeric types and a string."""
    m = Model()
    m.reads(
        """<urn:a> <urn:v> "9"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:b> <urn:v> "2.25"^^<http://www.w3.org/2001/XMLSchema#decimal> .
<urn:c> <urn:v> "16.0e0"^^<http://www.w3.org/2001/XMLSchema#double> .
<urn:d> <urn:v> "text" .
""",
        format="ntriples",
    )
    df = m.query(PREFIX + "SELECT ?s ?r ?p WHERE { ?s <urn:v> ?v BIND(math:sqrt(?v) AS ?r) BIND(math:pow(?v, 2) AS ?p) } ORDER BY ?s")
    assert df.get_column("r").to_list() == [3.0, 1.5, 4.0, None]
    assert df.get_column("p").to_list() == [81.0, 5.0625, 256.0, None]


def test_apparent_power_from_active_and_reactive_power():
    """The use that asked for it: S = √(P² + Q²) per row."""
    m = Model()
    m.reads(
        """<urn:t1> <urn:p> "3.0e0"^^<http://www.w3.org/2001/XMLSchema#double> .
<urn:t1> <urn:q> "4.0e0"^^<http://www.w3.org/2001/XMLSchema#double> .
<urn:t2> <urn:p> "5"^^<http://www.w3.org/2001/XMLSchema#integer> .
<urn:t2> <urn:q> "12"^^<http://www.w3.org/2001/XMLSchema#integer> .
""",
        format="ntriples",
    )
    df = m.query(PREFIX + "SELECT ?t ?s WHERE { ?t <urn:p> ?p ; <urn:q> ?q BIND(math:sqrt(?p * ?p + ?q * ?q) AS ?s) } ORDER BY ?t")
    assert df.get_column("s").to_list() == [5.0, 13.0]
    assert df.schema["s"] == pl.Float64


@pytest.mark.parametrize("expression", ["-0.0e0", "-(0.0e0)", '-"0.0"^^xsd:float'])
def test_unary_minus_of_a_double_zero_is_negative_zero(expression):
    """op:numeric-unary-minus negates (IEEE 754), so the double zero becomes -0, as math:sqrt
    and the other functions above need for their -0 examples."""
    got = evaluate(expression)
    assert got == 0.0 and math.copysign(1.0, got) == -1.0


def test_unary_minus_of_other_numbers():
    assert evaluate("-(3)") == -3
    assert evaluate("-(2.5)") == -2.5
    assert evaluate("-(-1.5e0)") == 1.5
