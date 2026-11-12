import json

import pytest

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"

DATA = """
@prefix : <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
:s1 :p1 :s2 .
:s2 :p2 "foo" .
:a :num -2, 1.5, "-3.5"^^xsd:double, "x" .
:a :str "1", "22" .
:b :lang "1"@en, "2"@fr .
:c :mixed 1, _:b1 .
"""


@pytest.fixture(scope="module")
def m() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def q(query: str) -> str:
    return "PREFIX : <http://example.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " + query


def bindings(m: Model, query: str) -> list:
    return json.loads(m.query(q(query), return_format="json"))["results"]["bindings"]


@pytest.mark.parametrize(
    "query,expected",
    [
        ("ASK { :s1 :p1 :s2 }", True),
        ("ASK { :s1 :p1 :s3 }", False),
        ("ASK { :s1 :p1 ?o }", True),
        ("ASK { ?s :missing ?o }", False),
        ("ASK { :s1 :p1 :s2 . :s2 :p2 \"foo\" }", True),
        ("ASK { :s1 :p1 :s2 . :s2 :p2 \"bar\" }", False),
        ("ASK { :s1 :p1 ?x . ?x :p2 ?y }", True),
        ("ASK {}", True),
        ("ASK { FILTER(false) }", False),
        ("ASK { FILTER(2 IN (1, 2, 3)) }", True),
    ],
)
def test_ask(m, query, expected):
    assert m.query(q(query)) is expected


@pytest.mark.parametrize("expected", [True, False])
def test_ask_json(m, expected):
    query = "ASK { :s1 :p1 :s2 }" if expected else "ASK { :s1 :p1 :s3 }"
    assert json.loads(m.query(q(query), return_format="json")) == {
        "head": {},
        "boolean": expected,
    }


def test_ground_triple_pattern_in_select(m):
    assert bindings(m, "SELECT (1 AS ?x) WHERE { :s1 :p1 :s2 }") == [
        {"x": {"type": "literal", "value": "1", "datatype": XSD + "integer"}}
    ]
    assert bindings(m, "SELECT (1 AS ?x) WHERE { :s1 :p1 :s3 }") == []
    rows = bindings(m, "SELECT ?y WHERE { :s1 :p1 :s2 . :s2 :p2 ?y }")
    assert [r["y"]["value"] for r in rows] == ["foo"]


def test_isnumeric(m):
    rows = bindings(
        m, "SELECT (STR(?o) AS ?v) (isNUMERIC(?o) AS ?n) WHERE { :a :num ?o } ORDER BY ?v"
    )
    assert {r["v"]["value"]: r["n"]["value"] for r in rows} == {
        "-2": "true",
        "1.5": "true",
        "-3.5": "true",
        "x": "false",
    }
    rows = bindings(m, "SELECT (isNUMERIC(:a) AS ?n) (isNUMERIC(\"1\") AS ?s) WHERE {}")
    assert rows[0]["n"]["value"] == "false"
    assert rows[0]["s"]["value"] == "false"


def test_concat_zero_and_one_argument(m):
    rows = bindings(m, "SELECT (CONCAT() AS ?e) (CONCAT(\"a\") AS ?a) WHERE {}")
    assert rows[0]["e"] == {"type": "literal", "value": ""}
    assert rows[0]["a"] == {"type": "literal", "value": "a"}


@pytest.mark.parametrize(
    "func,expected",
    [
        ("ABS", {"-2": ("2", "integer"), "1.5": ("1.5", "decimal"), "-3.5": ("3.5", "double")}),
        ("CEIL", {"-2": ("-2", "integer"), "1.5": ("2", "decimal"), "-3.5": ("-3.0", "double")}),
        ("FLOOR", {"-2": ("-2", "integer"), "1.5": ("1", "decimal"), "-3.5": ("-4.0", "double")}),
        ("ROUND", {"-2": ("-2", "integer"), "1.5": ("2", "decimal"), "-3.5": ("-3.0", "double")}),
    ],
)
def test_numeric_functions_multiple_types(m, func, expected):
    rows = bindings(
        m, f"SELECT (STR(?o) AS ?v) ({func}(?o) AS ?r) WHERE {{ :a :num ?o }}"
    )
    actual = {}
    for r in rows:
        if "r" in r:
            actual[r["v"]["value"]] = (r["r"]["value"], r["r"]["datatype"][len(XSD):])
        else:
            # Non-numeric arguments are an error, so the result is unbound
            assert r["v"]["value"] == "x"
    assert actual == expected


@pytest.mark.parametrize(
    "value,func,expected",
    [
        ("-1.5", "CEIL", "-1"),
        ("-1.5", "FLOOR", "-2"),
        ("2.0", "CEIL", "2"),
        ("2.0", "FLOOR", "2"),
        ("0.1", "CEIL", "1"),
        ("-0.1", "FLOOR", "-1"),
    ],
)
def test_ceil_floor_decimal(m, value, func, expected):
    rows = bindings(m, f"SELECT ({func}({value}) AS ?r) WHERE {{}}")
    assert rows[0]["r"] == {"type": "literal", "value": expected, "datatype": XSD + "decimal"}


def test_group_concat_default_separator(m):
    rows = bindings(m, "SELECT (GROUP_CONCAT(?o) AS ?g) WHERE { :a :str ?o }")
    assert rows[0]["g"]["value"] in ("1 22", "22 1")
    assert "datatype" not in rows[0]["g"]


def test_group_concat_separator_and_distinct(m):
    rows = bindings(
        m,
        "SELECT (GROUP_CONCAT(DISTINCT ?o; SEPARATOR=\":\") AS ?g) "
        "WHERE { VALUES ?o { \"a\" \"b\" \"a\" } }",
    )
    assert rows[0]["g"]["value"] in ("a:b", "b:a")


def test_group_concat_lang_strings_and_multiple_types(m):
    rows = bindings(m, "SELECT (GROUP_CONCAT(?o) AS ?g) WHERE { :b :lang ?o }")
    assert rows[0]["g"] == {"type": "literal", "value": rows[0]["g"]["value"]}
    assert rows[0]["g"]["value"] in ("1 2", "2 1")
    rows = bindings(m, "SELECT (GROUP_CONCAT(?o) AS ?g) WHERE { :a :num ?o }")
    assert sorted(rows[0]["g"]["value"].split(" ")) == sorted(["-2", "1.5", "-3.5", "x"])


def test_group_concat_grouped(m):
    rows = bindings(
        m,
        "SELECT ?p (GROUP_CONCAT(?o; SEPARATOR=\",\") AS ?g) WHERE { :a ?p ?o } GROUP BY ?p",
    )
    by_p = {r["p"]["value"]: sorted(r["g"]["value"].split(",")) for r in rows}
    assert by_p["http://example.org/str"] == ["1", "22"]


def test_cast_blank_node_is_unbound(m):
    rows = bindings(
        m, "SELECT ?o (xsd:double(?o) AS ?d) WHERE { :c :mixed ?o }"
    )
    by_type = {r["o"]["type"]: r.get("d") for r in rows}
    assert by_type["bnode"] is None
    assert by_type["literal"]["value"] in ("1", "1.0", "1.0E0")


def test_rand(m):
    df = m.query("SELECT (RAND() AS ?r) WHERE { VALUES ?x { 1 2 3 4 5 6 7 8 } }")
    values = df["r"].to_list()
    assert all(0.0 <= v < 1.0 for v in values)
    assert len(set(values)) > 1
    rows = bindings(m, "SELECT (DATATYPE(RAND()) AS ?t) WHERE {}")
    assert rows[0]["t"]["value"] == XSD + "double"


def term(b):
    """A JSON binding as (value, datatype, language), or None if unbound."""
    if b is None:
        return None
    return (b["value"], b.get("datatype"), b.get("xml:lang"))


def single(m: Model, query: str, var: str = "x"):
    rows = bindings(m, query)
    assert len(rows) == 1
    return term(rows[0].get(var))


@pytest.mark.parametrize(
    "expr,expected",
    [
        # Effective boolean value (SPARQL 1.1 17.2.2) of the IF condition
        ("IF(1, \"y\", \"n\")", ("y", None, None)),
        ("IF(0, \"y\", \"n\")", ("n", None, None)),
        ("IF(0.0, \"y\", \"n\")", ("n", None, None)),
        ("IF(\"a\", \"y\", \"n\")", ("y", None, None)),
        ("IF(\"\", \"y\", \"n\")", ("n", None, None)),
        ("IF(\"a\"@en, \"y\", \"n\")", ("y", None, None)),
        # An error in the condition is an error
        ("IF(1/0, \"y\", \"n\")", None),
        ("IF(<http://example.org/x>, \"y\", \"n\")", None),
    ],
)
def test_if_effective_boolean_value(m, expr, expected):
    assert single(m, f"SELECT ({expr} AS ?x) WHERE {{}}") == expected


def test_filter_effective_boolean_value(m):
    rows = bindings(
        m,
        "SELECT ?x WHERE { VALUES ?x { 0 1 2.5 0.0 \"\" \"a\" \"b\"@en <http://example.org/x> } FILTER(?x) }",
    )
    assert sorted(r["x"]["value"] for r in rows) == ["1", "2.5", "a", "b"]


def test_not_and_or_effective_boolean_value(m):
    rows = bindings(m, "SELECT ?x WHERE { VALUES ?x { 0 1 \"\" \"a\" } FILTER(!?x) }")
    assert sorted(r["x"]["value"] for r in rows) == ["", "0"]
    # An error || true is true, an error && false is false
    rows = bindings(
        m,
        "SELECT ?x WHERE { VALUES ?x { 1 <http://example.org/x> } FILTER(?x || true) }",
    )
    assert len(rows) == 2
    rows = bindings(
        m,
        "SELECT ?x WHERE { VALUES ?x { 1 <http://example.org/x> } FILTER(!(?x && false)) }",
    )
    assert len(rows) == 2


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("xsd:boolean(\"true\")", "true"),
        ("xsd:boolean(\"1\")", "true"),
        ("xsd:boolean(\"0\")", "false"),
        ("xsd:boolean(\"TRUE\")", None),
        ("xsd:boolean(\"abc\")", None),
        ("xsd:boolean(0.0)", "false"),
        ("xsd:boolean(-2)", "true"),
        ("xsd:integer(\"+12\")", "12"),
        ("xsd:integer(\"1.5\")", None),
        ("xsd:integer(\"1E0\")", None),
        ("xsd:integer(true)", "1"),
        ("xsd:integer(\"-7.875\"^^xsd:double)", "-7"),
        ("xsd:integer(\"NaN\"^^xsd:double)", None),
        ("xsd:decimal(true)", "1"),
        ("xsd:decimal(\"+33.3300\")", "33.33"),
        ("xsd:decimal(\"1E0\")", None),
        ("xsd:decimal(\"INF\"^^xsd:double)", None),
        ("xsd:double(\"-10.2E3\")", "-10200.0"),
        ("xsd:double(\"abc\")", None),
        ("xsd:double(\"2002-10-10T17:00:00Z\"^^xsd:dateTime)", None),
        ("xsd:string(2.5)", "2.5"),
        ("xsd:string(1.0)", "1"),
    ],
)
def test_xsd_casts(m, expr, expected):
    result = single(m, f"SELECT ({expr} AS ?x) WHERE {{}}")
    if expected is None:
        assert result is None
    else:
        assert result[0] == expected


def test_boolean_lexical_forms():
    m = Model()
    m.reads(
        """
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <http://example.org/a> <http://example.org/p> "1"^^xsd:boolean, "0"^^xsd:boolean .
        """,
        format="turtle",
    )
    rows = bindings(m, "SELECT ?o WHERE { ?s ?p ?o }")
    assert sorted(r["o"]["value"] for r in rows) == ["false", "true"]


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("CONCAT(\"a\"@en, \"b\"@en)", ("ab", None, "en")),
        ("CONCAT(\"a\"@en, \"b\"@fr)", ("ab", None, None)),
        ("CONCAT(\"a\"@en, \"b\")", ("ab", None, None)),
        ("CONCAT(\"a\", 1)", None),
        ("REPLACE(\"english\"@en, \"e\", \"-\")", ("-nglish", None, "en")),
        ("STRBEFORE(\"abc\"@en, \"b\")", ("a", None, "en")),
        ("STRBEFORE(\"abc\"@en, \"z\")", ("", None, None)),
        ("STRBEFORE(\"abc\"@en, \"\")", ("", None, "en")),
        ("STRBEFORE(\"abc\"@en, \"b\"@en)", ("a", None, "en")),
        ("STRBEFORE(\"abc\"@en, \"b\"@cy)", None),
        ("STRBEFORE(\"abc\", \"b\"@cy)", None),
        ("STRAFTER(\"abc\"@en, \"b\")", ("c", None, "en")),
        ("STRAFTER(\"abc\"@en, \"z\")", ("", None, None)),
    ],
)
def test_string_functions_language_tags(m, expr, expected):
    assert single(m, f"SELECT ({expr} AS ?x) WHERE {{}}") == expected


def test_count_distinct_star(m):
    rows = bindings(
        m,
        "SELECT (COUNT(DISTINCT *) AS ?x) WHERE { VALUES (?a ?b) { (1 2) (1 2) (1 3) } }",
    )
    assert rows[0]["x"]["value"] == "2"
    rows = bindings(m, "SELECT (COUNT(*) AS ?x) (COUNT(DISTINCT *) AS ?y) WHERE {}")
    assert rows[0]["x"]["value"] == "1"
    assert rows[0]["y"]["value"] == "1"


def test_select_star_without_variables(m):
    df = m.query(q("SELECT * WHERE { :s1 :p1 :s2 }"))
    assert df.width == 0
