"""Issue 80: xsd:dateTime and xsd:dateTimeStamp literals were normalised to UTC on read,
so the time zone offset was lost and +00:00 was added to dateTimes without a time zone.

The lexical forms must be kept (RDF 1.1 terms are equal only if their lexical forms are),
an absent time zone is not UTC (XSD 1.1), and STR() returns the lexical form (SPARQL 1.1).
"""

import pytest

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"

# Local midnight in Norway (+02:00), which used to come out as 2019-01-01T22:00:00+00:00.
DATA = """
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:stamp ex:validFrom "2019-01-02T00:00:00+02:00"^^xsd:dateTimeStamp .
ex:offset ex:validFrom "2019-01-02T00:00:00+02:00"^^xsd:dateTime .
ex:utc ex:validFrom "2019-01-02T00:00:00Z"^^xsd:dateTime .
ex:local ex:validFrom "2019-01-02T00:00:00"^^xsd:dateTime .
"""

EXPECTED_LITERALS = {
    "http://example.org/stamp": ("2019-01-02T00:00:00+02:00", XSD + "dateTimeStamp"),
    "http://example.org/offset": ("2019-01-02T00:00:00+02:00", XSD + "dateTime"),
    "http://example.org/utc": ("2019-01-02T00:00:00Z", XSD + "dateTime"),
    "http://example.org/local": ("2019-01-02T00:00:00", XSD + "dateTime"),
}


@pytest.fixture
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def select(m: Model, query: str) -> dict:
    """The rows of a SELECT query by subject, with each value as (lexical form, datatype)."""
    import json

    res = json.loads(m.query(query, return_format="json"))
    out = {}
    for row in res["results"]["bindings"]:
        out[row["s"]["value"]] = {
            k: (v["value"], v.get("datatype", XSD + "string"))
            for k, v in row.items()
            if k != "s"
        }
    return out


def test_issue_80_ntriples_keeps_lexical_forms(model):
    nt = model.writes(format="ntriples")
    for s, (lex, dt) in EXPECTED_LITERALS.items():
        assert f'<{s}> <http://example.org/validFrom> "{lex}"^^<{dt}> .' in nt
    assert "22:00:00" not in nt
    assert "+00:00" not in nt


def test_issue_80_turtle_round_trip(model):
    m2 = Model()
    m2.reads(model.writes(format="turtle"), format="turtle")
    assert sorted(m2.writes(format="ntriples").splitlines()) == sorted(
        model.writes(format="ntriples").splitlines()
    )


def test_issue_80_query_results_keep_lexical_forms(model):
    rows = select(
        model,
        "PREFIX ex: <http://example.org/> SELECT ?s ?v WHERE { ?s ex:validFrom ?v }",
    )
    assert {s: r["v"] for s, r in rows.items()} == EXPECTED_LITERALS


def test_issue_80_str_is_lexical_form(model):
    rows = select(
        model,
        "PREFIX ex: <http://example.org/> "
        "SELECT ?s (STR(?v) AS ?str) WHERE { ?s ex:validFrom ?v }",
    )
    assert {s: r["str"][0] for s, r in rows.items()} == {
        s: lex for s, (lex, _) in EXPECTED_LITERALS.items()
    }


def test_issue_80_tz_timezone_and_hours(model):
    rows = select(
        model,
        "PREFIX ex: <http://example.org/> "
        "SELECT ?s (TZ(?v) AS ?tz) (TIMEZONE(?v) AS ?timezone) (HOURS(?v) AS ?h) "
        "WHERE { ?s ex:validFrom ?v }",
    )
    tz = {s: r["tz"][0] for s, r in rows.items()}
    assert tz == {
        "http://example.org/stamp": "+02:00",
        "http://example.org/offset": "+02:00",
        "http://example.org/utc": "Z",
        "http://example.org/local": "",
    }
    timezone = {s: r.get("timezone") for s, r in rows.items()}
    assert timezone == {
        "http://example.org/stamp": ("PT2H", XSD + "dayTimeDuration"),
        "http://example.org/offset": ("PT2H", XSD + "dayTimeDuration"),
        "http://example.org/utc": ("PT0S", XSD + "dayTimeDuration"),
        # TIMEZONE of a dateTime without a time zone is an error, so it is unbound.
        "http://example.org/local": None,
    }
    # The local time is kept, so all are midnight.
    assert {s: r["h"][0] for s, r in rows.items()} == {s: "0" for s in EXPECTED_LITERALS}


PREFIXES = """
PREFIX ex: <http://example.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

# "2019-01-02T00:00:00+02:00" is the same instant as "2019-01-01T22:00:00Z", so = holds
# (xsd:dateTimeStamp is derived from xsd:dateTime) whichever of the two types the constant has.
SAME_INSTANT = ["<http://example.org/offset>", "<http://example.org/stamp>"]


@pytest.mark.parametrize("datatype", ["xsd:dateTime", "xsd:dateTimeStamp"])
def test_issue_80_same_instant_is_equal(model, datatype):
    equal = model.query(
        PREFIXES
        + f"""SELECT ?s WHERE {{
            ?s ex:validFrom ?v .
            FILTER(?v = "2019-01-01T22:00:00Z"^^{datatype})
        }}"""
    )
    assert sorted(equal["s"].to_list()) == SAME_INSTANT


@pytest.mark.parametrize("datatype", ["xsd:dateTime", "xsd:dateTimeStamp"])
def test_issue_80_same_instant_is_in(model, datatype):
    values_in = model.query(
        PREFIXES
        + f"""SELECT ?s WHERE {{
            ?s ex:validFrom ?v .
            FILTER(?v IN ("2019-01-01T22:00:00Z"^^{datatype}, "2000-01-01T00:00:00Z"^^{datatype}))
        }}"""
    )
    assert sorted(values_in["s"].to_list()) == SAME_INSTANT


@pytest.mark.parametrize("datatype", ["xsd:dateTime", "xsd:dateTimeStamp"])
def test_issue_80_same_instant_is_not_same_term(model, datatype):
    same_term = model.query(
        PREFIXES
        + f"""SELECT ?s WHERE {{
            ?s ex:validFrom ?v .
            FILTER(sameTerm(?v, "2019-01-01T22:00:00Z"^^{datatype}))
        }}"""
    )
    assert same_term.height == 0
