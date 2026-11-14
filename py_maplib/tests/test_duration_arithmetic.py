"""Arithmetic and comparisons on durations, dates, dateTimes and times (XPath F&O 3.1
sections 8 and 9.7, used by SPARQL 1.1 section 17.3) on values from the model, with
categorical encoding, several datatypes in one column, time zone offsets and filters.
The operators themselves are tested against the W3C QT3 tests in test_w3c_qt3_durations.py.
"""

import json

import pytest

from maplib import Model

XSD = "http://www.w3.org/2001/XMLSchema#"

DATA = """
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:a ex:start "2000-01-31T12:00:00+02:00"^^xsd:dateTime ;
     ex:end "2000-03-01T12:00:00Z"^^xsd:dateTime ;
     ex:day "2000-01-31"^^xsd:date ;
     ex:at "23:30:00"^^xsd:time ;
     ex:length "P1D"^^xsd:dayTimeDuration ;
     ex:period "P1M"^^xsd:yearMonthDuration .
ex:b ex:start "2000-02-29T00:00:00"^^xsd:dateTime ;
     ex:end "2001-02-28T00:00:00"^^xsd:dateTime ;
     ex:day "2000-02-29"^^xsd:date ;
     ex:at "00:15:00"^^xsd:time ;
     ex:length "PT24H"^^xsd:dayTimeDuration ;
     ex:period "P1Y"^^xsd:yearMonthDuration .
ex:c ex:length "PT36H"^^xsd:dayTimeDuration ;
     ex:period "P13M"^^xsd:yearMonthDuration .
ex:d ex:length "P1Y2M3DT4H"^^xsd:duration ;
     ex:period 7 .
"""


@pytest.fixture(scope="module")
def model() -> Model:
    m = Model()
    m.reads(DATA, format="turtle")
    return m


def select(m: Model, query: str) -> dict:
    """The rows by ?s, with each value as (lexical form, datatype)."""
    res = json.loads(m.query(query, return_format="json"))
    out = {}
    for row in res["results"]["bindings"]:
        out[row["s"]["value"].rsplit("/", 1)[1]] = {
            k: (v["value"], v.get("datatype", XSD + "string"))
            for k, v in row.items()
            if k != "s"
        }
    return out


PREFIXES = """
PREFIX ex: <http://example.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


def test_datetime_plus_durations(model):
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s ?p ?l WHERE {
            ?s ex:start ?t ; ex:period ?period ; ex:length ?length .
            BIND(?t + ?period AS ?p)
            BIND(?t - ?length AS ?l)
        }""",
    )
    # Months are added in local time, the day is the last day of a shorter month.
    assert rows["a"]["p"] == ("2000-02-29T12:00:00+02:00", XSD + "dateTime")
    assert rows["a"]["l"] == ("2000-01-30T12:00:00+02:00", XSD + "dateTime")
    assert rows["b"]["p"] == ("2001-02-28T00:00:00", XSD + "dateTime")
    assert rows["b"]["l"] == ("2000-02-28T00:00:00", XSD + "dateTime")


def test_subtract_datetimes_dates_times(model):
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s ?dt ?d ?t WHERE {
            ?s ex:start ?start ; ex:end ?end ; ex:day ?day ; ex:at ?at .
            BIND(?end - ?start AS ?dt)
            BIND(?day - "2000-01-01"^^xsd:date AS ?d)
            BIND(?at - "12:00:00"^^xsd:time AS ?t)
        }""",
    )
    assert rows["a"]["dt"] == ("P30DT2H", XSD + "dayTimeDuration")
    assert rows["b"]["dt"] == ("P365D", XSD + "dayTimeDuration")
    assert rows["a"]["d"] == ("P30D", XSD + "dayTimeDuration")
    assert rows["b"]["d"] == ("P59D", XSD + "dayTimeDuration")
    assert rows["a"]["t"] == ("PT11H30M", XSD + "dayTimeDuration")
    assert rows["b"]["t"] == ("-PT11H45M", XSD + "dayTimeDuration")


def test_date_and_time_plus_durations(model):
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s ?d ?t WHERE {
            ?s ex:day ?day ; ex:at ?at ; ex:period ?period ; ex:length ?length .
            BIND(?day + ?period AS ?d)
            BIND(?at + ?length + "PT1H"^^xsd:dayTimeDuration AS ?t)
        }""",
    )
    assert rows["a"]["d"] == ("2000-02-29", XSD + "date")
    assert rows["b"]["d"] == ("2001-02-28", XSD + "date")
    assert rows["a"]["t"] == ("00:30:00", XSD + "time")
    assert rows["b"]["t"] == ("01:15:00", XSD + "time")


def test_duration_arithmetic_mixed_column(model):
    """?period is a yearMonthDuration or an integer, ?length a dayTimeDuration or a duration."""
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s ?sum ?double ?half ?ratio WHERE {
            ?s ex:period ?period ; ex:length ?length .
            BIND(?period + "P1Y"^^xsd:yearMonthDuration AS ?sum)
            BIND(?length * 2 AS ?double)
            BIND(?length / 2.0 AS ?half)
            BIND(?period / "P1M"^^xsd:yearMonthDuration AS ?ratio)
        }""",
    )
    assert rows["a"]["sum"] == ("P1Y1M", XSD + "yearMonthDuration")
    assert rows["c"]["sum"] == ("P2Y1M", XSD + "yearMonthDuration")
    assert rows["c"]["double"] == ("P3D", XSD + "dayTimeDuration")
    assert rows["c"]["half"] == ("PT18H", XSD + "dayTimeDuration")
    assert float(rows["b"]["ratio"][0]) == 12.0
    assert rows["b"]["ratio"][1] == XSD + "decimal"
    # An integer plus a yearMonthDuration, and xsd:duration arithmetic, are errors
    assert "sum" not in rows["d"]
    assert "double" not in rows["d"]


def test_duration_value_equality(model):
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s WHERE {
            ?s ex:length ?length .
            FILTER(?length = "P1D"^^xsd:dayTimeDuration)
        }""",
    )
    assert set(rows) == {"a", "b"}
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s ?o WHERE {
            ?s ex:length ?l1 .
            ?o ex:length ?l2 .
            FILTER(?l1 = ?l2 && ?s != ?o)
        }""",
    )
    assert set(rows) == {"a", "b"}
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s WHERE {
            ?s ex:period ?period .
            FILTER(?period = "P12M"^^xsd:yearMonthDuration)
        }""",
    )
    assert set(rows) == {"b"}


def test_duration_ordering(model):
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s WHERE {
            ?s ex:length ?length .
            FILTER(?length > "PT30H"^^xsd:dayTimeDuration)
        }""",
    )
    assert set(rows) == {"c"}
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s WHERE {
            ?s ex:period ?period .
            FILTER(?period >= "P1Y"^^xsd:yearMonthDuration)
        }""",
    )
    assert set(rows) == {"b", "c"}


def test_equal_to_arithmetic_is_not_constrained_to_numbers(model):
    """?x = ?a + ?b does not mean that ?x is a number."""
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s WHERE {
            ?s ex:start ?start ; ex:length ?length .
            ?o ex:start ?x .
            FILTER(?x = ?start + ?length - ?length)
            FILTER(?s = ?o)
        }""",
    )
    assert set(rows) == {"a", "b"}


def test_cast_to_durations(model):
    rows = select(
        model,
        PREFIXES
        + """
        SELECT ?s ?ym ?dt WHERE {
            ?s ex:length ?length .
            BIND(xsd:yearMonthDuration(?length) AS ?ym)
            BIND(xsd:dayTimeDuration(STR(?length)) AS ?dt)
        }""",
    )
    assert rows["d"]["ym"] == ("P1Y2M", XSD + "yearMonthDuration")
    # "P1Y2M3DT4H" is not a valid dayTimeDuration lexical form
    assert "dt" not in rows["d"]
    assert rows["c"]["dt"] == ("P1DT12H", XSD + "dayTimeDuration")
    assert rows["c"]["ym"] == ("P0M", XSD + "yearMonthDuration")
